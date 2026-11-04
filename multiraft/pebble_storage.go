package multiraft

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	hardStateKey = []byte("raft/hard-state")
	snapshotKey  = []byte("raft/snapshot")
	lastIndexKey = []byte("raft/last-index")
	entryPrefix  = []byte("raft/entry/")
	kvPrefix     = []byte("kv/")
)

// PebbleStorage persists one Raft group's log, hard state, snapshot metadata,
// and applied key-value records in a single Pebble database directory.
type PebbleStorage struct {
	db *pebble.DB
	mu sync.RWMutex
}

func OpenPebbleStorage(path string) (*PebbleStorage, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open pebble storage %q: %w", path, err)
	}
	return &PebbleStorage{db: db}, nil
}

func (s *PebbleStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *PebbleStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var hardState raftpb.HardState
	if err := readProto(s.db, hardStateKey, &hardState); err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	var snapshot raftpb.Snapshot
	if err := readProto(s.db, snapshotKey, &snapshot); err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	return hardState, snapshot.Metadata.ConfState, nil
}

func (s *PebbleStorage) SetHardState(state raftpb.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeProto(s.db, hardStateKey, &state)
}

// Append persists consecutive Raft entries and updates the last-index marker
// in the same Pebble batch.
func (s *PebbleStorage) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, entry := range entries {
		data, err := entry.Marshal()
		if err != nil {
			return fmt.Errorf("marshal raft entry %d: %w", entry.Index, err)
		}
		if err := batch.Set(entryKey(entry.Index), data, nil); err != nil {
			return fmt.Errorf("write raft entry %d: %w", entry.Index, err)
		}
	}
	last := entries[len(entries)-1].Index
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], last)
	if err := batch.Set(lastIndexKey, index[:], nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit raft entries: %w", err)
	}
	return nil
}

func (s *PebbleStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lo == 0 {
		return nil, raft.ErrCompacted
	}
	last, err := s.lastIndexLocked()
	if err != nil {
		return nil, err
	}
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	entries := make([]raftpb.Entry, 0, hi-lo)
	var size uint64
	for index := lo; index < hi; index++ {
		var entry raftpb.Entry
		if err := readProto(s.db, entryKey(index), &entry); err != nil {
			return nil, raft.ErrUnavailable
		}
		entrySize := uint64(entry.Size())
		if len(entries) > 0 && size+entrySize > maxSize {
			break
		}
		entries = append(entries, entry)
		size += entrySize
	}
	if len(entries) == 0 && hi > lo {
		return nil, raft.ErrUnavailable
	}
	return entries, nil
}

func (s *PebbleStorage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var entry raftpb.Entry
	if err := readProto(s.db, entryKey(index), &entry); err != nil {
		if index == 0 {
			return 0, nil
		}
		return 0, raft.ErrUnavailable
	}
	return entry.Term, nil
}

func (s *PebbleStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *PebbleStorage) lastIndexLocked() (uint64, error) {
	value, closer, err := s.db.Get(lastIndexKey)
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid persisted last index")
	}
	return binary.BigEndian.Uint64(value), nil
}

func (s *PebbleStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return 1, nil
}

func (s *PebbleStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var snapshot raftpb.Snapshot
	if err := readProto(s.db, snapshotKey, &snapshot); err != nil {
		return raftpb.Snapshot{}, err
	}
	return snapshot, nil
}

// Apply persists a committed command as the range's key-value state.
func (s *PebbleStorage) Apply(command Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := append(append([]byte{}, kvPrefix...), []byte(command.Key)...)
	switch command.Op {
	case "", "put", "set":
		return s.db.Set(key, command.Value, pebble.Sync)
	case "delete", "del":
		return s.db.Delete(key, pebble.Sync)
	default:
		return fmt.Errorf("unsupported command operation %q", command.Op)
	}
}

func (s *PebbleStorage) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	storageKey := append(append([]byte{}, kvPrefix...), []byte(key)...)
	value, closer, err := s.db.Get(storageKey)
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), true, nil
}

func entryKey(index uint64) []byte {
	key := make([]byte, len(entryPrefix)+8)
	copy(key, entryPrefix)
	binary.BigEndian.PutUint64(key[len(entryPrefix):], index)
	return key
}

func writeProto(db *pebble.DB, key []byte, message interface{ Marshal() ([]byte, error) }) error {
	data, err := message.Marshal()
	if err != nil {
		return err
	}
	return db.Set(key, data, pebble.Sync)
}

func readProto(db *pebble.DB, key []byte, message interface{ Unmarshal([]byte) error }) error {
	data, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	return message.Unmarshal(data)
}

var _ raft.Storage = (*PebbleStorage)(nil)
