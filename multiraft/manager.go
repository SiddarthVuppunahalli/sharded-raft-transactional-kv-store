package multiraft

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Manager routes keys to independent range groups. It never shares a Raft
// node or log between ranges.
type Manager struct {
	mu     sync.RWMutex
	groups map[uint64]*Group
}

func NewManager() *Manager { return &Manager{groups: make(map[uint64]*Group)} }

func (m *Manager) Add(group *Group) error {
	if group == nil {
		return fmt.Errorf("group must not be nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.groups[group.Descriptor().ID]; exists {
		return fmt.Errorf("range %d already exists", group.Descriptor().ID)
	}
	m.groups[group.Descriptor().ID] = group
	return nil
}

func (m *Manager) Group(rangeID uint64) (*Group, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.groups[rangeID]
	return g, ok
}

func (m *Manager) Lookup(key string) (*Group, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]uint64, 0, len(m.groups))
	for id := range m.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if m.groups[id].Descriptor().Contains(key) {
			return m.groups[id], nil
		}
	}
	return nil, fmt.Errorf("no range owns key %q", key)
}

func (m *Manager) Propose(ctx context.Context, command Command) error {
	group, err := m.Lookup(command.Key)
	if err != nil {
		return err
	}
	return group.Propose(ctx, command)
}

func (m *Manager) Stop() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, group := range m.groups {
		group.Stop()
	}
}
