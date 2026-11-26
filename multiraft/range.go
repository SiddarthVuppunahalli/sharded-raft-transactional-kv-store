package multiraft

// RangeDescriptor identifies the half-open key interval owned by one Raft
// group. An empty EndKey represents the end of the keyspace.
type RangeDescriptor struct {
		ID       uint64
		StartKey string
		EndKey   string
}

func (r RangeDescriptor) Contains(key string) bool {
	if key < r.StartKey {
		return false
	}
	return r.EndKey == "" || key < r.EndKey
}
