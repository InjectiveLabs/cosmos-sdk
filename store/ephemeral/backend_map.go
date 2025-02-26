package ephemeral

var _ EphemeralCommitKVStore = (*EphemeralBackendBTree)(nil)

type EphemeralBackendMap struct {
	store           map[string]any
	underlyingStore EphemeralCommitKVStore
}

// Branch implements EphemeralBackend.
func (e *EphemeralBackendMap) Branch() EphemeralCacheKVStore {
	panic("TO")

	// return &EphemeralBackendMap{
	// 	store:           make(map[string]any),
	// 	underlyingStore: e,
	// }
}

func (e *EphemeralBackendMap) Write() {
	panic("???")
}

// Commit implements EphemeralBackend.
func (e *EphemeralBackendMap) Commit() {
	panic("unimplemented")
}

// Get implements EphemeralBackend.
func (e *EphemeralBackendMap) Get(key []byte) any {
	// k way merge
	panic("unimplemented")
}

// Iterator implements EphemeralBackend.
func (e *EphemeralBackendMap) Iterator(start []byte, end []byte) {
	// k way merge iterator
	panic("unimplemented")
}

// ReverseIterator implements EphemeralBackend.
func (e *EphemeralBackendMap) ReverseIterator(start []byte, end []byte) {
	panic("unimplemented")
}

// Set implements EphemeralBackend.
func (e *EphemeralBackendMap) Set(key []byte, value any) {
	panic("unimplemented")
}

func NewEphemeralBackendMap() *EphemeralBackendMap {
	return &EphemeralBackendMap{}
}
