package ephemeral

var _ EphemeralBackend = (*EphemeralBackendBTree)(nil)

type EphemeralBackendBTree struct {
	underlyingStore EphemeralBackend
}

// Branch implements EphemeralBackend.
func (e *EphemeralBackendBTree) Branch() EphemeralCacheStore {
	e.underlyingStore

	panic("unimplemented")
}

// Commit implements EphemeralBackend.
func (e *EphemeralBackendBTree) Commit() {
	panic("unimplemented")
}

// Get implements EphemeralBackend.
func (e *EphemeralBackendBTree) Get(key []byte) any {
	panic("unimplemented")
}

// Iterator implements EphemeralBackend.
func (e *EphemeralBackendBTree) Iterator(start []byte, end []byte) {
	panic("unimplemented")
}

// ReverseIterator implements EphemeralBackend.
func (e *EphemeralBackendBTree) ReverseIterator(start []byte, end []byte) {
	panic("unimplemented")
}

// Set implements EphemeralBackend.
func (e *EphemeralBackendBTree) Set(key []byte, value any) {
	panic("unimplemented")
}

func NewEphemeralBackendBTree() *EphemeralBackendBTree {
	return &EphemeralBackendBTree{}
}
