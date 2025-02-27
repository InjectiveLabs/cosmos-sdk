package ephemeral

import (
	"cosmossdk.io/store/ephemeral/internal"
)

var _ EphemeralBackend = (*EphemeralBackendBTree)(nil)

type tombstone struct{}

type EphemeralBackendBTree struct {
	bTree *internal.BTree
	batch *internal.BTree
}

// Branch implements EphemeralBackend.
func (e *EphemeralBackendBTree) Branch() EphemeralCacheStore {
	return NewEphemeralCacheBTree(e)
}

// Commit implements EphemeralBackend.
func (e *EphemeralBackendBTree) Commit() {
	itr, err := e.batch.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}

	for itr.Valid() {
		if _, ok := itr.Value().(*tombstone); ok {
			e.bTree.Delete(itr.Key())
		} else {
			e.bTree.Set(itr.Key(), itr.Value())
		}
		itr.Next()
	}

	e.batch = internal.NewBTree()
}

// Set implements EphemeralBackend.
func (e *EphemeralBackendBTree) Set(key []byte, value any) {
	e.batch.Set(key, &value)
}

// Get implements EphemeralBackend.
func (e *EphemeralBackendBTree) Get(key []byte) any {
	return e.bTree.Get(key)
}

func (e *EphemeralBackendBTree) Delete(key []byte) {
	e.batch.Set(key, &tombstone{})
}

// Iterator implements EphemeralBackend.
func (e *EphemeralBackendBTree) Iterator(start []byte, end []byte) EphemeralIterator {
	itr, err := e.bTree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return itr
}

// ReverseIterator implements EphemeralBackend.
func (e *EphemeralBackendBTree) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	itr, err := e.bTree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return itr
}

func NewEphemeralBackendBTree() *EphemeralBackendBTree {
	return &EphemeralBackendBTree{
		bTree: internal.NewBTree(),
		batch: internal.NewBTree(),
	}
}

var _ EphemeralCacheStore = (*EphemeralCacheBTree)(nil)

type EphemeralCacheBTree struct {
	parent EphemeralKVStore
	bTree  *internal.BTree
}

func (e *EphemeralCacheBTree) Branch() EphemeralCacheStore {
	return NewEphemeralCacheBTree(e)
}

func (e *EphemeralCacheBTree) Write() {
	itr, err := e.bTree.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}

	for itr.Valid() {
		e.parent.Set(itr.Key(), itr.Value())
		itr.Next()
	}
}

func (e *EphemeralCacheBTree) Set(key []byte, value any) {
	e.bTree.Set(key, &value)
}

func (e *EphemeralCacheBTree) Get(key []byte) any {
	return e.bTree.Get(key)
}

func (e *EphemeralCacheBTree) Delete(key []byte) {
	e.bTree.Delete(key)
}

func (e *EphemeralCacheBTree) Iterator(start []byte, end []byte) EphemeralIterator {
	itr, err := e.bTree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return itr
}

func (e *EphemeralCacheBTree) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	itr, err := e.bTree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return itr
}

func NewEphemeralCacheBTree(parent EphemeralKVStore) *EphemeralCacheBTree {
	return &EphemeralCacheBTree{
		parent: parent,
		bTree:  internal.NewBTree(),
	}
}
