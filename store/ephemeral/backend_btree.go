package ephemeral

import (
	"cosmossdk.io/store/ephemeral/internal"
)

var _ EphemeralCommitKVStore = (*EphemeralBackendBTree)(nil)

func NewEphemeralBackendBTree() *EphemeralBackendBTree {
	return &EphemeralBackendBTree{
		btree: internal.NewBTree(),
		batch: internal.NewBTree(),
	}
}

// TODO: make thread-safe
type EphemeralBackendBTree struct {
	// mtx   *sync.RWMutex
	btree *internal.BTree

	batch *internal.BTree
}

// Branch implements EphemeralBackend.
func (e *EphemeralBackendBTree) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(e)
}

// Commit implements EphemeralBackend.
func (e *EphemeralBackendBTree) Commit() {
	iter, err := e.batch.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	newTree := e.btree.Copy()
	for ; iter.Valid(); iter.Next() {
		if _, ok := iter.Value().(*tombstone); ok {
			newTree.Delete(iter.Key())
		} else {
			newTree.Set(iter.Key(), iter.Value())
		}
	}

	e.btree = newTree
	e.batch.Clear()
}

func (e *EphemeralBackendBTree) Get(key []byte) any {
	val := e.btree.Get(key)
	if val == nil {
		// TODO: handle tombstone
		return e.batch.Get(key)
	}
	return val
}

func (e *EphemeralBackendBTree) Delete(key []byte) {
	e.batch.Set(key, &tombstone{})
}

func (e *EphemeralBackendBTree) Set(key []byte, value any) {
	e.batch.Set(key, &value)
}

func (e *EphemeralBackendBTree) Iterator(start []byte, end []byte) EphemeralIterator {
	mainIter, err := e.btree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	// TODO: lock?
	batchTree := e.batch.Copy()
	batchIter, err := batchTree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(mainIter, batchIter, true)
}

// TODO: btree + batch merged iterator?
func (e *EphemeralBackendBTree) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	mainIter, err := e.btree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	// TODO: lock?
	batchTree := e.batch.Copy()
	batchIter, err := batchTree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(mainIter, batchIter, false)
}
