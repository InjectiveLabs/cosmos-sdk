package ephemeral

import (
	"sync"
	"sync/atomic"

	"cosmossdk.io/store/ephemeral/internal"
)

var _ EphemeralCommitKVStore = (*EphemeralBackendBTree)(nil)

func NewEphemeralBackendBTree() *EphemeralBackendBTree {
	btree := &atomic.Pointer[internal.BTree]{}
	btree.Store(internal.NewBTree())

	return &EphemeralBackendBTree{
		readOnlyBTree: btree,
		batch:         internal.NewBTree(),

		mtx: &sync.RWMutex{},
	}
}

type EphemeralBackendBTree struct {
	readOnlyBTree *atomic.Pointer[internal.BTree]

	batch *internal.BTree

	mtx *sync.RWMutex
}

func (e *EphemeralBackendBTree) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(e)
}

func (e *EphemeralBackendBTree) Commit() {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	iter, err := e.batch.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	newTree := e.readOnlyBTree.Load().Copy()
	for ; iter.Valid(); iter.Next() {
		if internal.IsTombstone(iter.Value()) {
			newTree.Delete(iter.Key())
		} else {
			newTree.Set(iter.Key(), iter.Value())
		}
	}

	e.readOnlyBTree.Store(newTree)
	e.batch.Clear()
}

func (e *EphemeralBackendBTree) Get(key []byte) Sized {
	e.mtx.RLock()
	val := e.batch.Get(key)
	e.mtx.RUnlock()
	if val != nil {
		if internal.IsTombstone(val) {
			return nil
		}
		return val
	}

	return e.readOnlyBTree.Load().Get(key)
}

func (e *EphemeralBackendBTree) Set(key []byte, value internal.Sized) {
	e.mtx.Lock()
	e.batch.Set(key, value)
	e.mtx.Unlock()
}

func (e *EphemeralBackendBTree) Delete(key []byte) {
	e.mtx.Lock()
	e.batch.Set(key, internal.NewTombstone())
	e.mtx.Unlock()
}

func (e *EphemeralBackendBTree) Iterator(start []byte, end []byte) EphemeralIterator {
	mainIter, err := e.readOnlyBTree.Load().Iterator(start, end)
	if err != nil {
		panic(err)
	}

	e.mtx.RLock()
	batchTree := e.batch.Copy()
	e.mtx.RUnlock()
	batchIter, err := batchTree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(mainIter, batchIter, true)
}

func (e *EphemeralBackendBTree) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	mainIter, err := e.readOnlyBTree.Load().ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	e.mtx.RLock()
	batchTree := e.batch.Copy()
	e.mtx.RUnlock()
	batchIter, err := batchTree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(mainIter, batchIter, false)
}
