package ephemeral

import (
	"sync"

	"cosmossdk.io/store/ephemeral/internal"
)

var _ EphemeralCacheKVStore = (*EphemeralCacheKV)(nil)

func NewEphemeralCacheKV(parent EphemeralKVStore) *EphemeralCacheKV {
	return &EphemeralCacheKV{
		parent: parent,

		mtx:        &sync.RWMutex{},
		cacheBTree: internal.NewBTree(),
	}
}

type EphemeralCacheKV struct {
	parent EphemeralKVStore

	mtx        *sync.RWMutex
	cacheBTree *internal.BTree
}

func (e *EphemeralCacheKV) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(e)
}

func (e *EphemeralCacheKV) Write() {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	iter, err := e.cacheBTree.Iterator(nil, nil)
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		if internal.IsTombstone(iter.Value()) {
			e.parent.Delete(iter.Key())
		} else {
			e.parent.Set(iter.Key(), iter.Value())
		}
	}
}

func (e *EphemeralCacheKV) Set(key []byte, value Sized) {
	e.mtx.Lock()
	e.cacheBTree.Set(key, value)
	e.mtx.Unlock()
}

func (e *EphemeralCacheKV) Delete(key []byte) {
	e.mtx.Lock()
	e.cacheBTree.Set(key, internal.NewTombstone())
	e.mtx.Unlock()
}

func (e *EphemeralCacheKV) Get(key []byte) Sized {
	e.mtx.RLock()
	cached := e.cacheBTree.Get(key)
	e.mtx.RUnlock()

	if cached != nil {
		if internal.IsTombstone(cached) {
			return nil
		}
		return cached
	}

	return e.parent.Get(key)
}

func (e *EphemeralCacheKV) Iterator(start []byte, end []byte) EphemeralIterator {
	e.mtx.RLock()
	btree := e.cacheBTree.Copy()
	e.mtx.RUnlock()

	cacheIter, err := btree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	parentIter := e.parent.Iterator(start, end)

	return internal.NewCacheMergeIterator(parentIter, cacheIter, true)
}

func (e *EphemeralCacheKV) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	e.mtx.RLock()
	btree := e.cacheBTree.Copy()
	e.mtx.RUnlock()

	cacheIter, err := btree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	parentIter := e.parent.ReverseIterator(start, end)

	return internal.NewCacheMergeIterator(parentIter, cacheIter, false)
}
