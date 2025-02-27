package ephemeral

import "cosmossdk.io/store/ephemeral/internal"

var _ EphemeralCacheKVStore = (*EphemeralCacheKV)(nil)

func NewEphemeralCacheKV(parent EphemeralKVStore) *EphemeralCacheKV {
	return &EphemeralCacheKV{
		parent: parent,

		cacheBTree: internal.NewBTree(),
	}
}

// TODO: make thread-safe
type EphemeralCacheKV struct {
	parent EphemeralKVStore

	cacheBTree *internal.BTree
}

func (e *EphemeralCacheKV) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(e)
}

func (e *EphemeralCacheKV) Write() {
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

func (e *EphemeralCacheKV) Set(key []byte, value any) {
	e.cacheBTree.Set(key, &value)
}

func (e *EphemeralCacheKV) Delete(key []byte) {
	e.cacheBTree.Set(key, internal.NewTombstone())
}

func (e *EphemeralCacheKV) Get(key []byte) any {
	cached := e.cacheBTree.Get(key)
	if cached != nil {
		if internal.IsTombstone(cached) {
			return nil
		}
		return cached
	}

	return e.parent.Get(key)
}

func (e *EphemeralCacheKV) Iterator(start []byte, end []byte) EphemeralIterator {
	parentIter := e.parent.Iterator(start, end)

	cacheIter, err := e.cacheBTree.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(parentIter, cacheIter, true)
}

func (e *EphemeralCacheKV) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	parentIter := e.parent.ReverseIterator(start, end)

	cacheIter, err := e.cacheBTree.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(parentIter, cacheIter, false)
}
