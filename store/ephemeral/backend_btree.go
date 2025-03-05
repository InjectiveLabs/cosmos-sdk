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

		batch: make([]struct {
			key   []byte
			value Sized
			op    operation
		}, 0),
		mtx: &sync.Mutex{},
	}
}

type (
	EphemeralBackendBTree struct {
		readOnlyBTree *atomic.Pointer[internal.BTree]

		batch []struct {
			key   []byte
			value Sized
			op    operation
		}
		mtx *sync.Mutex
	}

	operation int8
)

const (
	operationSet operation = iota
	operationDelete
)

func (e *EphemeralBackendBTree) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(e)
}

func (e *EphemeralBackendBTree) Commit() {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	newTree := e.readOnlyBTree.Load().Copy()
	for i := 0; i < len(e.batch); i++ {
		switch e.batch[i].op {
		case operationSet:
			newTree.Set(e.batch[i].key, e.batch[i].value)
		case operationDelete:
			newTree.Delete(e.batch[i].key)
		}
	}

	e.readOnlyBTree.Store(newTree)
	e.batch = make([]struct {
		key   []byte
		value Sized
		op    operation
	}, 0)
}

func (e *EphemeralBackendBTree) Get(key []byte) Sized {
	return e.readOnlyBTree.Load().Get(key)
}

func (e *EphemeralBackendBTree) Set(key []byte, value internal.Sized) {
	e.mtx.Lock()
	e.batch = append(e.batch, struct {
		key   []byte
		value Sized
		op    operation
	}{
		key:   key,
		value: value,
		op:    operationSet,
	})
	e.mtx.Unlock()
}

func (e *EphemeralBackendBTree) Delete(key []byte) {
	e.mtx.Lock()
	e.batch = append(e.batch, struct {
		key   []byte
		value Sized
		op    operation
	}{
		key:   key,
		value: nil,
		op:    operationDelete,
	})
	e.mtx.Unlock()
}

func (e *EphemeralBackendBTree) Iterator(start []byte, end []byte) EphemeralIterator {
	iter, err := e.readOnlyBTree.Load().Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

func (e *EphemeralBackendBTree) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	iter, err := e.readOnlyBTree.Load().ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}
