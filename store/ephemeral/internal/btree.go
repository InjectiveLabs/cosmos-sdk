package internal

import (
	"bytes"
	"errors"

	"github.com/tidwall/btree"
)

const (
	// The approximate number of items and children per B-tree node. Tuned with benchmarks.
	// copied from memdb.
	bTreeDegree = 32
)

var errKeyEmpty = errors.New("key cannot be empty")

// BTree implements the sorted cache for cachekv store,
// we don't use MemDB here because cachekv is used extensively in sdk core path,
// we need it to be as fast as possible, while `MemDB` is mainly used as a mocking db in unit tests.
//
// We choose tidwall/btree over google/btree here because it provides API to implement step iterator directly.
type (
	Sized interface {
		// like len([]byte) or proto.Size()
		Size() int
	}

	BTree struct {
		tree *btree.BTreeG[item[Sized]]
	}
)

// NewBTree creates a wrapper around `btree.BTreeG`.
func NewBTree() *BTree {
	return &BTree{
		tree: btree.NewBTreeGOptions[item[Sized]](byKeys, btree.Options{
			Degree:  bTreeDegree,
			NoLocks: true,
		}),
	}
}

func (bt *BTree) Set(key []byte, value Sized) {
	bt.tree.Set(newItem(key, value))
}

func (bt *BTree) Delete(key []byte) {
	bt.tree.Delete(newItem[Sized](key, nil))
}

func (bt BTree) Get(key []byte) Sized {
	i, found := bt.tree.Get(newItem[Sized](key, nil))
	if !found {
		return nil
	}
	return i.value
}

func (bt BTree) Iterator(start, end []byte) (TypedEphemeralIterator[Sized], error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemIterator(start, end, bt, true), nil
}

func (bt BTree) ReverseIterator(start, end []byte) (TypedEphemeralIterator[Sized], error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	return newMemIterator(start, end, bt, false), nil
}

// Copy the tree. This is a copy-on-write operation and is very fast because
// it only performs a shadowed copy.
func (bt BTree) Copy() *BTree {
	return &BTree{
		tree: bt.tree.Copy(),
	}
}

func (bt BTree) Clear() {
	bt.tree.Clear()
}

// item is a btree item with byte slices as keys and values
type item[T Sized] struct {
	key   []byte
	value T
}

// byKeys compares the items by key
func byKeys[T Sized](a, b item[T]) bool {
	return bytes.Compare(a.key, b.key) == -1
}

// newItem creates a new pair item.
func newItem[T Sized](key []byte, value T) item[T] {
	return item[T]{key: key, value: value}
}
