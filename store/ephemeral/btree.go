package ephemeral

import (
	"sync/atomic"

	"cosmossdk.io/store/ephemeral/internal"
)

var (
	_ EphemeralStore = &Tree{}
	_ EphemeralBatch = &IndexedBatch{}
)

type (
	Tree struct {
		// root is an atomic pointer to the current root of the btree.
		// When a batch is committed, it creates a new root node and atomically
		// swaps it with the existing one.
		root *atomic.Pointer[btree]
		// The current B-tree is stored in the root atomic.Pointer when Tree.Commit() occurs.
		// The reason for creating it temporarily in this way is that it should not be read
		// in the FinalizeBlock state before BaseApp.Commit happens.
		//
		// `current` is implemented with the assumption that it is accessed only by a single writer.
		current *btree
		// base ensures that only one IndexedBatch (L1) can be committed.
		//
		// It is set to nil for Trees retrieved from the snapshotPool.
		base *atomic.Pointer[btree]

		height       int64
		snapshotPool SnapshotPool
	}

	// `IndexedBatch` implements a copy-on-write batch operation pattern.
	// Nested batches follow this structure as well, updating their parent's current on `Commit()`.
	IndexedBatch struct {
		// parent is nil for top-level batches, non-nil for nested batches
		parent *IndexedBatch

		// current holds the current working copy of the btree for this top-level batch.
		current *btree

		// base points to Tree.base when the batch is created.
		//
		// If Tree.base differs from base at commit time, the commit fails and a panic occurs.
		base *btree

		// The tree exists to swap the pointer of Tree.current in the top-level batch.
		tree *Tree

		height int64
	}

	UncommittableBatch struct {
		EphemeralBatch
	}
)

func (u *UncommittableBatch) Commit() {
	panic("uncommittable batch cannot be committed")
}

// NewTree creates a new empty Tree.
func NewTree() *Tree {
	tree := internal.NewBTree()

	root := &atomic.Pointer[btree]{}
	root.Store(tree)

	base := &atomic.Pointer[btree]{}
	base.Store(tree)

	return &Tree{
		root:    root,
		current: tree.Copy(),
		base:    base,

		snapshotPool: newSnapshotPool(),
	}
}

func (t *Tree) SetSnapshotPoolLimit(limit int64) {
	t.snapshotPool.Limit(limit)
}

// Committing a batch created here is unsafe.
func (t *Tree) GetSnapshotBatch(height int64) (EphemeralBatch, bool) {
	reader, ok := t.snapshotPool.Get(height)
	if !ok {
		return nil, false
	}

	batch := reader.NewBatch()
	// The snapshot batch is a Batch type used in CacheMultiStoreWithVersion(..).
	// Since it handles data for queries at past heights, it is immutable.
	// Therefore, Commit() cannot be performed.
	return &UncommittableBatch{batch}, true
}

// NewBatch creates a top-level batch.
// It creates a copy-on-write snapshot of the tree's root btree as its working copy.
func (t *Tree) NewBatch() EphemeralBatch {
	root := t.root.Load()
	// Create a copy-on-write snapshot for the current
	current := root.Copy()

	var base *btree
	if t.base != nil {
		base = t.base.Load()
	}

	return &IndexedBatch{
		// This is a top-level batch, so parent is nil
		parent: nil,

		current: current,
		base:    base,
		tree:    t,
	}
}

func (t *Tree) Commit() {
	// Since `current` is used only in a single thread, direct access is safe.
	current := t.current

	if current == nil {
		panic("`EphemeralStore.Commit()` should not be called on an ephemeral store retrieved from the snapshot pool.")
	}

	copiedTree := current.Copy()
	if !t.root.CompareAndSwap(t.base.Load(), current) {
		panic("commit failed: concurrent modification detected")
	}
	t.current = copiedTree
	t.base.Store(current)

	if t.height != 0 {
		snapshotTree := copiedTree.Copy()

		root := &atomic.Pointer[btree]{}
		root.Store(snapshotTree)

		t.snapshotPool.Set(t.height, &Tree{
			root:    root,
			current: nil, // Trees stored in the snapshot pool cannot be committed
			base:    nil,

			height:       0,
			snapshotPool: nil,
		})
		t.height = 0
	}
}

// Get retrieves a value for the given key from the current batch.
func (b *IndexedBatch) Get(key []byte) any {
	return b.current.Get(key)
}

// Iterator returns an iterator over the key-value pairs in the batch
// within the specified range.
//
// The iterator will include items with key >= start and key < end.
// If start is nil, it returns all items from the beginning.
// If end is nil, it returns all items until the end.
//
// If an error occurs during initialization, this method panics.
func (b *IndexedBatch) Iterator(start, end []byte) Iterator {
	// NOTE(ephemeral): If a snapshot is not created, the current BTree cannot be modified until the Iterator is closed.
	snapshot := b.current.Copy()

	iter, err := snapshot.Iterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// ReverseIterator returns an iterator over the key-value pairs in the batch
// within the specified range, in reverse order (from end to start).
//
// The iterator will include items with key >= start and key < end.
// If start is nil, it returns all items from the beginning.
// If end is nil, it returns all items until the end.
//
// If an error occurs during initialization, this method panics.
func (b *IndexedBatch) ReverseIterator(start, end []byte) Iterator {
	// NOTE(ephemeral): If a snapshot is not created, the current BTree cannot be modified until the Iterator is closed.
	snapshot := b.current.Copy()

	iter, err := snapshot.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}

	return iter
}

// Set adds or updates a key-value pair in the current batch.
func (b *IndexedBatch) Set(key []byte, value any) {
	b.current.Set(key, value)
}

// Delete removes a key from the current batch.
func (b *IndexedBatch) Delete(key []byte) {
	b.current.Delete(key)
}

// NewNestedBatch creates a nested batch on top of the current batch.
// It copies the current batch's btree to create an independent workspace.
func (b *IndexedBatch) NewNestedBatch() EphemeralBatch {
	// Here, current refers to the current level's -1 level batch:
	//   -> If current is L3: points to L2's current
	//   -> If current is L2: points to L1's current
	//   -> If current is L1: points to tree.current
	//
	// newCurrent is a Copy()'d btree.
	newCurrent := b.current.Copy()

	return &IndexedBatch{
		parent: b,

		current: newCurrent,
	}
}

// Commit applies the changes in the batch:
// - For nested batches, it updates the parent batch's current pointer.
// - For top-level batches, it replaces tree.root with the batch's current writer.
func (b *IndexedBatch) Commit() {
	if b.parent != nil {
		// nested batch: update parent's current pointer
		b.parent.current = b.current
		if b.height != 0 { // If height is set in the batch, propagate it to the tree.
			b.parent.height = b.height
		}

		return
	}

	if b.current != nil {
		// top-level batch: swap *Tree.current
		if b.tree.base.Load() != b.base {
			panic("commit failed: concurrent modification detected")
		}

		b.tree.current = b.current
		if b.height != 0 { // If height is set in the batch, propagate it to the tree.
			b.tree.height = b.height
		}

		return
	}

	panic("unreachable code, parent is nil & current is nil")
}

func (t *Tree) SetHeight(height int64) {
	t.height = height
}

// NOTE(ephemeral): lifecycle
//  1. call baseapp.setState -> cms.GetEphemeralStore().SetHeight(height)
//  2. call in rootmulti.Store.CacheMultiStoreWithVersion(ver int64)
func (b *IndexedBatch) SetHeight(height int64) {
	b.height = height
}
