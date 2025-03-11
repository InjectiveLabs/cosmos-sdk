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
		root         *atomic.Pointer[btree]
		snapshotPool SnapshotPool
	}

	// `IndexedBatch` implements a copy-on-write batch operation pattern.
	// Nested batches follow this structure as well, updating their parent's current on `Commit()`.
	IndexedBatch struct {
		// parent is nil for top-level batches, non-nil for nested batches
		parent *IndexedBatch
		// tree is non-nil for top-level batches
		tree *Tree
		// base is a snapshot of the btree at the time the batch was created
		base *btree
		// current reflects changes made during batch operations
		current *btree

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

	return &Tree{
		root: root,

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
	base := t.root.Load()

	// Create a copy-on-write snapshot for the current
	current := base.Copy()

	return &IndexedBatch{
		// This is a top-level batch, so parent is nil
		parent: nil,

		tree:    t,
		base:    base,
		current: current,
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
	//   -> If current is L1: points to tree.root
	//
	// newCurrent is a Copy()'d btree.
	newCurrent := b.current.Copy()

	return &IndexedBatch{
		parent: b,

		// base is nil for nested batches
		base: nil,
		tree: nil,

		current: newCurrent,
	}
}

// Commit applies the changes in the batch:
// - For nested batches, it updates the parent batch's current pointer.
// - For top-level batches, it replaces tree.root with the batch's current writer.
func (b *IndexedBatch) Commit() {
	if b.parent != nil {
		// Nested batch: update parent's current pointer
		b.parent.current = b.current

		// TODO(ephemeral): Should we panic if the L1 Batch is already committed?
		// If we should, consider the following options:
		//  1. Prevent committing if there are remaining references to L2 from the parent (L1) using reference counting.
		//  2. Check the base pointer when committing from a child (L2 ~ ..).
		//  3. Maintain the current behavior.
		return
	}

	if b.base != nil {
		// Top-level batch: replace tree's root using atomic CompareAndSwap
		if !b.tree.root.CompareAndSwap(b.base, b.current) {
			panic("commit failed: concurrent modification detected")
		}

		if b.height != 0 {
			// Update the height map with the new store
			copiedStore := b.current.Copy()
			root := &atomic.Pointer[btree]{}
			root.Store(copiedStore)

			b.tree.snapshotPool.Set(b.height, &Tree{
				root: root,
				// snapshotPool is not used for snapshot batches
				snapshotPool: nil,
			})
		}

		return
	}

	// This case should never happen
	panic("unreachable code, base is nil & parent is nil")
}

// NOTE(ephemeral): lifecycle
//  1. call baseapp.setState -> cms.GetEphemeralStore().SetHeight(height)
//  2. call in rootmulti.Store.CacheMultiStoreWithVersion(ver int64)
func (b *IndexedBatch) SetHeight(height int64) {
	b.height = height
}
