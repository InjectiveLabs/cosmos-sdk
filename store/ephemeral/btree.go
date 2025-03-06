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
		root *atomic.Pointer[btree]
	}

	// IndexedBatch implements a copy-on-write batch operation pattern.
	// Nested batches follow this structure as well, updating their parent's current on Commit.
	IndexedBatch struct {
		// parent is nil for top-level batches, non-nil for nested batches
		parent *IndexedBatch
		// tree is non-nil for top-level batches
		tree *Tree
		// base is a snapshot of the btree at the time the batch was created
		base *btree
		// current reflects changes made during batch operations
		current *struct {
			// reader uses copy-on-write snapshot pattern.
			// It's wrapped in atomic.Pointer for safe concurrent access from other goroutines.
			reader *atomic.Pointer[btree]
			// writer doesn't need atomic.Pointer as it's used with the single writer assumption.
			writer *btree
		}
	}
)

// NewTree creates a new empty Tree.
func NewTree() *Tree {
	root := &atomic.Pointer[btree]{}
	root.Store(internal.NewBTree())

	return &Tree{
		root: root,
	}
}

// NewBatch creates a top-level batch.
// It creates a copy-on-write snapshot of the tree's root btree as its working copy.
func (t *Tree) NewBatch() EphemeralBatch {
	base := t.root.Load()
	// Create a copy-on-write snapshot
	readerTree := base
	writer := base.Copy()

	reader := &atomic.Pointer[btree]{}
	reader.Store(readerTree)

	return &IndexedBatch{
		// This is a top-level batch, so parent is nil
		parent: nil,

		tree: t,
		base: base,
		current: &struct {
			reader *atomic.Pointer[btree]
			writer *btree
		}{
			reader: reader,
			writer: writer,
		},
	}
}

// Get retrieves a value for the given key from the current batch.
func (b *IndexedBatch) Get(key []byte) any {
	return b.current.reader.Load().Get(key)
}

// Set adds or updates a key-value pair in the current batch.
func (b *IndexedBatch) Set(key []byte, value any) {
	b.write(func(tree *btree) {
		tree.Set(key, value)
	})
}

// Delete removes a key from the current batch.
func (b *IndexedBatch) Delete(key []byte) {
	b.write(func(tree *btree) {
		tree.Delete(key)
	})
}

// write executes operations on the current batch's btree.
//
// NOTE:
//   - This write operation performs copy-on-write (CoW) on every execution.
//   - Due to B-tree characteristics, this can create up to 'depth' number of new nodes
//     compared to an implementation without CoW.
//   - Alternative implementations to reduce this overhead include:
//     1. Applying rwlock only at L2 (where actual write operations occur)
//     2. Storing diffs and performing k-way merges instead of copy-on-write
//
// benchmark result:
// BenchmarkCopyPerOperation-12    	  470655	      2777 ns/op	    5230 B/op	      21 allocs/op
func (b *IndexedBatch) write(cb func(tree *btree)) {
	// Use the current batch's btree to perform operations.
	// The current.writer is a Copy()'d btree.
	writer := b.current.writer
	// Execute Set or Delete operation in the callback.
	cb(writer)

	// After completing the operation, create a copy-on-write snapshot of the writer.
	copiedWriter := writer.Copy()
	// Store the previous snapshot in current.reader.
	// The reader is wrapped in atomic.Pointer so it can be accessed safely from other goroutines.
	// This ensures that other goroutines accessing the reader will use the previous snapshot.
	b.current.reader.Store(writer)
	// Replace writer with the copied btree.
	// The writer doesn't need atomic.Pointer as it's used with the single writer assumption.
	b.current.writer = copiedWriter
}

// NewNestedBatch creates a nested batch on top of the current batch.
// It copies the current batch's btree to create an independent workspace.
func (b *IndexedBatch) NewNestedBatch() EphemeralBatch {
	// Reader uses the current batch's btree.
	// Here, current refers to the current level's -1 level batch:
	//   -> If current is L3: points to L2's current
	//   -> If current is L2: points to L1's current
	//   -> If current is L1: points to tree.root
	readerTree := b.current.reader.Load()

	reader := &atomic.Pointer[btree]{}
	reader.Store(readerTree)

	// Writer is a Copy()'d btree.
	// The writer doesn't need atomic.Pointer as it's used with the single writer assumption.
	writer := readerTree.Copy()

	return &IndexedBatch{
		parent: b,
		base:   nil,
		current: &struct {
			reader *atomic.Pointer[btree]
			writer *btree
		}{
			reader: reader,
			writer: writer,
		},
	}
}

// Commit applies the changes in the batch:
// - For nested batches, it updates the parent batch's current pointer.
// - For top-level batches, it swaps tree.root using atomic.CompareAndSwap().
func (b *IndexedBatch) Commit() {
	if b.parent != nil {
		// Nested batch: update parent's current pointer
		b.parent.current.reader.Store(b.current.reader.Load())
		return
	}

	if b.base != nil {
		// Top-level batch: replace tree's root using atomic CompareAndSwap
		if !b.tree.root.CompareAndSwap(b.base, b.current.reader.Load()) {
			panic("commit failed: concurrent modification detected")
		}
		return
	}

	// This case should never happen
	panic("unreachable code, base is nil & parent is nil")
}
