package ephemeral

import "cosmossdk.io/store/ephemeral/internal"

type btree = internal.BTree

type (
	// EphemeralStore defines the interface for a tree with batching capabilities.
	EphemeralStore interface {
		// NewBatch creates a new batch that can be safely used from multiple goroutines.
		//
		// For convenience, we refer to this as a Level1 (L1) batch.
		//
		// The L1 batch's current btree is created by copying (Copy()) a snapshot of tree.root.
		// When L1 batch's Commit() is called, it replaces tree.root using atomic.CompareAndSwap().
		NewBatch() EphemeralBatch

		GetSnapshot(height int64) (EphemeralSnapshot, bool)

		// directly returns the value for the given key from the tree's reader btree.
		UnsafeSetter() interface{ Set(key []byte, value any) }
	}

	EphemeralSnapshot interface {
		Get(key []byte) any

		Iterator(start, end []byte) Iterator
		ReverseIterator(start, end []byte) Iterator
	}

	// EphemeralBatch defines operations that can be performed on a batch.
	EphemeralBatch interface {
		// NewNestedBatch creates a nested batch on top of the current batch.
		// It makes a copy (Copy()) of the current batch's btree to create an independent workspace.
		// The parent field points to the current batch.
		NewNestedBatch() EphemeralBatch

		// Get retrieves a value for the given key from the current batch's reader btree.
		// Returns nil if the key does not exist.
		Get(key []byte) any

		// Set adds or updates a key-value pair in the current batch's writer btree.
		// If the key already exists, its value is overwritten.
		// The current.writer is a Copy()'d btree.
		//
		// After the operation completes, the reader pointer is updated to current.writer
		// and the writer is replaced with a Copy()'d btree.
		Set(key []byte, value any)

		// Delete removes a key from the current batch's writer btree.
		// The current.writer is a Copy()'d btree.
		// After the operation completes, the reader pointer is updated to current.writer
		// and the writer is replaced with a Copy()'d btree.
		Delete(key []byte)

		// Commit applies the changes in the current batch:
		// - For nested batches, it updates the parent batch's current pointer.
		// - For top-level batches, it swaps tree.root using atomic.CompareAndSwap().
		// - Commit() will panic if it fails.
		//   Failure can occur if another goroutine has modified tree.root.
		//   We assume this case is handled by higher-level usecases.
		Commit()

		Iterator(start, end []byte) Iterator
		ReverseIterator(start, end []byte) Iterator

		SetHeight(height int64)
	}

	Iterator interface {
		Key() []byte
		Value() any

		Valid() bool

		// Next moves the iterator to the next item.
		Next()

		Close() error
	}

	HeightMap interface {
		//
		Get(height int64) (EphemeralSnapshot, bool)

		//
		Set(height int64, store EphemeralSnapshot)
	}
)
