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

		GetSnapshotBatch(height int64) (EphemeralBatch, bool)

		SetSnapshotPoolLimit(limit int64)

		Commit()

		SetHeight(height int64)
	}

	EphemeralReader interface {
		// Get retrieves a value for the given key from the current batch's reader btree.
		// Returns nil if the key does not exist.
		Get(key []byte) any

		// Iterator returns an iterator over the key-value pairs in the batch
		// within the specified range.
		//
		// The iterator will include items with key >= start and key < end.
		// If start is nil, it returns all items from the beginning.
		// If end is nil, it returns all items until the end.
		//
		// If an error occurs during initialization, this method panics.
		Iterator(start, end []byte) Iterator
		// ReverseIterator returns an iterator over the key-value pairs in the batch
		// within the specified range, in reverse order (from end to start).
		//
		// The iterator will include items with key >= start and key < end.
		// If start is nil, it returns all items from the beginning.
		// If end is nil, it returns all items until the end.
		//
		// If an error occurs during initialization, this method panics.
		ReverseIterator(start, end []byte) Iterator
	}

	EphemeralWriter interface {
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
	}

	// EphemeralBatch defines operations that can be performed on a batch.
	//
	// The implementation of Ephemeral{Reader,Writer} in Batch is not thread-safe.
	// Therefore, when concurrent access is required, users should protect it
	// using rwlock at the application level.
	EphemeralBatch interface {
		// NewNestedBatch creates a nested batch on top of the current batch.
		// It makes a copy (BTree.Copy()) of the current batch's btree to create an independent workspace.
		// The parent field points to the current batch.
		NewNestedBatch() EphemeralBatch

		EphemeralReader
		EphemeralWriter

		SetHeight(height int64)
	}

	// Iterator defines an interface for traversing key-value pairs in order.
	// Callers must call Close when done to release any allocated resources.
	Iterator interface {
		// Domain returns the start and end keys defining the range of this iterator.
		// The returned values match what was passed to Iterator() or ReverseIterator().
		Domain() (start, end []byte)

		// Key returns the current key.
		// Panics if the iterator is not valid.
		Key() []byte

		// Value returns the current value.
		// Panics if the iterator is not valid.
		Value() any

		// Valid returns whether the iterator is positioned at a valid item.
		// Once false, Valid() will never return true again.
		Valid() bool

		// Next moves the iterator to the next item.
		// If Valid() returns false after this call, the iteration is complete.
		Next()

		// Close releases any resources associated with the iterator.
		// It must be called when done using the iterator.
		Close() error
	}

	SnapshotPool interface {
		Get(height int64) (EphemeralStore, bool)

		Set(height int64, store EphemeralStore)

		Limit(length int64)
	}
)
