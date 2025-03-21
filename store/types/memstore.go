package types

type (
	// MemStoreManager defines the interface for a tree with batching capabilities.
	MemStoreManager interface {
		// Branch creates a new MemStore that can be safely used from multiple goroutines.
		//
		// For convenience, we refer to this as a Level1 (L1) MemStore.
		//
		// The L1 MemStore's current btree is created by copying (Copy()) a snapshot of tree.root.
		// When L1 MemStore's Commit() is called, it replaces tree.root using atomic.CompareAndSwap().
		Branch() MemStore

		GetSnapshotBranch(height int64) (MemStore, bool)

		SetSnapshotPoolLimit(limit int64)

		Commit(height int64)
	}

	MemStoreReader interface {
		// Get retrieves a value for the given key from the current MemStore's btree.
		// Returns nil if the key does not exist.
		Get(key []byte) any

		// Iterator returns an iterator over the key-value pairs in the MemStore
		// within the specified range.
		//
		// The iterator will include items with key >= start and key < end.
		// If start is nil, it returns all items from the beginning.
		// If end is nil, it returns all items until the end.
		//
		// If an error occurs during initialization, this method panics.
		Iterator(start, end []byte) MemStoreIterator
		// ReverseIterator returns an iterator over the key-value pairs in the MemStore
		// within the specified range, in reverse order (from end to start).
		//
		// The iterator will include items with key >= start and key < end.
		// If start is nil, it returns all items from the beginning.
		// If end is nil, it returns all items until the end.
		//
		// If an error occurs during initialization, this method panics.
		ReverseIterator(start, end []byte) MemStoreIterator
	}

	MemStoreWriter interface {
		// Set adds or updates a key-value pair in the current MemStore's btree.
		// If the key already exists, its value is overwritten.
		//
		// Changes are made to the Copy-on-Write btree of the current MemStore.
		Set(key []byte, value any)

		// Delete removes a key from the current MemStore's btree.
		//
		// Changes are made to the Copy-on-Write btree of the current MemStore.
		Delete(key []byte)

		// Commit applies the changes in the current MemStore:
		// - For nested MemStores, it updates the parent MemStore's current pointer.
		// - For top-level MemStores, it updates tree.current and prepares for atomic swap during tree.Commit().
		// - Commit() will panic if it fails.
		//   Failure can occur if concurrent modification is detected.
		//   We assume this case is handled by higher-level usecases.
		Commit()
	}

	// MemStore defines operations that can be performed on a memory store.
	//
	// The implementation of MemStore is not thread-safe.
	// Therefore, when concurrent access is required, users should protect it
	// using rwlock at the application level.
	MemStore interface {
		// Branch creates a nested MemStore on top of the current MemStore.
		// It makes a copy (BTree.Copy()) of the current MemStore's btree to create an independent workspace.
		// The parent field points to the current MemStore.
		Branch() MemStore

		MemStoreReader
		MemStoreWriter
	}

	// MemStoreIterator defines an interface for traversing key-value pairs in order.
	// Callers must call Close when done to release any allocated resources.
	MemStoreIterator interface {
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
		Get(height int64) (MemStoreManager, bool)

		Set(height int64, store MemStoreManager)

		Limit(length int64)
	}
)
