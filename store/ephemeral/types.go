package ephemeral

import "cosmossdk.io/store/ephemeral/internal"

type (
	// EphemeralCommitKVStore acts as a Committer, recording the changes made so far
	// in memory (within rootmulti.Store) that persists throughout the process.
	EphemeralCommitKVStore interface {
		// rootmulti.Commit()
		Commit()

		EphemeralKVStore
	}

	// EphemeralCacheKVStore acts as a Cache, recording the changes made so far
	// and writing them to the parent via the Write() method.
	// Its lifecycle is aligned with CacheMultiStore.
	EphemeralCacheKVStore interface {
		// cachemulti.Write()
		Write()

		EphemeralKVStore
	}
)

type (
	// EphemeralKVStore defines a key-value store for ephemeral data storage.
	// Values are stored as `any` type, allowing flexible storage and management of various data types.
	EphemeralKVStore interface {
		// value = nil -> not found
		Get(key []byte) Sized
		//
		Set(key []byte, value Sized)
		//
		Delete(key []byte)

		//
		Iterator(start, end []byte) EphemeralIterator
		//
		ReverseIterator(start, end []byte) EphemeralIterator

		// cachekv.New(self)
		Branch() EphemeralCacheKVStore
	}

	// re-export any iterator
	EphemeralIterator = TypedEphemeralIterator[Sized]
	Sized             = internal.Sized
)
