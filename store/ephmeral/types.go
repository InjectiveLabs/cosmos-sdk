package ephemeral

type (
	EphemeralStore interface {
		Branch() EphemeralCacheStore
		Commit() // like CacheMultiStore.(Commiter).Commit()
	}

	EphemeralKVStore = TypedEphemeralKVStore[any]

	TypedEphemeralKVStore[Value any] interface {
		EphemeralStore

		Get(key []byte) *Value

		Set(key []byte, value *Value)
		Delete(key []byte)

		Iterator(start, end []byte) TypedEphemeralIterator[Value]
		ReverseIterator(start, end []byte) TypedEphemeralIterator[Value]
	}

	TypedEphemeralIterator[Value any] interface {
		Domain() (start []byte, end []byte)

		Valid() bool

		Next()

		Key() (key []byte)
		Value() (value *Value)

		Error() error

		Close() error
	}

	EphemeralCacheStore interface {
		EphemeralStore
		Write() // like CacheMultiStore.Write()
	}
)
