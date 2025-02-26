package ephemeral

type EphemeralStore interface {
	Branch() EphemeralCacheStore
	Commit()
}

type EphemeralKVStore interface {
	EphemeralStore
	Set(key []byte, value any)
	Get(key []byte) any

	Iterator(start, end []byte)
	ReverseIterator(start, end []byte)
}

type EphemeralCacheStore interface {
	EphemeralStore
	Write()
}

func NewEphemeralStore() EphemeralStore {
	panic("Asdfsda")
}
