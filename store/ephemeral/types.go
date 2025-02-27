package ephemeral

type EphemeralStore interface {
	Branch() EphemeralCacheStore
}

type EphemeralCommitStore interface {
	Commit()
}

type EphemeralKVStore interface {
	EphemeralStore
	Set(key []byte, value any)
	Get(key []byte) any
	Delete(key []byte)

	Iterator(start, end []byte) EphemeralIterator
	ReverseIterator(start, end []byte) EphemeralIterator
}

type EphemeralCacheStore interface {
	EphemeralKVStore
	Write()
}

type EphemeralIterator = TypedEphemeralIterator[any]

func NewEphemeralStore() EphemeralStore {
	panic("Asdfsda")
}

func NewPrefixEpheemeralKVStore(ephemeralStore EphemeralStore, prefix string) EphemeralKVStore {
	panic("Asdfsda")
}

func NewEphemeralKVStore() EphemeralKVStore {
	panic("Asdfsda")
}
