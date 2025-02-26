package ephemeral

type EphemeralBackend interface {
	EphemeralStore
}

const (
	EphemeralBackendType         = EphemeralBackendTypeBTree // default: btree
	EphemeralBackendTypeBTree    = "btree"
	EphemeralBackendTypeSkipList = "skiplist"
	EphemeralBackendTypeMap      = "map" // test only
)

func NewEphemeralBackend() EphemeralBackend {
	switch EphemeralBackendType {
	case EphemeralBackendTypeBTree:
		return NewEphemeralBackendBTree()
	case EphemeralBackendTypeMap:
		return NewEphemeralBackendMap()
	default:
		panic("unknown ephemeral backend type")
	}
}
