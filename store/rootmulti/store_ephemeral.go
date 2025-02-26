package rootmulti

import (
	"cosmossdk.io/store/ephemeral"
	storetypes "cosmossdk.io/store/types"
)

func (rs *Store) GetEphemeralStore(storetypes.StoreKey) ephemeral.EphemeralStore {
	panic("asdf")
}

func (rs *Store) GetEphemeralKVStore(storetypes.StoreKey) ephemeral.EphemeralKVStore {
	panic("asdf")
}
