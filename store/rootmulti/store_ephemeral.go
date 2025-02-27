package rootmulti

import (
	"cosmossdk.io/store/ephemeral"
	storetypes "cosmossdk.io/store/types"
)

func (rs *Store) GetEphemeralStore(storeKey storetypes.StoreKey) ephemeral.EphemeralStore {
	return ephemeral.NewPrefixEpheemeralKVStore(rs.ephemeralKVStore, storeKey.Name())
}

func (rs *Store) GetEphemeralKVStore(storeKey storetypes.StoreKey) ephemeral.EphemeralKVStore {
	return ephemeral.NewPrefixEpheemeralKVStore(rs.ephemeralKVStore, storeKey.Name())
}
