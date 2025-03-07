package ephemeral

import "sync"

type heightMap struct {
	*sync.Map
}

func newHeightMap() *heightMap {
	return &heightMap{
		Map: &sync.Map{},
	}
}

func (hm *heightMap) Get(height int64) (EphemeralStore, bool) {
	store, ok := hm.Load(height)
	if !ok {
		return nil, false
	}

	return store.(EphemeralStore), true
}

func (hm *heightMap) Set(height int64, store EphemeralStore) {
	hm.Store(height, store)
}
