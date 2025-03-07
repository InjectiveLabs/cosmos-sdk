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

func (hm *heightMap) Get(height int64) (EphemeralSnapshot, bool) {
	store, ok := hm.Load(height)
	if !ok {
		return nil, false
	}

	return store.(EphemeralSnapshot), true
}

func (hm *heightMap) Set(height int64, store EphemeralSnapshot) {
	hm.Store(height, store)
}
