package ephemeral

import (
	"fmt"
	"reflect"
	"testing"
)

type dummyItem[T any] struct {
	value T
}

func (d *dummyItem[T]) Size() int {
	return int(reflect.TypeOf(d.value).Size()) +
		calculateAdditionalSize(d.value)
}

func calculateAdditionalSize[T any](v T) int {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return len(rv.String())
	case reflect.Slice:
		elemSize := int(rv.Type().Elem().Size())
		return rv.Len() * elemSize
	case reflect.Map:
		return rv.Len() * (int(reflect.TypeOf(v).Key().Size()) +
			int(reflect.TypeOf(v).Elem().Size()))
	default:
		return 0
	}
}

func key(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func TestEphemeralCacheKVStore(t *testing.T) {
	dummyItems := make([]interface{}, 0)
	dummyItems = append(dummyItems, &dummyItem[int]{value: int(1)})
	dummyItems = append(dummyItems, &dummyItem[int64]{value: int64(2)})
	dummyItems = append(dummyItems, &dummyItem[string]{value: "3"})
	dummyItems = append(dummyItems, &dummyItem[[]int]{value: []int{4, 5, 6}})
	dummyItems = append(dummyItems, &dummyItem[map[string]int]{value: map[string]int{"a": 1, "b": 2}})

	set := func(store EphemeralKVStore, dummyItems []interface{}) {
		for i, item := range dummyItems {
			sized, ok := item.(Sized)
			if !ok {
				t.Fatalf("item must implement Sized interface")
			}
			store.Set(key(i), sized)
		}
	}

	verifyExist := func(store EphemeralKVStore, dummyItems []interface{}) {
		for i := 0; i < len(dummyItems); i++ {
			item := store.Get(key(i))
			if item == nil {
				t.Fatalf("item should be in the store")
			}

			if !reflect.DeepEqual(item, dummyItems[i]) {
				t.Fatalf("item is not equal to the original item")
			}
		}
	}

	verifyNotExist := func(store EphemeralKVStore, dummyItems []interface{}) {
		for i := 0; i < len(dummyItems); i++ {
			item := store.Get(key(i))
			if item != nil {
				t.Fatalf("item should not be in the store")
			}
		}
	}

	t.Run("Branch: Write & Commit", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()

		// Set
		set(eCacheKVStore, dummyItems)

		// Write & Commit
		eCacheKVStore.Write()
		eCommitKVStore.Commit()

		// Get from the underlying store
		verifyExist(eCommitKVStore, dummyItems)
	})

	t.Run("Branch: Drop", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()

		// Set
		set(eCacheKVStore, dummyItems)

		// Get from the underlying store
		verifyNotExist(eCommitKVStore, dummyItems)
	})

	t.Run("Branch: Write & Commit with multi layer caches", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()
		eCacheKVStore2 := eCacheKVStore.Branch()

		// Set to cache layer 2
		set(eCacheKVStore2, dummyItems)

		// Write
		eCacheKVStore2.Write()

		// Get from cache layer 1
		verifyExist(eCacheKVStore, dummyItems)

		// Get from EphemeralBackend
		verifyNotExist(eCommitKVStore, dummyItems)

		// Write & Commit
		eCacheKVStore.Write()
		eCommitKVStore.Commit()

		// Get from EphemeralBackend
		verifyExist(eCommitKVStore, dummyItems)
	})
}
