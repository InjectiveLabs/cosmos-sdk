package ephemeral

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type testItem struct {
	key  []byte
	item interface{}
}

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

func makeTestItem(key int, value interface{}) *testItem {
	return &testItem{
		key:  []byte(fmt.Sprintf("%d", key)),
		item: value,
	}
}

func TestEphemeralCacheKVStore(t *testing.T) {
	testItems := make([]*testItem, 0)
	testItems = append(testItems, makeTestItem(0, &dummyItem[int]{value: int(1)}))
	testItems = append(testItems, makeTestItem(1, &dummyItem[int64]{value: int64(2)}))
	testItems = append(testItems, makeTestItem(2, &dummyItem[string]{value: "3"}))
	testItems = append(testItems, makeTestItem(3, &dummyItem[[]int]{value: []int{4, 5, 6}}))
	testItems = append(testItems, makeTestItem(4, &dummyItem[map[string]int]{value: map[string]int{"a": 1, "b": 2}}))

	set := func(store EphemeralKVStore, testItems []*testItem) {
		for i := 0; i < len(testItems); i++ {
			sized, ok := testItems[i].item.(Sized)
			if !ok {
				t.Fatalf("item must implement Sized interface")
			}
			store.Set(testItems[i].key, sized)
		}
	}

	verifyExist := func(store EphemeralKVStore, testItems []*testItem) {
		for i := 0; i < len(testItems); i++ {
			item := store.Get(testItems[i].key)
			if item == nil {
				t.Fatalf("item should be in the store")
			}

			if !reflect.DeepEqual(item, testItems[i].item) {
				t.Fatalf("item is not equal to the original item")
			}
		}
	}

	verifyNotExist := func(store EphemeralKVStore, testItems []*testItem) {
		for i := 0; i < len(testItems); i++ {
			item := store.Get(testItems[i].key)
			if item != nil {
				t.Fatalf("item should not be in the store")
			}
		}
	}

	t.Run("Branch: Write & Commit", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()

		// Set
		set(eCacheKVStore, testItems)

		// Write & Commit
		eCacheKVStore.Write()
		eCommitKVStore.Commit()

		// Get from the underlying store
		verifyExist(eCommitKVStore, testItems)
	})

	t.Run("Branch: Drop", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()

		// Set
		set(eCacheKVStore, testItems)

		// Get from the underlying store
		verifyNotExist(eCommitKVStore, testItems)
	})

	t.Run("Branch: Write & Commit with multi layer caches", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()
		eCacheKVStore := eCommitKVStore.Branch()
		eCacheKVStore2 := eCacheKVStore.Branch()

		// Set to cache layer 2
		set(eCacheKVStore2, testItems)

		// Write
		eCacheKVStore2.Write()

		// Get from cache layer 1
		verifyExist(eCacheKVStore, testItems)

		// Get from EphemeralBackend
		verifyNotExist(eCommitKVStore, testItems)

		// Write & Commit
		eCacheKVStore.Write()
		eCommitKVStore.Commit()

		// Get from EphemeralBackend
		verifyExist(eCommitKVStore, testItems)
	})

	t.Run("Branch: Write & Commit with living iterator", func(t *testing.T) {
		eCommitKVStore := NewEphemeralBackend()

		// Set to first cache
		firstCacheKVStore := eCommitKVStore.Branch()
		set(firstCacheKVStore, testItems[:3])

		// Write & Commit
		firstCacheKVStore.Write()
		eCommitKVStore.Commit()

		// Get from EphemeralBackend
		firstCacheItems := testItems[:3]
		verifyExist(eCommitKVStore, firstCacheItems)

		// get iterator with the first cache items
		itr := eCommitKVStore.Iterator(nil, nil)
		defer itr.Close()

		// Set to second cache
		secondCacheKVStore := eCommitKVStore.Branch()
		set(secondCacheKVStore, testItems[3:])

		// Write & Commit
		secondCacheKVStore.Write()
		eCommitKVStore.Commit()

		// get iterator with the test items
		itr2 := eCommitKVStore.Iterator(nil, nil)
		defer itr2.Close()

		// this iterator should have the same length as firstCacheItems
		i := 0
		for itr.Valid() {
			if i >= len(firstCacheItems) {
				t.Fatalf("iterator has more items than the original items")
			}

			item := itr.Value()
			if !reflect.DeepEqual(item, firstCacheItems[i].item) {
				t.Fatalf("item is not equal to the original item")
			}

			itr.Next()
			i++
		}
		require.Equal(t, i, len(firstCacheItems))

		// this iterator should have the same length as testItems
		i = 0
		for itr2.Valid() {
			if i >= len(testItems) {
				t.Fatalf("iterator has more items than the original items")
			}

			item := itr2.Value()
			if !reflect.DeepEqual(item, testItems[i].item) {
				t.Fatalf("item is not equal to the original item")
			}

			itr2.Next()
			i++
		}
		require.Equal(t, i, len(testItems))

	})

}
