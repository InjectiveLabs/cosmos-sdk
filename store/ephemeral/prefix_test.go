package ephemeral

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// Simple utility function for testing
func bz(s string) []byte { return []byte(s) }

// TestPrefixEphemeralStore_BasicOperations tests basic operations (Get, Set, Delete)
func TestPrefixEphemeralStore_BasicOperations(t *testing.T) {
	// Create basic EphemeralStore
	tree := NewTree()

	// Set up initial data
	setupBatch := tree.NewBatch()
	setupBatch.Set(bz("prefixkey1"), "value1")
	setupBatch.Set(bz("prefix"), "root")
	setupBatch.Set(bz("prefixkey2"), "value2")
	setupBatch.Set(bz("other"), "other")
	setupBatch.Commit()

	// Create prefix store
	prefix := bz("prefix")
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Fetch with new batch
	batch := prefixStore.NewBatch()

	// When the key is prefix, the value should be root
	require.Equal(t, "root", batch.Get(bz("")))

	// prefixkey1 -> key1, value should be correctly fetched
	require.Equal(t, "value1", batch.Get(bz("key1")))

	// prefixkey2 -> key2, value should be correctly fetched
	require.Equal(t, "value2", batch.Get(bz("key2")))

	// Keys outside of prefix range should not be fetched
	require.Nil(t, batch.Get(bz("other")))

	// Test nil key
	require.Panics(t, func() {
		batch.Get(nil)
	})

	// Test setting values
	batch.Set(bz("newkey"), "newvalue")
	require.Equal(t, "newvalue", batch.Get(bz("newkey")))

	// Panic when setting nil key
	require.Panics(t, func() {
		batch.Set(nil, "value")
	})

	// After commit, verify with a new batch
	batch.Commit()

	// Create a new batch
	newBatch := prefixStore.NewBatch()
	require.Equal(t, "newvalue", newBatch.Get(bz("newkey")))

	// Verify with prefixed key in base store
	baseBatch := tree.NewBatch()
	prefixedKey := append(prefix, bz("newkey")...)
	require.Equal(t, "newvalue", baseBatch.Get(prefixedKey))

	// Delete test
	deleteBatch := prefixStore.NewBatch()
	deleteBatch.Delete(bz("newkey"))
	deleteBatch.Commit()

	// Check
	checkBatch := prefixStore.NewBatch()
	require.Nil(t, checkBatch.Get(bz("newkey")))

	// Verify in base store as well
	baseBatch = tree.NewBatch()
	require.Nil(t, baseBatch.Get(prefixedKey))
}

// TestPrefixEphemeralStore_Iterator tests the iterator functionality
func TestPrefixEphemeralStore_Iterator(t *testing.T) {
	// Setup base store
	tree := NewTree()
	prefix := bz("prefix")

	// Set data
	setupBatch := tree.NewBatch()
	setupBatch.Set(bz("prefix1"), "value1")
	setupBatch.Set(bz("prefix2"), "value2")
	setupBatch.Set(bz("prefix3"), "value3")
	setupBatch.Set(bz("other"), "other")
	setupBatch.Commit()

	// Create prefix store
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Create batch for iterator testing
	batch := prefixStore.NewBatch()

	// Prefix store should iterate only items starting with the prefix
	iter := batch.Iterator(nil, nil)

	// Check the start and end domain
	start, end := iter.Domain()
	require.Nil(t, start)
	require.Nil(t, end)

	// Iterate through items starting with prefix
	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
		key := iter.Key()
		value := iter.Value()

		// Check keys without prefix
		switch string(key) {
		case "1":
			require.Equal(t, "value1", value)
		case "2":
			require.Equal(t, "value2", value)
		case "3":
			require.Equal(t, "value3", value)
		default:
			t.Errorf("Unexpected key: %s", key)
		}
	}

	// Ensure only 3 items starting with the prefix are checked
	require.Equal(t, 3, count)

	// Clean up resources
	iter.Close()

	// Range-based iterator test
	iter = batch.Iterator(bz("1"), bz("3"))

	// Check domain
	start, end = iter.Domain()
	require.Equal(t, bz("1"), start)
	require.Equal(t, bz("3"), end)

	// Verify range items
	count = 0
	for ; iter.Valid(); iter.Next() {
		count++
		key := iter.Key()
		value := iter.Value()

		switch string(key) {
		case "1":
			require.Equal(t, "value1", value)
		case "2":
			require.Equal(t, "value2", value)
		default:
			t.Errorf("Unexpected key: %s", key)
		}
	}

	// Only 2 items should be in the range
	require.Equal(t, 2, count)

	// Clean up resources
	iter.Close()
}

// TestPrefixEphemeralStore_ReverseIterator tests reverse iterator functionality
func TestPrefixEphemeralStore_ReverseIterator(t *testing.T) {
	// Setup base store
	tree := NewTree()
	prefix := bz("prefix")

	// Set data
	setupBatch := tree.NewBatch()
	setupBatch.Set(bz("prefix1"), "value1")
	setupBatch.Set(bz("prefix2"), "value2")
	setupBatch.Set(bz("prefix3"), "value3")
	setupBatch.Set(bz("other"), "other")
	setupBatch.Commit()

	// Create prefix store
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Create batch for reverse iterator testing
	batch := prefixStore.NewBatch()

	// Iterate in reverse over the entire range
	iter := batch.ReverseIterator(nil, nil)

	// Check the domain
	start, end := iter.Domain()
	require.Nil(t, start)
	require.Nil(t, end)

	// Reverse iteration
	expectedKeys := []string{"3", "2", "1"}
	count := 0

	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		require.Equal(t, expectedKeys[count], string(key))
		require.Equal(t, "value"+expectedKeys[count], value)
		count++
	}

	require.Equal(t, 3, count)

	// Clean up resources
	iter.Close()

	// Range-based reverse iterator
	iter = batch.ReverseIterator(bz("1"), bz("3"))

	// Check domain
	start, end = iter.Domain()
	require.Equal(t, bz("1"), start)
	require.Equal(t, bz("3"), end)

	// Verify range items in reverse
	expectedKeys = []string{"2", "1"}
	count = 0

	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		require.Equal(t, expectedKeys[count], string(key))
		require.Equal(t, "value"+expectedKeys[count], value)
		count++
	}

	require.Equal(t, 2, count)

	// Clean up resources
	iter.Close()
}

// TestPrefixEphemeralStore_NestedBatch tests nested batch operations
func TestPrefixEphemeralStore_NestedBatch(t *testing.T) {
	// Create base store
	tree := NewTree()
	prefix := bz("prefix")

	// Create prefix store
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Create parent batch
	parentBatch := prefixStore.NewBatch()

	// Set data in parent batch
	parentBatch.Set(bz("parent"), "parentvalue")

	// Create and test nested batch (scoped for resource management)
	{
		nestedBatch := parentBatch.NewNestedBatch()

		// Verify parent value in nested batch
		require.Equal(t, "parentvalue", nestedBatch.Get(bz("parent")))

		// Set new value in nested batch
		nestedBatch.Set(bz("nested"), "nestedvalue")

		// Nested batch can see the value
		require.Equal(t, "nestedvalue", nestedBatch.Get(bz("nested")))

		// Parent batch should not see it yet
		require.Nil(t, parentBatch.Get(bz("nested")))

		// Commit changes from nested batch to parent
		nestedBatch.Commit()
	}

	// After commit, parent batch should see nested changes
	require.Equal(t, "nestedvalue", parentBatch.Get(bz("nested")))

	// Commit parent batch
	parentBatch.Commit()

	// Verify commit in new batch
	checkBatch := prefixStore.NewBatch()
	require.Equal(t, "parentvalue", checkBatch.Get(bz("parent")))
	require.Equal(t, "nestedvalue", checkBatch.Get(bz("nested")))

	// Verify in base store (with prefix)
	baseBatch := tree.NewBatch()
	require.Equal(t, "parentvalue", baseBatch.Get(append(prefix, bz("parent")...)))
	require.Equal(t, "nestedvalue", baseBatch.Get(append(prefix, bz("nested")...)))
}

// TestPrefixEphemeralStore_GetSnapshotBatch tests snapshot batch functionality
func TestPrefixEphemeralStore_GetSnapshotBatch(t *testing.T) {
	// Create base store
	tree := NewTree()
	tree.SetSnapshotPoolLimit(10)

	// Create prefix store
	prefix := bz("prefix")
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Set initial data (height 1)
	batch1 := prefixStore.NewBatch()
	batch1.SetHeight(1)
	batch1.Set(bz("key"), "value1")
	batch1.Commit()

	// Set data with height 2
	batch2 := prefixStore.NewBatch()
	batch2.SetHeight(2)
	batch2.Set(bz("key"), "value2")
	batch2.Commit()

	// Get snapshot batch for height 1
	snapshot1, ok := prefixStore.GetSnapshotBatch(1)
	require.True(t, ok)
	require.Equal(t, "value1", snapshot1.Get(bz("key")))

	// Get snapshot batch for height 2
	snapshot2, ok := prefixStore.GetSnapshotBatch(2)
	require.True(t, ok)
	require.Equal(t, "value2", snapshot2.Get(bz("key")))

	// Snapshot for non-existent height
	snapshot3, ok := prefixStore.GetSnapshotBatch(3)
	require.False(t, ok)
	require.Nil(t, snapshot3)

	// Verify commit panic for snapshot
	require.Panics(t, func() {
		snapshot1.Commit()
	})
}

// TestPrefixEphemeralStore_EdgeCases tests edge cases
func TestPrefixEphemeralStore_EdgeCases(t *testing.T) {
	// Create base store
	tree := NewTree()

	// Test empty prefix
	emptyPrefix := []byte{}
	emptyPrefixStore := NewPrefixEphemeralStore(tree, emptyPrefix)

	// Create empty prefix batch
	batch := emptyPrefixStore.NewBatch()
	batch.Set(bz("key"), "value")
	batch.Commit()

	// Verify in base store
	baseBatch := tree.NewBatch()
	require.Equal(t, "value", baseBatch.Get(bz("key")))

	// Test edge case prefix (last byte is 0xFF)
	edgePrefix := []byte{0x01, 0xFF}
	edgePrefixStore := NewPrefixEphemeralStore(tree, edgePrefix)

	// Set data
	edgeBatch := edgePrefixStore.NewBatch()
	edgeBatch.Set(bz("key"), "edgevalue")
	edgeBatch.Commit()

	// Verify in base store
	baseBatch = tree.NewBatch()
	require.Equal(t, "edgevalue", baseBatch.Get(append(edgePrefix, bz("key")...)))

	// Verify different prefix can't access
	wrongPrefix := []byte{0x01, 0xFE}
	wrongPrefixStore := NewPrefixEphemeralStore(tree, wrongPrefix)
	wrongBatch := wrongPrefixStore.NewBatch()
	require.Nil(t, wrongBatch.Get(bz("key")))
}

// Utility for generating random key-value pairs
func genRandomKVPairs(t *testing.T, n int) []struct {
	key   []byte
	value []byte
} {
	t.Helper()
	kvs := make([]struct {
		key   []byte
		value []byte
	}, n)

	for i := 0; i < n; i++ {
		kvs[i].key = make([]byte, 16)
		_, err := rand.Read(kvs[i].key)
		require.NoError(t, err)

		kvs[i].value = make([]byte, 16)
		_, err = rand.Read(kvs[i].value)
		require.NoError(t, err)
	}

	return kvs
}

// TestPrefixEphemeralStore_RandomData tests wide range of tests with random data
func TestPrefixEphemeralStore_RandomData(t *testing.T) {
	// Create base store
	tree := NewTree()
	prefix := bz("prefix")
	prefixStore := NewPrefixEphemeralStore(tree, prefix)

	// Generate random key-value pairs
	kvs := genRandomKVPairs(t, 50)

	// Set data
	batch := prefixStore.NewBatch()
	for _, kv := range kvs {
		batch.Set(kv.key, kv.value)
	}
	batch.Commit()

	// Verify all key-value pairs
	checkBatch := prefixStore.NewBatch()
	for _, kv := range kvs {
		require.Equal(t, kv.value, checkBatch.Get(kv.key))
	}

	// Verify in base store (with prefix)
	baseBatch := tree.NewBatch()
	for _, kv := range kvs {
		prefixedKey := append(prefix, kv.key...)
		require.Equal(t, kv.value, baseBatch.Get(prefixedKey))
	}

	// Delete half of the keys
	deleteBatch := prefixStore.NewBatch()
	for i := 0; i < len(kvs)/2; i++ {
		deleteBatch.Delete(kvs[i].key)
	}
	deleteBatch.Commit()

	// Verify deletions
	finalBatch := prefixStore.NewBatch()
	for i, kv := range kvs {
		if i < len(kvs)/2 {
			require.Nil(t, finalBatch.Get(kv.key))
		} else {
			require.Equal(t, kv.value, finalBatch.Get(kv.key))
		}
	}
}
