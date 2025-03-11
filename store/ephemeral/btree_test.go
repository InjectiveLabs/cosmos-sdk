package ephemeral

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (t *Tree) get(key string) any {
	return t.root.Load().Get([]byte(key))
}

// TestTreeBatchNestedBatch verifies the relationship between tree, L1 batch, and L2 batch.
// It ensures that changes propagate correctly from L2 to L1 to tree root.
func TestTreeBatchNestedBatch(t *testing.T) {
	// 1. Create tree and verify initial state
	tree := NewTree()
	val := tree.get("a")
	require.Nil(t, val, "tree should not have key 'a' before any batch")

	// 2. Create L1 (top-level) batch and set key "a"
	batchL1 := tree.NewBatch()
	batchL1.Set([]byte("a"), "alpha")
	val = batchL1.Get([]byte("a"))
	require.Equal(t, "alpha", val, "L1 key 'a' should be 'alpha'")

	{ // 3. Create L2 (nested) batch from L1, set key "b", and delete key "a"
		batchL2 := batchL1.NewNestedBatch()
		batchL2.Set([]byte("b"), "beta")
		batchL2.Delete([]byte("a"))
		// Commit L2: L1's current pointer should be updated
		batchL2.Commit()
	}

	// Verify L1 state: key "a" should be deleted and key "b" should be added
	val = batchL1.Get([]byte("a"))
	require.Nil(t, val, "L1 should not have key 'a' after L2 commit")
	val = batchL1.Get([]byte("b"))
	require.Equal(t, "beta", val, "L1 key 'b' should be 'beta' after L2 commit")

	// 4. Commit L1: Tree's root pointer should change
	originalRoot := tree.root.Load()
	batchL1.Commit()
	newRoot := tree.root.Load()
	require.NotEqual(t, originalRoot, newRoot, "tree's root pointer should change after L1 commit")

	// Verify tree state: L1's changes should be reflected
	val = tree.get("a")
	require.Nil(t, val, "tree should not have key 'a' after L1 commit")
	val = tree.get("b")
	require.Equal(t, "beta", val, "tree key 'b' should be 'beta' after L1 commit")
}

// TestWriterReaderInconsistency tests potential inconsistency between reader and writer
// when creating an L1 batch.
func TestWriterReaderInconsistency(t *testing.T) {
	// Create tree
	tree := NewTree()

	// Add initial data
	batchInitial := tree.NewBatch()
	batchInitial.Set([]byte("key"), "initial-value")
	batchInitial.Commit()

	// Create new batch - at this point reader has data but writer is empty
	batchL1 := tree.NewBatch()

	// This value should be read from reader - "initial-value" should exist
	val := batchL1.Get([]byte("key"))
	require.Equal(t, "initial-value", val, "Initial read should return existing value")

	// Set new value in writer
	batchL1.Set([]byte("key2"), "new-value")

	// Try to delete existing key - exists in reader but not in writer
	batchL1.Delete([]byte("key"))

	// Commit changes
	batchL1.Commit()

	// Verify final state
	batchCheck := tree.NewBatch()
	val = batchCheck.Get([]byte("key"))
	require.Nil(t, val, "Key should be deleted")
	val = batchCheck.Get([]byte("key2"))
	require.Equal(t, "new-value", val, "New key should be present")
}

// TestConcurrencyL1Batch tests creation of multiple L1 batches concurrently.
// It ensures each batch maintains its own isolated view of the tree.
func TestConcurrencyL1Batch(t *testing.T) {
	// Create tree
	tree := NewTree()

	// Verify initial state
	require.Nil(t, tree.get("key"), "Tree should be empty initially")

	// Create multiple L1 batches
	const numBatches = 10_000
	batches := make([]EphemeralBatch, numBatches)

	// Set the same key with different values in each batch
	wg := &sync.WaitGroup{}
	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(i int) {
			batches[i] = tree.NewBatch()
			batches[i].Set([]byte("key"), fmt.Sprintf("value-%d", i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Verify each batch's state
	for i, batch := range batches {
		val := batch.Get([]byte("key"))
		require.Equal(t, fmt.Sprintf("value-%d", i), val, "Each batch should maintain its own independent value")
	}

	// Commit only one batch (index 2)
	selectedIndex := 2
	batches[selectedIndex].Commit()

	// Don't commit other batches
	_ = batches

	// Verify final tree state
	finalValue := tree.get("key")
	require.Equal(t, fmt.Sprintf("value-%d", selectedIndex), finalValue,
		"Only the value from committed batch should be reflected in the tree")
}

// TestConcurrentBatchCreationWithCommits tests the safety of creating L1 batches
// during continuous commits from another goroutine.
// This simulates heavy concurrency with one thread committing and many creating batches.
func TestConcurrentBatchCreationWithCommits(t *testing.T) {
	tree := NewTree()

	// Initialize tree with some data
	initialBatch := tree.NewBatch()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		initialBatch.Set([]byte(key), fmt.Sprintf("init-value-%d", i))
	}
	initialBatch.Commit()

	// Number of worker goroutines creating batches
	const numWorkers = 50
	// Number of batches each worker will create
	const batchesPerWorker = 20
	// Number of commit operations the committer will perform
	const numCommits = 100

	// Channels for coordination
	startWorkers := make(chan struct{})
	stopCommitter := make(chan struct{})

	// Track successful batch creations
	var createdBatches sync.WaitGroup
	createdBatches.Add(numWorkers * batchesPerWorker)

	// Track batches created
	var batchCounter atomic.Uint64
	batchCreated := func() {
		batchCounter.Add(1)
		createdBatches.Done()
	}

	// Committer goroutine that continuously commits batches
	go func() {
		<-startWorkers // Wait for workers to start

		commitCount := 0
		for {
			select {
			case <-stopCommitter:
				return
			default:
				// Create and commit a batch
				batch := tree.NewBatch()
				key := fmt.Sprintf("commit-%d", commitCount)
				batch.Set([]byte(key), fmt.Sprintf("committed-value-%d", commitCount))

				// Small random delay to increase chance of race conditions
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(5)))

				// Commit the batch
				batch.Commit()
				commitCount++

				if commitCount >= numCommits {
					// We've done enough commits
					return
				}
			}
		}
	}()

	// Worker goroutines that create batches
	for worker := 0; worker < numWorkers; worker++ {
		go func(workerID int) {
			// Each worker creates multiple batches
			for i := 0; i < batchesPerWorker; i++ {
				// Small random delay to increase concurrency variations
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(3)))

				// Create a batch
				batch := tree.NewBatch()

				// Ensure the batch is valid
				assert.NotNil(t, batch, fmt.Errorf("worker %d: nil batch created", workerID))
				if batch == nil {
					batchCreated()
					continue
				}

				// Test basic operations
				key := fmt.Sprintf("worker-%d-batch-%d", workerID, i)
				value := fmt.Sprintf("value-%d-%d", workerID, i)

				// Try setting a value
				batch.Set([]byte(key), value)

				// Attempt to read it back
				readValue := batch.Get([]byte(key))
				assert.Equal(t, readValue, value, fmt.Errorf("worker %d: batch %d failed to read written value", workerID, i))

				// Try reading an init key that should be present in all batches
				initKey := "init-key-0"
				assert.Equal(t, batch.Get([]byte(initKey)), "init-value-0", fmt.Errorf("worker %d: batch %d failed to read initial value", workerID, i))

				// Mark this batch as created and tested
				batchCreated()
			}
		}(worker)
	}

	// Start all workers
	close(startWorkers)

	// Wait for all batches to be created
	done := make(chan struct{})
	go func() {
		createdBatches.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		// All batches created successfully
	case <-time.After(30 * time.Second):
		t.Fatalf("Test timed out waiting for batch creations. Created %d/%d batches",
			batchCounter.Load(), numWorkers*batchesPerWorker)
	}

	// Stop the committer
	close(stopCommitter)

	// Verify final tree state contains some committed values
	lastBatch := tree.NewBatch()

	// Initial values should still be present
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value := lastBatch.Get([]byte(key))
		require.Equal(t, value, fmt.Sprintf("init-value-%d", i), fmt.Errorf("Initial value missing or incorrect for key %s: got %v", key, value))
	}

	// At least some committed values should be present
	start := []byte("commit-")
	end := []byte("commit-" + fmt.Sprintf("%d", numCommits))
	iter := lastBatch.Iterator(start, end)
	defer iter.Close()

	commitValuesFound := 0
	for ; iter.Valid(); iter.Next() {
		commitValuesFound++

		require.True(t, strings.HasPrefix(string(iter.Key()), "commit-"))
	}

	t.Logf("Found %d/%d committed values in final tree state", commitValuesFound, numCommits)
	require.True(t, commitValuesFound > 0, "Should find at least some committed values")
}

// TestUncommittedL2BatchChanges verifies that changes in an uncommitted L2 batch
// are not propagated to its parent L1 batch.
func TestUncommittedL2BatchChanges(t *testing.T) {
	// Create tree
	tree := NewTree()

	// Create L1 batch and set initial data
	batchL1 := tree.NewBatch()
	batchL1.Set([]byte("key1"), "original-value")

	// Verify initial L1 state
	val := batchL1.Get([]byte("key1"))
	require.Equal(t, "original-value", val, "L1 key1 should have original value")

	// Create L2 batch (limited scope with curly braces)
	{
		// Create L2 batch
		batchL2 := batchL1.NewNestedBatch()

		// Modify existing key in L2
		batchL2.Set([]byte("key1"), "modified-value")

		// Add new key in L2
		batchL2.Set([]byte("key2"), "new-key-value")

		// Verify values in L2
		val = batchL2.Get([]byte("key1"))
		require.Equal(t, "modified-value", val, "L2 should have modified key1 value")
		val = batchL2.Get([]byte("key2"))
		require.Equal(t, "new-key-value", val, "L2 should have new key2 value")

		// L2 batch is not committed - when the scope ends, the variable goes out of scope
		// Without committing L2 batch, changes should not be reflected in L1
	}

	// Verify L1 state - L2's changes should not be applied
	val = batchL1.Get([]byte("key1"))
	require.Equal(t, "original-value", val, "L1 key1 should still have original value")

	// Key added in L2 should not be present in L1
	val = batchL1.Get([]byte("key2"))
	require.Nil(t, val, "L1 should not have key2 as L2 was not committed")

	// Commit L1 batch
	batchL1.Commit()

	// Verify tree state - only L1's changes should be applied, not L2's
	val = tree.get("key1")
	require.Equal(t, "original-value", val, "Tree should have L1's original value for key1")

	val = tree.get("key2")
	require.Nil(t, val, "Tree should not have key2 as it was only in uncommitted L2")
}

// TestUncommittedL1BatchChanges verifies that changes in an uncommitted L1 batch
// are not propagated to the tree root.
func TestUncommittedL1BatchChanges(t *testing.T) {
	// Create tree
	tree := NewTree()

	// Add initial data (for comparison)
	initialBatch := tree.NewBatch()
	initialBatch.Set([]byte("initial-key"), "initial-value")
	initialBatch.Set([]byte("to-delete"), "temp-value") // Add key to be deleted later
	initialBatch.Commit()

	// Verify initial tree state
	val := tree.get("initial-key")
	require.Equal(t, "initial-value", val, "Tree should have initial value")

	{ // Create L1 batch and set data (limited scope)
		batchL1 := tree.NewBatch()

		// Modify existing key
		batchL1.Set([]byte("initial-key"), "modified-value")

		// Add new key
		batchL1.Set([]byte("new-key"), "new-value")

		// Delete key
		batchL1.Delete([]byte("to-delete"))

		// Verify L1 batch state
		val = batchL1.Get([]byte("initial-key"))
		require.Equal(t, "modified-value", val, "L1 batch should have modified value")

		val = batchL1.Get([]byte("new-key"))
		require.Equal(t, "new-value", val, "L1 batch should have new key")

		val = batchL1.Get([]byte("to-delete"))
		require.Nil(t, val, "L1 batch should not have deleted key")

		// L1 batch is not committed
		// L1 batch changes should not be reflected in the tree
	}

	// Verify tree state - L1's changes should not be applied
	val = tree.get("initial-key")
	require.Equal(t, "initial-value", val, "Tree should still have initial value")

	val = tree.get("new-key")
	require.Nil(t, val, "Tree should not have new key as L1 was not committed")

	val = tree.get("to-delete")
	require.Equal(t, "temp-value", val, "Tree should still have key that was deleted in uncommitted batch")

	// Verify tree state with a new batch
	checkBatch := tree.NewBatch()
	val = checkBatch.Get([]byte("initial-key"))
	require.Equal(t, "initial-value", val, "New batch should see initial value in tree")

	val = checkBatch.Get([]byte("to-delete"))
	require.Equal(t, "temp-value", val, "New batch should still see key that was deleted in uncommitted batch")
}

// TestBatchIteratorIsolation verifies that an iterator from one batch does not see
// write operations performed in another concurrent batch.
// It demonstrates the point-in-time view property of iterators.
func TestBatchIteratorIsolation(t *testing.T) {
	// Create tree with initial data
	tree := NewTree()
	setupBatch := tree.NewBatch()

	// Add initial data to tree
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("init-key-%d", i))
		value := fmt.Sprintf("init-value-%d", i)
		setupBatch.Set(key, value)
	}
	setupBatch.Commit()

	// Create two L1 batches concurrently - both have same initial view
	batch1 := tree.NewBatch()
	batch2 := tree.NewBatch()

	// Verify both batches see the same initial state
	val1 := batch1.Get([]byte("init-key-5"))
	val2 := batch2.Get([]byte("init-key-5"))
	require.Equal(t, val1, val2, "Both batches should start with identical view")

	// Create iterator from batch2 before any modifications
	iter := batch2.Iterator(nil, nil)

	// Modify data in batch1
	batch1.Set([]byte("new-key"), "new-value")
	batch1.Set([]byte("init-key-5"), "modified-in-batch1")
	batch1.Delete([]byte("init-key-3"))

	// Collect all keys/values from batch2's iterator
	iteratorPairs := make(map[string]any)
	for ; iter.Valid(); iter.Next() {
		iteratorPairs[string(iter.Key())] = iter.Value()
	}

	// Verify iterator doesn't see batch1's changes
	require.NotContains(t, iteratorPairs, "new-key", "Iterator should not see keys added in other batch")
	require.Equal(t, "init-value-5", iteratorPairs["init-key-5"], "Iterator should see original values, not modified")
	require.Contains(t, iteratorPairs, "init-key-3", "Iterator should still see deleted keys")

	// Verify direct Get on batch2 also doesn't see changes
	require.Nil(t, batch2.Get([]byte("new-key")), "batch2 Get should not see new key from batch1")
	require.Equal(t, "init-value-5", batch2.Get([]byte("init-key-5")), "batch2 Get should see original values")
	require.NotNil(t, batch2.Get([]byte("init-key-3")), "batch2 Get should still see deleted key")

	// Commit batch1
	batch1.Commit()

	// Verify changes are in the tree
	require.Equal(t, "new-value", tree.NewBatch().Get([]byte("new-key")), "Tree should have new key after batch1 commit")
	require.Equal(t, "modified-in-batch1", tree.NewBatch().Get([]byte("init-key-5")), "Tree should have modified value")
	require.Nil(t, tree.NewBatch().Get([]byte("init-key-3")), "Tree should not have deleted key")

	// Create a new iterator on batch2 and verify it still doesn't see changes
	// (batch2 maintains its point-in-time view even after batch1 is committed)
	iter2 := batch2.Iterator(nil, nil)

	iteratorPairs2 := make(map[string]any)
	for ; iter2.Valid(); iter2.Next() {
		iteratorPairs2[string(iter2.Key())] = iter2.Value()
	}

	require.NotContains(t, iteratorPairs2, "new-key", "New iterator should still not see changes from batch1")
	require.Equal(t, "init-value-5", iteratorPairs2["init-key-5"], "New iterator should see original values")
	require.Contains(t, iteratorPairs2, "init-key-3", "New iterator should still see deleted key")
}

// TestSnapshotPool verifies the functionality of height maps and snapshot retrieval.
// It ensures that batches can be created with specific heights and later retrieved correctly.
func TestSnapshotPool(t *testing.T) {
	// Create tree
	tree := NewTree()
	tree.SetSnapshotPoolLimit(10)

	// Add data at different heights
	heights := []int64{10, 11, 12}
	expectedValues := make(map[int64]string)

	for _, height := range heights {
		// Create batch with height
		batch := tree.NewBatch()
		batch.SetHeight(height)

		// Set height-specific value
		key := []byte("key")
		value := fmt.Sprintf("value-at-height-%d", height)
		batch.Set(key, value)

		// Store expected value for later verification
		expectedValues[height] = value

		// Commit batch
		batch.Commit()
	}

	// Get snapshots at each height and verify values
	for _, height := range heights {
		snapshotBatch, found := tree.GetSnapshotBatch(height)
		require.True(t, found, "Should find snapshot for height %d", height)

		// Check correct value at this height
		val := snapshotBatch.Get([]byte("key"))
		require.Equal(t, expectedValues[height], val, "Snapshot at height %d should have correct value", height)

		// Verify we get UncommittableBatch that panics on commit
		require.Panics(t, func() {
			snapshotBatch.Commit()
		}, "Snapshot batch should panic on commit attempt")
	}

	// Verify requesting non-existent height returns false
	_, found := tree.GetSnapshotBatch(999)
	require.False(t, found, "Should not find snapshot for non-existent height")

	// Test that snapshot batches reflect the state at exactly that height
	// by creating multiple batches at same height with different changes
	finalHeight := int64(100)

	// First batch at height 100
	batch1 := tree.NewBatch()
	batch1.SetHeight(finalHeight)
	batch1.Set([]byte("multi-key-1"), "first-batch")
	batch1.Commit()

	// Second batch at height 100 (should overwrite the snapshot)
	batch2 := tree.NewBatch()
	batch2.SetHeight(finalHeight)
	batch2.Set([]byte("multi-key-1"), "second-batch")
	batch2.Set([]byte("multi-key-2"), "additional-value")
	batch2.Commit()

	// Get snapshot and verify it reflects the second batch
	snapshotBatch, found := tree.GetSnapshotBatch(finalHeight)
	require.True(t, found, "Should find snapshot for final height")

	val := snapshotBatch.Get([]byte("multi-key-1"))
	require.Equal(t, "second-batch", val, "Snapshot should reflect the latest commit at height")

	val = snapshotBatch.Get([]byte("multi-key-2"))
	require.Equal(t, "additional-value", val, "Snapshot should contain all keys from last commit")
}

// TestNestedSnapshotBatches tests behavior with deeply nested batches and height management
func TestNestedSnapshotBatches(t *testing.T) {
	tree := NewTree()

	// Create L1 with height 100
	batchL1 := tree.NewBatch()
	batchL1.SetHeight(100)
	batchL1.Set([]byte("key"), "L1-value")

	// Create L2 from L1
	batchL2 := batchL1.NewNestedBatch()
	batchL2.Set([]byte("key"), "L2-value")

	// Create L3 from L2
	batchL3 := batchL2.NewNestedBatch()
	batchL3.Set([]byte("key"), "L3-value")

	// Commit from L3 up to L1
	batchL3.Commit() // L3 → L2
	batchL2.Commit() // L2 → L1
	batchL1.Commit() // L1 → tree

	// Verify final value in tree
	currentBatch := tree.NewBatch()
	val := currentBatch.Get([]byte("key"))
	require.Equal(t, "L3-value", val, "Tree should have value from L3 after cascading commits")

	// Get snapshot at height 100
	snapshotBatch, found := tree.GetSnapshotBatch(100)
	require.True(t, found, "Should find snapshot at height 100")

	val = snapshotBatch.Get([]byte("key"))
	require.Equal(t, "L3-value", val, "Snapshot should have L3 value")

	// Try modifying a snapshot batch (should work, but not affect original)
	snapshotBatch.Set([]byte("key"), "modified-snapshot")
	val = snapshotBatch.Get([]byte("key"))
	require.Equal(t, "modified-snapshot", val, "Can modify snapshot batch locally")

	// But original snapshot should be unchanged
	newSnapshotBatch, _ := tree.GetSnapshotBatch(100)
	val = newSnapshotBatch.Get([]byte("key"))
	require.Equal(t, "L3-value", val, "Original snapshot should be unchanged")
}

// cpu: Apple M2 Pro
// BenchmarkTreeBatchSet-12    	  244624	      5341 ns/op	   12467 B/op	      34 allocs/op
func BenchmarkTreeBatchSet(b *testing.B) {
	b.ReportAllocs()
	tree := NewTree()
	batch := tree.NewBatch()
	for i := 0; i < 50_000_000; i++ {
		batch.Set(
			[]byte(fmt.Sprintf("key-%d", i)),
			fmt.Sprintf("value-%d", i),
		)
	}
	batch.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := tree.NewBatch()
		batch.Set(
			[]byte(fmt.Sprintf("key-%d", i)),
			fmt.Sprintf("value-%d", i),
		)
		batch.Commit()
	}
}
