package ephemeral

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (t *Tree) load() *btree {
	// Load the current root btree from the atomic pointer
	return t.root.Load()
}

func (t *Tree) get(key string) any {
	return t.load().Get([]byte(key))
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
	originalRoot := tree.load()
	batchL1.Commit()
	newRoot := tree.load()
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

// TestL1L2Interference tests consistency of L1 reading when L2 is committed.
// It ensures that concurrent reads see either the original or the final state.
func TestL1L2Interference(t *testing.T) {
	// Create tree and L1 batch
	tree := NewTree()
	batchL1 := tree.NewBatch()

	// Set initial data in L1
	batchL1.Set([]byte("shared"), "l1-original")

	// Create L2 batch and modify the same key
	batchL2 := batchL1.NewNestedBatch()
	batchL2.Set([]byte("shared"), "l2-modified")

	// Channels for read/write synchronization
	readyToRead := make(chan struct{})
	readDone := make(chan struct{})

	// Read continuously from L1 in a separate goroutine
	go func() {
		<-readyToRead // Wait for signal to start reading

		// Try reading multiple times during L2 commit
		for i := 0; i < 1000; i++ {
			val := batchL1.Get([]byte("shared"))
			// Value read should be either l1-original or l2-modified
			assert.Contains(
				t,
				[]any{"l1-original", "l2-modified"},
				val,
				"Read during commit should return either original or modified value",
			)
		}

		close(readDone)
	}()

	// Send signal to start reading
	close(readyToRead)

	// Wait briefly then commit L2
	time.Sleep(10 * time.Millisecond)
	batchL2.Commit()

	// Wait for reading to complete
	<-readDone

	// Verify final state
	val := batchL1.Get([]byte("shared"))
	require.Equal(t, "l2-modified", val, "L1 should have L2's value after commit")
}

// TestConcurrencyL1Batch tests creation of multiple L1 batches concurrently.
// It ensures each batch maintains its own isolated view of the tree.
func TestConcurrencyL1Batch(t *testing.T) {
	// Create tree
	tree := NewTree()

	// Verify initial state
	require.Nil(t, tree.get("key"), "Tree should be empty initially")

	// Create multiple L1 batches
	const numBatches = 5
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

// cpu: Apple M2 Pro
// BenchmarkTreeBatchSet-12    	  290637	      3805 ns/op	   10797 B/op	      30 allocs/op
func BenchmarkTreeBatchSet(b *testing.B) {
	b.ReportAllocs()
	tree := NewTree()

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
