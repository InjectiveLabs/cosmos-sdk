package internal

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type testItem[T any] struct {
	value T
}

func (t *testItem[T]) Size() int {
	return 1
}

func TestBtree(t *testing.T) {
	bt := NewBTree()

	bt.Set([]byte("a"), &testItem[string]{value: "a"})

	val := bt.Get([]byte("a"))

	require.NotNil(t, val)
}
