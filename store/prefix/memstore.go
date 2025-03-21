package prefix

import (
	"bytes"
	"errors"

	"cosmossdk.io/store/types"
)

// typedPrefixMemStore integrates prefix functionality with type support
type typedPrefixMemStore[T any] struct {
	parent types.MemStore
	prefix []byte
}

// NewMemStore creates a prefix memory store that supports generic type T
func NewMemStore[T any](parent types.MemStore, prefix []byte) *typedPrefixMemStore[T] {
	return &typedPrefixMemStore[T]{
		parent: parent,
		prefix: prefix,
	}
}

// key prefixes the given key with the store's prefix
func (b *typedPrefixMemStore[T]) key(key []byte) (res []byte) {
	if key == nil {
		panic("nil key on PrefixEphemeralBatch")
	}
	res = cloneAppend(b.prefix, key)
	return
}

// Get retrieves a value for the given key as type T
func (b *typedPrefixMemStore[T]) Get(key []byte) T {
	value := b.parent.Get(b.key(key))
	if value == nil {
		var zero T
		return zero
	}

	typedValue := value.(T)
	return typedValue
}

// Set adds or updates a key-value pair
func (b *typedPrefixMemStore[T]) Set(key []byte, value T) {
	b.parent.Set(b.key(key), value)
}

// Delete removes a key
func (b *typedPrefixMemStore[T]) Delete(key []byte) {
	b.parent.Delete(b.key(key))
}

// Commit applies the changes in the current batch
func (b *typedPrefixMemStore[T]) Commit() {
	b.parent.Commit()
}

// Branch creates a nested branch
func (b *typedPrefixMemStore[T]) Branch() *typedPrefixMemStore[T] {
	return &typedPrefixMemStore[T]{
		parent: b.parent.Branch(),
		prefix: b.prefix,
	}
}

// Iterator returns an iterator over the key-value pairs within the specified range
func (b *typedPrefixMemStore[T]) Iterator(start, end []byte) *typedPrefixMemStoreIterator[T] {
	var newStart, newEnd []byte

	if start == nil {
		newStart = b.prefix
	} else {
		newStart = cloneAppend(b.prefix, start)
	}

	if end == nil {
		newEnd = cpIncr(b.prefix)
	} else {
		newEnd = cloneAppend(b.prefix, end)
	}

	iter := b.parent.Iterator(newStart, newEnd)

	return newTypedPrefixMemStoreIterator[T](b.prefix, start, end, iter)
}

// ReverseIterator returns an iterator over the key-value pairs in reverse order
func (b *typedPrefixMemStore[T]) ReverseIterator(start, end []byte) *typedPrefixMemStoreIterator[T] {
	var newStart, newEnd []byte

	if start == nil {
		newStart = b.prefix
	} else {
		newStart = cloneAppend(b.prefix, start)
	}

	if end == nil {
		newEnd = cpIncr(b.prefix)
	} else {
		newEnd = cloneAppend(b.prefix, end)
	}

	iter := b.parent.ReverseIterator(newStart, newEnd)

	return newTypedPrefixMemStoreIterator[T](b.prefix, start, end, iter)
}

// typedPrefixMemStoreIterator is an iterator that supports generic type T
type typedPrefixMemStoreIterator[T any] struct {
	prefix []byte
	start  []byte
	end    []byte
	iter   types.MemStoreIterator
	valid  bool
}

// newTypedPrefixMemStoreIterator creates a new typed prefix iterator
func newTypedPrefixMemStoreIterator[T any](prefix, start, end []byte, parent types.MemStoreIterator) *typedPrefixMemStoreIterator[T] {
	valid := parent.Valid() && bytes.HasPrefix(parent.Key(), prefix)
	return &typedPrefixMemStoreIterator[T]{
		prefix: prefix,
		start:  start,
		end:    end,
		iter:   parent,
		valid:  valid,
	}
}

// Domain returns the start and end keys
func (pi *typedPrefixMemStoreIterator[T]) Domain() ([]byte, []byte) {
	return pi.start, pi.end
}

// Valid returns whether the iterator is positioned at a valid item
func (pi *typedPrefixMemStoreIterator[T]) Valid() bool {
	return pi.valid && pi.iter.Valid()
}

// Next moves to the next item
func (pi *typedPrefixMemStoreIterator[T]) Next() {
	if !pi.valid {
		panic("prefixIterator invalid, cannot call Next()")
	}

	pi.iter.Next()
	if !pi.iter.Valid() || !bytes.HasPrefix(pi.iter.Key(), pi.prefix) {
		pi.valid = false
	}
}

// Key returns the current key with the prefix stripped
func (pi *typedPrefixMemStoreIterator[T]) Key() []byte {
	if !pi.valid {
		panic("prefixIterator invalid, cannot call Key()")
	}

	key := pi.iter.Key()
	return stripPrefix(key, pi.prefix)
}

// Value returns the current value as type T
func (pi *typedPrefixMemStoreIterator[T]) Value() T {
	if !pi.valid {
		var zero T
		return zero
	}

	value := pi.iter.Value()
	return value.(T)
}

// Close releases resources
func (pi *typedPrefixMemStoreIterator[T]) Close() error {
	return pi.iter.Close()
}

// Error returns an error if the iterator is invalid
func (pi *typedPrefixMemStoreIterator[T]) Error() error {
	if !pi.Valid() {
		return errors.New("invalid prefixIterator")
	}

	return nil
}
