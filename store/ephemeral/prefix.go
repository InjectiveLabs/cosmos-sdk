package ephemeral

import (
	"bytes"
	"errors"
)

var _ EphemeralBatch = (*PrefixEphemeralBatch)(nil)
var _ Iterator = (*prefixIterator)(nil)

func NewPrefixEphemeralBatch(parent EphemeralBatch, prefix []byte) *PrefixEphemeralBatch {
	return &PrefixEphemeralBatch{
		parent: parent,
		prefix: prefix,
	}
}

// PrefixEphemeralBatch is a wrapper for EphemeralBatch that prefixes all keys
type PrefixEphemeralBatch struct {
	parent EphemeralBatch
	prefix []byte
}

// cloneAppend makes a copy of bz and appends tail to it
func cloneAppend(bz, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}

// key prefixes the given key with the store's prefix
func (b *PrefixEphemeralBatch) key(key []byte) (res []byte) {
	if key == nil {
		panic("nil key on PrefixEphemeralBatch")
	}
	res = cloneAppend(b.prefix, key)
	return
}

// Get retrieves a value for the given key
func (b *PrefixEphemeralBatch) Get(key []byte) any {
	return b.parent.Get(b.key(key))
}

// Set adds or updates a key-value pair
func (b *PrefixEphemeralBatch) Set(key []byte, value any) {
	b.parent.Set(b.key(key), value)
}

// Delete removes a key
func (b *PrefixEphemeralBatch) Delete(key []byte) {
	b.parent.Delete(b.key(key))
}

// Commit applies the changes in the current batch
func (b *PrefixEphemeralBatch) Commit() {
	b.parent.Commit()
}

func (b *PrefixEphemeralBatch) SetHeight(height int64) {
	b.parent.SetHeight(height)
}

// NewNestedBatch creates a nested batch
func (b *PrefixEphemeralBatch) NewNestedBatch() EphemeralBatch {
	return &PrefixEphemeralBatch{
		parent: b.parent.NewNestedBatch(),
		prefix: b.prefix,
	}
}

// Iterator returns an iterator over the key-value pairs within the specified range
func (b *PrefixEphemeralBatch) Iterator(start, end []byte) Iterator {
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

	return newPrefixIterator(b.prefix, start, end, iter)
}

// ReverseIterator returns an iterator over the key-value pairs in reverse order
func (b *PrefixEphemeralBatch) ReverseIterator(start, end []byte) Iterator {
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

	return newPrefixIterator(b.prefix, start, end, iter)
}

// prefixIterator wraps an Iterator and strips the prefix from keys
type prefixIterator struct {
	prefix []byte
	start  []byte
	end    []byte
	iter   Iterator
	valid  bool
}

// newPrefixIterator creates a new prefixIterator
func newPrefixIterator(prefix, start, end []byte, parent Iterator) *prefixIterator {
	valid := parent.Valid() && bytes.HasPrefix(parent.Key(), prefix)
	return &prefixIterator{
		prefix: prefix,
		start:  start,
		end:    end,
		iter:   parent,
		valid:  valid,
	}
}

// Domain returns the start and end keys
func (pi *prefixIterator) Domain() ([]byte, []byte) {
	return pi.start, pi.end
}

// Valid returns whether the iterator is positioned at a valid item
func (pi *prefixIterator) Valid() bool {
	return pi.valid && pi.iter.Valid()
}

// Next moves to the next item
func (pi *prefixIterator) Next() {
	if !pi.valid {
		panic("prefixIterator invalid, cannot call Next()")
	}

	pi.iter.Next()
	if !pi.iter.Valid() || !bytes.HasPrefix(pi.iter.Key(), pi.prefix) {
		pi.valid = false
	}
}

// Key returns the current key with the prefix stripped
func (pi *prefixIterator) Key() []byte {
	if !pi.valid {
		panic("prefixIterator invalid, cannot call Key()")
	}

	key := pi.iter.Key()
	return stripPrefix(key, pi.prefix)
}

// Value returns the current value
func (pi *prefixIterator) Value() any {
	if !pi.valid {
		panic("prefixIterator invalid, cannot call Value()")
	}

	return pi.iter.Value()
}

// Close releases resources
func (pi *prefixIterator) Close() error {
	return pi.iter.Close()
}

// Error returns an error if the iterator is invalid
func (pi *prefixIterator) Error() error {
	if !pi.Valid() {
		return errors.New("invalid prefixIterator")
	}

	return nil
}

// stripPrefix removes the prefix from a key
func stripPrefix(key, prefix []byte) []byte {
	if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
		panic("should not happen")
	}

	return key[len(prefix):]
}

// cpIncr returns the end byte for a prefix
func cpIncr(bz []byte) []byte {
	end := make([]byte, len(bz))
	copy(end, bz)

	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}

	// This should never happen since we only have a finite key space
	// and the prefix is not infinitely long
	end = make([]byte, len(bz)+1)
	copy(end, bz)
	end[len(bz)] = 1
	return end
}
