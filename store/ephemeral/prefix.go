package ephemeral

import (
	"bytes"
)

var _ EphemeralKVStore = (*prefixEphemeralKVStore)(nil)

type prefixEphemeralKVStore struct {
	parent EphemeralKVStore
	prefix []byte
}

func NewPrefixEphemeralKVStore(ephemeralStore EphemeralKVStore, prefix []byte) EphemeralKVStore {
	return &prefixEphemeralKVStore{
		parent: ephemeralStore,
		prefix: prefix,
	}
}

func cloneAppend(bz, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}

func (p *prefixEphemeralKVStore) key(key []byte) (res []byte) {
	if key == nil {
		panic("nil key on Store")
	}
	res = cloneAppend(p.prefix, key)
	return
}

func (p *prefixEphemeralKVStore) Branch() EphemeralCacheKVStore {
	return NewEphemeralCacheKV(p)
}

func (p *prefixEphemeralKVStore) Get(key []byte) any {
	return p.parent.Get(p.key(key))
}

func (p *prefixEphemeralKVStore) Set(key []byte, value any) {
	p.parent.Set(p.key(key), value)
}

func (p *prefixEphemeralKVStore) Delete(key []byte) {
	p.parent.Delete(p.key(key))
}

func (p *prefixEphemeralKVStore) Iterator(start []byte, end []byte) EphemeralIterator {
	newStart := cloneAppend(p.prefix, start)

	var newEnd []byte
	if end == nil {
		newEnd = cpIncr(p.prefix)
	} else {
		newEnd = cloneAppend(p.prefix, end)
	}

	iter := p.parent.Iterator(newStart, newEnd)
	return newPrefixIterator(p.prefix, start, end, iter)
}

func (p *prefixEphemeralKVStore) ReverseIterator(start []byte, end []byte) EphemeralIterator {
	newStart := cloneAppend(p.prefix, start)

	var newEnd []byte
	if end == nil {
		newEnd = cpIncr(p.prefix)
	} else {
		newEnd = cloneAppend(p.prefix, end)
	}

	iter := p.parent.ReverseIterator(newStart, newEnd)
	return newPrefixIterator(p.prefix, start, end, iter)
}

func cpIncr(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := make([]byte, len(prefix))
	copy(end, prefix)

	for {
		if end[len(end)-1] != byte(255) {
			end[len(end)-1]++
			break
		}

		end = end[:len(end)-1]

		if len(end) == 0 {
			end = nil
			break
		}
	}

	return end
}

type prefixedEphemeralIterator struct {
	prefix []byte
	start  []byte
	end    []byte
	iter   EphemeralIterator
	valid  bool
}

func newPrefixIterator(prefix, start, end []byte, parent EphemeralIterator) *prefixedEphemeralIterator {
	return &prefixedEphemeralIterator{
		prefix: prefix,
		start:  start,
		end:    end,
		iter:   parent,
		valid:  parent.Valid() && bytes.HasPrefix(parent.Key(), prefix),
	}
}

func (p *prefixedEphemeralIterator) Close() error {
	return p.iter.Close()
}

func (p *prefixedEphemeralIterator) Domain() (start []byte, end []byte) {
	return p.start, p.end
}

func (p *prefixedEphemeralIterator) Key() []byte {
	if !p.valid {
		panic("prefixIterator invalid, cannot call Key()")
	}

	key := p.iter.Key()
	key = stripPrefix(key, p.prefix)

	return key
}

func (p *prefixedEphemeralIterator) Next() {
	if !p.valid {
		panic("prefixIterator invalid, cannot call Next()")
	}

	if p.iter.Next(); !p.iter.Valid() || !bytes.HasPrefix(p.iter.Key(), p.prefix) {
		// TODO: shouldn't p be set to nil instead?
		p.valid = false
	}
}

func (p *prefixedEphemeralIterator) Valid() bool {
	return p.valid && p.iter.Valid()
}

func (p *prefixedEphemeralIterator) Value() any {
	if !p.valid {
		panic("prefixIterator invalid, cannot call Value()")
	}

	return p.iter.Value()
}

// copied from github.com/cometbft/cometbft/libs/db/prefix_db.go
func stripPrefix(key, prefix []byte) []byte {
	if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
		panic("should not happen")
	}

	return key[len(prefix):]
}
