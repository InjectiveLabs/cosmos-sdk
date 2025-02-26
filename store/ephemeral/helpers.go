package ephemeral

import "cosmossdk.io/store/ephemeral/internal"

type TypedEphemeralStore[T any] interface {
	Get(key []byte) *T

	Set(key []byte, value *T)
	Delete(key []byte)

	Iterator(start, end []byte) TypedEphemeralIterator[T]
	ReverseIterator(start, end []byte) TypedEphemeralIterator[T]
}

var _ TypedEphemeralStore[any] = (*typedEphemeralKVStore[any])(nil)

type typedEphemeralKVStore[T any] struct {
	store EphemeralKVStore
}

func NewTypedEpeheralKVStore[T any](
	prefix []byte,
	ephemeralStore EphemeralKVStore,
) TypedEphemeralStore[T] {
	return &typedEphemeralKVStore[T]{
		store: ephemeralStore,
	}
}

func (t *typedEphemeralKVStore[T]) Get(key []byte) *T {
	v := t.store.Get(key)
	if v != nil {
		return v.(*T)
	}
	return nil
}

func (t *typedEphemeralKVStore[T]) Set(key []byte, value *T) {
	t.store.Set(key, value)
}

func (t *typedEphemeralKVStore[_]) Delete(key []byte) {
	t.store.Delete(key)
}

func (t *typedEphemeralKVStore[T]) Iterator(start []byte, end []byte) TypedEphemeralIterator[T] {
	return &typedEphemeralIterator[T]{
		EphemeralIterator: t.store.Iterator(start, end),
	}
}

func (t *typedEphemeralKVStore[T]) ReverseIterator(start []byte, end []byte) TypedEphemeralIterator[T] {
	return &typedEphemeralIterator[T]{
		EphemeralIterator: t.store.ReverseIterator(start, end),
	}
}

type TypedEphemeralIterator[T any] internal.TypedEphemeralIterator[T]

type typedEphemeralIterator[T any] struct {
	EphemeralIterator
}

func (t *typedEphemeralIterator[T]) Value() T {
	if !t.Valid() {
		var zero T
		return zero
	}

	return t.EphemeralIterator.Value().(T)
}
