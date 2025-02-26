package ephemeral

type TypedEphemeralStore[T any] interface {
	Get(key []byte) *T
	Set(key []byte, value *T)
	Iterator(start, end []byte) TypedEphemeralIterator[T]
	ReverseIterator(start, end []byte) TypedEphemeralIterator[T]
}

// Get implements TypedEphemeralStore.
func (t *Placeholder[T]) Get(key []byte) *T {
	panic("unimplemented")
}

// Iterator implements TypedEphemeralStore.
func (t *Placeholder[T]) Iterator(start []byte, end []byte) TypedEphemeralIterator[T] {
	panic("unimplemented")
}

// ReverseIterator implements TypedEphemeralStore.
func (t *Placeholder[T]) ReverseIterator(start []byte, end []byte) TypedEphemeralIterator[T] {
	panic("unimplemented")
}

// Set implements TypedEphemeralStore.
func (t *Placeholder[T]) Set(key []byte, value *T) {
	panic("unimplemented")
}

type TypedEphemeralIterator[T any] interface {
	Next()
	Key() []byte
	Value() *T
}

var _ TypedEphemeralStore[any] = (*Placeholder[any])(nil)

type Placeholder[T any] struct {
	EphemeralStore
}

func NewTypedEpeheralStore[T any](
	prefix []byte,
	ephemeralStore EphemeralStore,
) TypedEphemeralStore[T] {
	return &Placeholder[T]{
		ephemeralStore,
	}
}
