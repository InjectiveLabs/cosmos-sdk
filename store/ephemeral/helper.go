package ephemeral

type (
	TypedBatch[T any] struct {
		batch EphemeralBatch
	}

	TypedIterator[T any] struct {
		Iterator
	}
)

func NewTypedBatch[T any](batch EphemeralBatch) TypedBatch[T] {
	return TypedBatch[T]{
		batch: batch,
	}
}

func (b *TypedBatch[T]) Get(key []byte) T {
	value := b.batch.Get(key)
	if value == nil {
		var zero T
		return zero
	}

	typedValue := value.(T)
	return typedValue
}

func (b *TypedBatch[T]) Set(key []byte, value T) {
	b.batch.Set(key, value)
}

func (b *TypedBatch[T]) Delete(key []byte) {
	b.batch.Delete(key)
}

func (b *TypedBatch[T]) Iterator(start, end []byte) TypedIterator[T] {
	return TypedIterator[T]{
		Iterator: b.batch.Iterator(start, end),
	}
}

func (b *TypedBatch[T]) ReverseIterator(start, end []byte) TypedIterator[T] {
	return TypedIterator[T]{
		Iterator: b.batch.ReverseIterator(start, end),
	}
}

func (i *TypedIterator[T]) Value() T {
	if !i.Valid() {
		var zero T
		return zero
	}

	value := i.Iterator.Value()
	return value.(T)
}

func (b *TypedBatch[T]) Commit() {
	b.batch.Commit()
}
