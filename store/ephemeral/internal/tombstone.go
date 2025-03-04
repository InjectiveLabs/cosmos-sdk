package internal

type tombstone struct{}

func IsTombstone(v any) bool {
	if v == nil {
		return false
	}
	_, ok := v.(*tombstone)
	return ok
}

func NewTombstone() Sized {
	return &tombstone{}
}

func (t *tombstone) Size() int {
	return 0
}
