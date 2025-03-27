package memstore

import (
	"cosmossdk.io/store/types"
	"fmt"
)

var _ types.MemStore = (*unusableMemstore)(nil)

// unusableMemstore is a memstore that panics on all operations.
// A panic occurs when attempting to use a snapshot memstore of a non-existent version.
type unusableMemstore struct {
	height int64
}

func NewUnusableMemstore(height int64) types.MemStore {
	return &unusableMemstore{
		height: height,
	}
}

func (u *unusableMemstore) Branch() types.MemStore {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) Get(key []byte) any {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) Iterator(start, end []byte) types.MemStoreIterator {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) ReverseIterator(start, end []byte) types.MemStoreIterator {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) Set(key []byte, value any) {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) Delete(key []byte) {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}

func (u *unusableMemstore) Commit() {
	//TODO implement me
	panic(fmt.Sprintf("no %d height memstore snapshot", u.height))
}
