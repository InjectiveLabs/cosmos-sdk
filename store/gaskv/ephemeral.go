package gaskv

import (
	"cosmossdk.io/store/ephemeral"
	"cosmossdk.io/store/types"
)

var _ ephemeral.EphemeralKVStore = &EphemeralStore{}

type EphemeralStore struct {
	gasMeter  types.GasMeter
	gasConfig types.GasConfig
	parent    ephemeral.EphemeralKVStore
}

func NewEphemeralStore(parent ephemeral.EphemeralKVStore, gasMeter types.GasMeter, gasConfig types.GasConfig) *EphemeralStore {
	return &EphemeralStore{
		gasMeter:  gasMeter,
		gasConfig: gasConfig,
		parent:    parent,
	}
}

func (e *EphemeralStore) Branch() ephemeral.EphemeralCacheKVStore {
	panic("cannot branch GasEphemeralStore")
}

func (e *EphemeralStore) Get(key []byte) any {
	e.gasMeter.ConsumeGas(e.gasConfig.ReadCostFlat, types.GasReadCostFlatDesc)
	value := e.parent.Get(key)

	// TODO overflow-safe math?
	e.gasMeter.ConsumeGas(e.gasConfig.ReadCostPerByte*types.Gas(len(key)), types.GasReadPerByteDesc)
	// FIXME: how to get the length of value? (proto.Size()?)
	// e.gasMeter.ConsumeGas(e.gasConfig.ReadCostPerByte*types.Gas(len(value)), types.GasReadPerByteDesc)

	return value
}

func (e *EphemeralStore) Set(key []byte, value any) {
	e.gasMeter.ConsumeGas(e.gasConfig.WriteCostFlat, types.GasWriteCostFlatDesc)
	// TODO overflow-safe math?
	e.gasMeter.ConsumeGas(e.gasConfig.WriteCostPerByte*types.Gas(len(key)), types.GasWritePerByteDesc)
	// FIXME: how to get the length of value? (proto.Size()?)
	// e.gasMeter.ConsumeGas(e.gasConfig.WriteCostPerByte*types.Gas(len(value)), types.GasWritePerByteDesc)
	e.parent.Set(key, value)
}

func (e *EphemeralStore) Delete(key []byte) {
	// charge gas to prevent certain attack vectors even though space is being freed
	e.gasMeter.ConsumeGas(e.gasConfig.DeleteCost, types.GasDeleteDesc)
	e.parent.Delete(key)
}

func (e *EphemeralStore) Iterator(start []byte, end []byte) ephemeral.EphemeralIterator {
	iterator := e.parent.Iterator(start, end)
	return newEphemeralGasIterator(e.gasMeter, e.gasConfig, iterator)
}

func (e *EphemeralStore) ReverseIterator(start []byte, end []byte) ephemeral.EphemeralIterator {
	iterator := e.parent.ReverseIterator(start, end)
	return newEphemeralGasIterator(e.gasMeter, e.gasConfig, iterator)
}

type ephemeralGasIterator struct {
	gasMeter  types.GasMeter
	gasConfig types.GasConfig
	iterator  ephemeral.EphemeralIterator
}

func newEphemeralGasIterator(gasMeter types.GasMeter, gasConfig types.GasConfig, iterator ephemeral.EphemeralIterator) *ephemeralGasIterator {
	iter := &ephemeralGasIterator{
		gasMeter:  gasMeter,
		gasConfig: gasConfig,
		iterator:  iterator,
	}
	iter.consumeSeekGas()

	return iter
}

func (e *ephemeralGasIterator) Domain() (start []byte, end []byte) {
	return e.iterator.Domain()
}

func (e *ephemeralGasIterator) Key() []byte {
	return e.iterator.Key()
}

func (e *ephemeralGasIterator) Value() any {
	return e.iterator.Value()
}

func (e *ephemeralGasIterator) Next() {
	e.consumeSeekGas()
	e.iterator.Next()
}

func (e *ephemeralGasIterator) Valid() bool {
	return e.iterator.Valid()
}

func (e *ephemeralGasIterator) Close() error {
	return e.iterator.Close()
}

func (e *ephemeralGasIterator) consumeSeekGas() {
	if e.Valid() {
		key := e.Key()
		// FIXME: how to get the length of value? (proto.Size()?)
		// value := e.Value()

		e.gasMeter.ConsumeGas(e.gasConfig.ReadCostPerByte*types.Gas(len(key)), types.GasValuePerByteDesc)
		// gi.gasMeter.ConsumeGas(gi.gasConfig.ReadCostPerByte*types.Gas(len(value)), types.GasValuePerByteDesc)
	}
	e.gasMeter.ConsumeGas(e.gasConfig.IterNextCostFlat, types.GasIterNextCostFlatDesc)
}
