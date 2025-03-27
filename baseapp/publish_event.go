package baseapp

import (
	abci "github.com/cometbft/cometbft/abci/types"
	types "github.com/cosmos/cosmos-sdk/types"
)

type EventType byte

const (
	EventTypeAbci EventType = iota
	EventTypePublish
)

type EventSet struct {
	AbciEvents    []abci.Event
	PublishEvents types.PublishEvents
	TrueOrder     []EventType
}

type PublishEventFlush struct {
	Height      int64
	PrevAppHash []byte
	NewAppHash  []byte
	BlockEvents EventSet
	TxEvents    []EventSet
}

func (app *BaseApp) PublishBlockEvents(flush PublishEventFlush) {
	if app.EnablePublish {
		app.PublishEvents <- flush
	}
}
