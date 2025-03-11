package baseapp

import (
	abci "github.com/cometbft/cometbft/abci/types"
	types "github.com/cosmos/cosmos-sdk/types"
)

type PublishEventFlush struct {
	Height        int64
	PrevAppHash   []byte
	NewAppHash    []byte
	AbciEvents    []abci.Event
	PublishEvents types.PublishEvents
	TrueOrder     []string
}

func (app *BaseApp) PublishBlockEvents(flush PublishEventFlush) {
	if app.EnablePublish {
		app.PublishEvents <- flush
	}
}
