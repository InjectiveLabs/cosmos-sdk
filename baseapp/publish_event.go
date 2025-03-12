package baseapp

import (
	"encoding/json"

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

func (p *PublishEventFlush) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

func (p *PublishEventFlush) GetHeight() int64 {
	return p.Height
}
func (p *PublishEventFlush) GetAppHash() []byte {
	return p.NewAppHash
}
func (p *PublishEventFlush) GetLastAppHash() []byte {
	return p.PrevAppHash
}
