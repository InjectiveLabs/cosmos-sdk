package baseapp

import (
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	types "github.com/cosmos/cosmos-sdk/types"
)

type StreamEventsFlush struct {
	PublishEvents []types.PublishEvent
	PrevAppHash   []byte
	NewAppHash    []byte
}

type StreamEvents struct {
	Events    []abci.Event
	Height    uint64
	BlockTime time.Time
	Flush     *StreamEventsFlush
}

func (app *BaseApp) AddStreamEvents(height int64, blockTime time.Time, events []abci.Event) {
	if app.EnableStreamer {
		app.StreamEvents <- StreamEvents{
			Events:    events,
			Height:    uint64(height),
			BlockTime: blockTime,
		}
	}
}

func (app *BaseApp) FlushStreamEvents(height int64, blockTime time.Time, flush StreamEventsFlush) {
	if app.EnableStreamer {
		app.StreamEvents <- StreamEvents{
			Height:    uint64(height),
			BlockTime: blockTime,
			Flush:     &flush,
		}
	}
}
