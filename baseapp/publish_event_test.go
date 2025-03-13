package baseapp_test

import (
	"fmt"
	"testing"

	"cosmossdk.io/log"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
	baseapptestutil "github.com/cosmos/cosmos-sdk/baseapp/testutil"
	sdk "github.com/cosmos/cosmos-sdk/types"
	types "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"
)

var _ types.PublishEvent = (*StringPublishEvent)(nil)

type StringPublishEvent struct {
	data string
}

func (e StringPublishEvent) ToString() string {
	return e.data
}

func (e StringPublishEvent) Serialize() []byte {
	return []byte(e.data)
}

func getStreamEventFlushChan(app *baseapp.BaseApp) chan baseapp.PublishEventFlush {
	app.EnablePublish = true
	publishEventChan := make(chan baseapp.PublishEventFlush)

	go func() {
		for {
			select {
			case e := <-app.PublishEvents:
				publishEventChan <- e
			}
		}
	}()

	return publishEventChan
}

func TestPublishEvent_FinalizeBlock_WithBeginAndEndBlocker(t *testing.T) {
	name := t.Name()
	db := dbm.NewMemDB()
	app := baseapp.NewBaseApp(name, log.NewTestLogger(t), db, nil)

	publishEventChan := getStreamEventFlushChan(app)

	app.SetBeginBlocker(func(ctx sdk.Context) (sdk.BeginBlock, error) {
		ctx = ctx.WithEventManager(sdk.NewEventManager())
		ctx.EventManager().EmitEvent(types.Event{
			Type: "sometype",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "bar",
				},
			},
		})
		ctx.PublishEventManager().EmitEvent(StringPublishEvent{"sometype2"})

		return sdk.BeginBlock{
			Events: ctx.EventManager().ABCIEvents(),
		}, nil
	})

	app.SetEndBlocker(func(ctx sdk.Context) (sdk.EndBlock, error) {
		ctx = ctx.WithEventManager(sdk.NewEventManager())
		ctx.EventManager().EmitEvent(types.Event{
			Type: "anothertype",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "bar",
				},
			},
		})

		ctx.PublishEventManager().EmitEvent(StringPublishEvent{"anothertype2"})

		return sdk.EndBlock{
			Events: ctx.EventManager().ABCIEvents(),
		}, nil
	})

	_, err := app.InitChain(
		&abci.RequestInitChain{
			InitialHeight: 1,
		},
	)
	require.NoError(t, err)

	res, err := app.FinalizeBlock(&abci.RequestFinalizeBlock{Height: 1})
	require.NoError(t, err)

	require.Len(t, res.Events, 2)

	require.Equal(t, "sometype", res.Events[0].Type)
	require.Equal(t, "foo", res.Events[0].Attributes[0].Key)
	require.Equal(t, "bar", res.Events[0].Attributes[0].Value)
	require.Equal(t, "mode", res.Events[0].Attributes[1].Key)
	require.Equal(t, "BeginBlock", res.Events[0].Attributes[1].Value)

	require.Equal(t, "anothertype", res.Events[1].Type)
	require.Equal(t, "foo", res.Events[1].Attributes[0].Key)
	require.Equal(t, "bar", res.Events[1].Attributes[0].Value)
	require.Equal(t, "mode", res.Events[1].Attributes[1].Key)
	require.Equal(t, "EndBlock", res.Events[1].Attributes[1].Value)

	_, err = app.Commit()
	require.NoError(t, err)

	require.Equal(t, int64(1), app.LastBlockHeight())

	pevts := <-publishEventChan
	require.Len(t, pevts.BlockEvents.PublishEvents, 2)
	require.Equal(t, StringPublishEvent{"sometype2"}, pevts.BlockEvents.PublishEvents[0])
	require.Equal(t, StringPublishEvent{"anothertype2"}, pevts.BlockEvents.PublishEvents[1])

	require.Len(t, pevts.BlockEvents.AbciEvents, 2)

	require.Len(t, pevts.BlockEvents.TrueOrder, 4)
	require.Equal(t, pevts.BlockEvents.TrueOrder, []baseapp.EventType{
		baseapp.EventTypeAbci, baseapp.EventTypePublish, baseapp.EventTypeAbci, baseapp.EventTypePublish,
	})

}

func TestPublishEvent_FinalizeBlock_DeliverTx(t *testing.T) {
	anteKey := []byte("ante-key")
	anteOpt := func(bapp *baseapp.BaseApp) {
		bapp.SetAnteHandler(anteHandlerTxTestWithCustomEventEmit(t, capKey1, anteKey, true))
	}
	suite := NewBaseAppSuite(t, anteOpt)
	publishEventChan := getStreamEventFlushChan(suite.baseApp)

	_, err := suite.baseApp.InitChain(&abci.RequestInitChain{
		ConsensusParams: &cmtproto.ConsensusParams{},
	})
	require.NoError(t, err)

	deliverKey := []byte("deliver-key")
	baseapptestutil.RegisterCounterServer(suite.baseApp.MsgServiceRouter(), CounterServerImpl{t, capKey1, deliverKey, true})

	nBlocks := 3
	txPerHeight := 5

	var lastAppHash []byte

	for blockN := 0; blockN < nBlocks; blockN++ {

		txs := [][]byte{}
		for i := 0; i < txPerHeight; i++ {
			counter := int64(blockN*txPerHeight + i)
			tx := newTxCounter(t, suite.txConfig, counter, counter)

			txBytes, err := suite.txConfig.TxEncoder()(tx)
			require.NoError(t, err)

			txs = append(txs, txBytes)
		}

		res, err := suite.baseApp.FinalizeBlock(&abci.RequestFinalizeBlock{
			Height: int64(blockN) + 1,
			Txs:    txs,
		})
		require.NoError(t, err)

		for i := 0; i < txPerHeight; i++ {
			counter := int64(blockN*txPerHeight + i)
			require.True(t, res.TxResults[i].IsOK(), fmt.Sprintf("%v", res))

			events := res.TxResults[i].GetEvents()
			require.Len(t, events, 3, "should contain ante handler, message type and counter events respectively")
			require.Equal(t, sdk.MarkEventsToIndex(counterEvent("ante_handler", counter).ToABCIEvents(), map[string]struct{}{})[0], events[0], "ante handler event")
			require.Equal(t, sdk.MarkEventsToIndex(counterEvent(sdk.EventTypeMessage, counter).ToABCIEvents(), map[string]struct{}{})[0].Attributes[0], events[2].Attributes[0], "msg handler update counter event")
		}

		_, err = suite.baseApp.Commit()
		require.NoError(t, err)

		pevts := <-publishEventChan
		require.Equal(t, pevts.Height, int64(blockN+1))
		if blockN > 0 {
			require.Equal(t, pevts.PrevAppHash, lastAppHash, "should be the same as last app hash")
		}
		require.Equal(t, pevts.NewAppHash, res.AppHash)
		require.Len(t, pevts.TxEvents, txPerHeight)
		fmt.Println(pevts.TxEvents)
		for i := 0; i < txPerHeight; i++ {
			require.Len(t, pevts.TxEvents[i].PublishEvents, 2)
			require.Len(t, pevts.TxEvents[i].TrueOrder, 5)
		}

		lastAppHash = res.AppHash
	}
}
