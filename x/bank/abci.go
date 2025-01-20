package bank

import (
	"context"

	"cosmossdk.io/x/bank/keeper"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// EndBlocker is called every block, emits balance event
func EndBlocker(ctx context.Context, k keeper.Keeper) {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	k.EmitAllTransientBalances(sdkCtx)
}
