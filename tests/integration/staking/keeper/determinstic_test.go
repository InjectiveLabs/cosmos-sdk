package keeper_test

import (
	"testing"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"gotest.tools/v3/assert"
	"pgregory.net/rapid"

	"cosmossdk.io/core/appmodule"
	"cosmossdk.io/log"
	"cosmossdk.io/math"
	storetypes "cosmossdk.io/store/types"

	"github.com/cosmos/cosmos-sdk/codec"
	addresscodec "github.com/cosmos/cosmos-sdk/codec/address"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	"github.com/cosmos/cosmos-sdk/runtime"
	"github.com/cosmos/cosmos-sdk/testutil/integration"
	"github.com/cosmos/cosmos-sdk/testutil/testdata"
	sdk "github.com/cosmos/cosmos-sdk/types"
	moduletestutil "github.com/cosmos/cosmos-sdk/types/module/testutil"
	"github.com/cosmos/cosmos-sdk/x/auth"
	authkeeper "github.com/cosmos/cosmos-sdk/x/auth/keeper"
	authsims "github.com/cosmos/cosmos-sdk/x/auth/simulation"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/cosmos/cosmos-sdk/x/bank"
	bankkeeper "github.com/cosmos/cosmos-sdk/x/bank/keeper"
	banktestutil "github.com/cosmos/cosmos-sdk/x/bank/testutil"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/cosmos/cosmos-sdk/x/distribution"
	minttypes "github.com/cosmos/cosmos-sdk/x/mint/types"
	"github.com/cosmos/cosmos-sdk/x/staking"
	stakingkeeper "github.com/cosmos/cosmos-sdk/x/staking/keeper"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

var (
	validator1        = "cosmosvaloper1qqqryrs09ggeuqszqygqyqd2tgqmsqzewacjj7"
	validatorAddr1, _ = sdk.ValAddressFromBech32(validator1)
	validator2        = "cosmosvaloper1gghjut3ccd8ay0zduzj64hwre2fxs9ldmqhffj"
	validatorAddr2, _ = sdk.ValAddressFromBech32(validator2)
	delegator1        = "cosmos1nph3cfzk6trsmfxkeu943nvach5qw4vwstnvkl"
	delegatorAddr1    = sdk.MustAccAddressFromBech32(delegator1)
	delegator2        = "cosmos139f7kncmglres2nf3h4hc4tade85ekfr8sulz5"
	delegatorAddr2    = sdk.MustAccAddressFromBech32(delegator2)
)

type deterministicFixture struct {
	app *integration.App

	ctx  sdk.Context
	cdc  codec.Codec
	keys map[string]*storetypes.KVStoreKey

	accountKeeper authkeeper.AccountKeeper
	bankKeeper    bankkeeper.BaseKeeper
	stakingKeeper *stakingkeeper.Keeper

	queryClient stakingtypes.QueryClient
	amt1        math.Int
	amt2        math.Int
}

func initDeterministicFixture(t *testing.T) *deterministicFixture {
	keys := storetypes.NewKVStoreKeys(
		authtypes.StoreKey, banktypes.StoreKey, stakingtypes.StoreKey,
	)
	tkey := storetypes.NewTransientStoreKey(banktypes.TStoreKey)
	cdc := moduletestutil.MakeTestEncodingConfig(auth.AppModuleBasic{}, distribution.AppModuleBasic{}).Codec

	logger := log.NewTestLogger(t)
	cms := integration.CreateMultiStore(keys, logger)
	cms.MountStoreWithDB(tkey, storetypes.StoreTypeTransient, nil)
	if err := cms.LoadLatestVersion(); err != nil {
		t.Fatalf("failed to load latest version: %v", err)
	}

	newCtx := sdk.NewContext(cms, cmtproto.Header{}, true, logger)

	authority := authtypes.NewModuleAddress("gov")

	maccPerms := map[string][]string{
		minttypes.ModuleName:           {authtypes.Minter},
		stakingtypes.ModuleName:        {authtypes.Minter},
		stakingtypes.BondedPoolName:    {authtypes.Burner, authtypes.Staking},
		stakingtypes.NotBondedPoolName: {authtypes.Burner, authtypes.Staking},
	}

	accountKeeper := authkeeper.NewAccountKeeper(
		cdc,
		runtime.NewKVStoreService(keys[authtypes.StoreKey]),
		authtypes.ProtoBaseAccount,
		maccPerms,
		addresscodec.NewBech32Codec(sdk.Bech32MainPrefix),
		sdk.Bech32MainPrefix,
		authority.String(),
	)

	blockedAddresses := map[string]bool{
		accountKeeper.GetAuthority(): false,
	}
	bankKeeper := bankkeeper.NewBaseKeeper(
		cdc,
		runtime.NewKVStoreService(keys[banktypes.StoreKey]),
		runtime.NewTransientKVStoreService(tkey),
		accountKeeper,
		blockedAddresses,
		authority.String(),
		log.NewNopLogger(),
	)

	stakingKeeper := stakingkeeper.NewKeeper(cdc, runtime.NewKVStoreService(keys[stakingtypes.StoreKey]), accountKeeper, bankKeeper, authority.String(), addresscodec.NewBech32Codec(sdk.Bech32PrefixValAddr), addresscodec.NewBech32Codec(sdk.Bech32PrefixConsAddr))

	authModule := auth.NewAppModule(cdc, accountKeeper, authsims.RandomGenesisAccounts, nil)
	bankModule := bank.NewAppModule(cdc, bankKeeper, accountKeeper, nil)
	stakingModule := staking.NewAppModule(cdc, stakingKeeper, accountKeeper, bankKeeper, nil)

	integrationApp := integration.NewIntegrationApp(newCtx, logger, keys, cdc, map[string]appmodule.AppModule{
		authtypes.ModuleName:    authModule,
		banktypes.ModuleName:    bankModule,
		stakingtypes.ModuleName: stakingModule,
	})

	ctx := integrationApp.Context()

	// Register MsgServer and QueryServer
	stakingtypes.RegisterMsgServer(integrationApp.MsgServiceRouter(), stakingkeeper.NewMsgServerImpl(stakingKeeper))
	stakingtypes.RegisterQueryServer(integrationApp.QueryHelper(), stakingkeeper.NewQuerier(stakingKeeper))

	// set default staking params
	assert.NilError(t, stakingKeeper.SetParams(ctx, stakingtypes.DefaultParams()))

	// set pools
	startTokens := stakingKeeper.TokensFromConsensusPower(ctx, 10)
	bondDenom, err := stakingKeeper.BondDenom(ctx)
	assert.NilError(t, err)
	notBondedPool := stakingKeeper.GetNotBondedPool(ctx)
	assert.NilError(t, banktestutil.FundModuleAccount(ctx, bankKeeper, notBondedPool.GetName(), sdk.NewCoins(sdk.NewCoin(bondDenom, startTokens))))
	accountKeeper.SetModuleAccount(ctx, notBondedPool)
	bondedPool := stakingKeeper.GetBondedPool(ctx)
	assert.NilError(t, banktestutil.FundModuleAccount(ctx, bankKeeper, bondedPool.GetName(), sdk.NewCoins(sdk.NewCoin(bondDenom, startTokens))))
	accountKeeper.SetModuleAccount(ctx, bondedPool)

	qr := integrationApp.QueryHelper()
	queryClient := stakingtypes.NewQueryClient(qr)

	amt1 := stakingKeeper.TokensFromConsensusPower(ctx, 101)
	amt2 := stakingKeeper.TokensFromConsensusPower(ctx, 102)

	f := deterministicFixture{
		app:           integrationApp,
		ctx:           sdk.UnwrapSDKContext(ctx),
		cdc:           cdc,
		keys:          keys,
		accountKeeper: accountKeeper,
		bankKeeper:    bankKeeper,
		stakingKeeper: stakingKeeper,
		queryClient:   queryClient,
		amt1:          amt1,
		amt2:          amt2,
	}

	return &f
}

func durationGenerator() *rapid.Generator[time.Duration] {
	return rapid.Custom(func(t *rapid.T) time.Duration {
		now := time.Now()
		// range from current time to 365days.
		duration := rapid.Int64Range(now.Unix(), 365*24*60*60*now.Unix()).Draw(t, "time")
		return time.Duration(duration)
	})
}

func pubKeyGenerator() *rapid.Generator[ed25519.PubKey] {
	return rapid.Custom(func(t *rapid.T) ed25519.PubKey {
		pkBz := rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "hex")
		return ed25519.PubKey{Key: pkBz}
	})
}

func bondTypeGenerator() *rapid.Generator[stakingtypes.BondStatus] {
	bondTypes := []stakingtypes.BondStatus{stakingtypes.Bonded, stakingtypes.Unbonded, stakingtypes.Unbonding}
	return rapid.Custom(func(t *rapid.T) stakingtypes.BondStatus {
		return bondTypes[rapid.IntRange(0, 2).Draw(t, "range")]
	})
}

// createValidator creates a validator with random values.
func createValidator(rt *rapid.T, f *deterministicFixture, t *testing.T) stakingtypes.Validator {
	pubkey := pubKeyGenerator().Draw(rt, "pubkey")
	pubkeyAny, err := codectypes.NewAnyWithValue(&pubkey)
	assert.NilError(t, err)

	// Use the safe address generator to ensure the account is created
	valAddress := sdk.ValAddress(safeAddressGenerator(rt, f, t))

	return stakingtypes.Validator{
		OperatorAddress: valAddress.String(),
		ConsensusPubkey: pubkeyAny,
		Jailed:          rapid.Bool().Draw(rt, "jailed"),
		Status:          bondTypeGenerator().Draw(rt, "bond-status"),
		Tokens:          math.NewInt(rapid.Int64Min(10000).Draw(rt, "tokens")),
		DelegatorShares: math.LegacyNewDecWithPrec(rapid.Int64Range(1, 100).Draw(rt, "commission"), 2),
		Description: stakingtypes.NewDescription(
			rapid.StringN(5, 250, 255).Draw(rt, "moniker"),
			rapid.StringN(5, 250, 255).Draw(rt, "identity"),
			rapid.StringN(5, 250, 255).Draw(rt, "website"),
			rapid.StringN(5, 250, 255).Draw(rt, "securityContact"),
			rapid.StringN(5, 250, 255).Draw(rt, "details"),
		),
		UnbondingHeight: rapid.Int64Min(1).Draw(rt, "unbonding-height"),
		UnbondingTime:   time.Now().Add(durationGenerator().Draw(rt, "duration")),
		Commission: stakingtypes.NewCommission(
			math.LegacyNewDecWithPrec(rapid.Int64Range(0, 100).Draw(rt, "rate"), 2),
			math.LegacyNewDecWithPrec(rapid.Int64Range(0, 100).Draw(rt, "max-rate"), 2),
			math.LegacyNewDecWithPrec(rapid.Int64Range(0, 100).Draw(rt, "max-change-rate"), 2),
		),
		MinSelfDelegation: math.NewInt(rapid.Int64Min(1).Draw(rt, "tokens")),
	}
}

// createAndSetValidatorWithStatus creates a validator with random values but with given status and sets to the state
func createAndSetValidatorWithStatus(rt *rapid.T, f *deterministicFixture, t *testing.T, status stakingtypes.BondStatus) stakingtypes.Validator {
	val := createValidator(rt, f, t)
	val.Status = status
	setValidator(f, t, val)
	return val
}

// createAndSetValidator creates a validator with random values and sets to the state
func createAndSetValidator(rt *rapid.T, f *deterministicFixture, t *testing.T) stakingtypes.Validator {
	val := createValidator(rt, f, t)
	setValidator(f, t, val)
	return val
}

func setValidator(f *deterministicFixture, t *testing.T, validator stakingtypes.Validator) {
	assert.NilError(t, f.stakingKeeper.SetValidator(f.ctx, validator))
	assert.NilError(t, f.stakingKeeper.SetValidatorByPowerIndex(f.ctx, validator))
	assert.NilError(t, f.stakingKeeper.SetValidatorByConsAddr(f.ctx, validator))
	valbz, err := f.stakingKeeper.ValidatorAddressCodec().StringToBytes(validator.GetOperator())
	assert.NilError(t, err)

	assert.NilError(t, f.stakingKeeper.Hooks().AfterValidatorCreated(f.ctx, valbz))

	delegatorAddress := sdk.AccAddress(valbz)
	coins := sdk.NewCoins(sdk.NewCoin(sdk.DefaultBondDenom, validator.BondedTokens()))
	assert.NilError(t, banktestutil.FundAccount(f.ctx, f.bankKeeper, delegatorAddress, coins))

	_, err = f.stakingKeeper.Delegate(f.ctx, delegatorAddress, validator.BondedTokens(), stakingtypes.Unbonded, validator, true)
	assert.NilError(t, err)
}

// getStaticValidator creates a validator with hard-coded values and sets to the state.
func getStaticValidator(f *deterministicFixture, t *testing.T) stakingtypes.Validator {
	pubkey := ed25519.PubKey{Key: []byte{24, 179, 242, 2, 151, 3, 34, 6, 1, 11, 0, 194, 202, 201, 77, 1, 167, 40, 249, 115, 32, 97, 18, 1, 1, 127, 255, 103, 13, 1, 34, 1}}
	pubkeyAny, err := codectypes.NewAnyWithValue(&pubkey)
	assert.NilError(t, err)

	validator := stakingtypes.Validator{
		OperatorAddress: validator1,
		ConsensusPubkey: pubkeyAny,
		Jailed:          false,
		Status:          stakingtypes.Bonded,
		Tokens:          math.NewInt(100),
		DelegatorShares: math.LegacyNewDecWithPrec(5, 2),
		Description: stakingtypes.NewDescription(
			"moniker",
			"identity",
			"website",
			"securityContact",
			"details",
		),
		UnbondingHeight: 10,
		UnbondingTime:   time.Date(2022, 10, 1, 0, 0, 0, 0, time.UTC),
		Commission: stakingtypes.NewCommission(
			math.LegacyNewDecWithPrec(5, 2),
			math.LegacyNewDecWithPrec(5, 2),
			math.LegacyNewDecWithPrec(5, 2),
		),
		MinSelfDelegation: math.NewInt(10),
	}

	setValidator(f, t, validator)
	return validator
}

// getStaticValidator2 creates a validator with hard-coded values and sets to the state.
func getStaticValidator2(f *deterministicFixture, t *testing.T) stakingtypes.Validator {
	pubkey := ed25519.PubKey{Key: []byte{40, 249, 115, 32, 97, 18, 1, 1, 127, 255, 103, 13, 1, 34, 1, 24, 179, 242, 2, 151, 3, 34, 6, 1, 11, 0, 194, 202, 201, 77, 1, 167}}
	pubkeyAny, err := codectypes.NewAnyWithValue(&pubkey)
	assert.NilError(t, err)

	validator := stakingtypes.Validator{
		OperatorAddress: validator2,
		ConsensusPubkey: pubkeyAny,
		Jailed:          true,
		Status:          stakingtypes.Bonded,
		Tokens:          math.NewInt(10012),
		DelegatorShares: math.LegacyNewDecWithPrec(96, 2),
		Description: stakingtypes.NewDescription(
			"moniker",
			"identity",
			"website",
			"securityContact",
			"details",
		),
		UnbondingHeight: 100132,
		UnbondingTime:   time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
		Commission: stakingtypes.NewCommission(
			math.LegacyNewDecWithPrec(15, 2),
			math.LegacyNewDecWithPrec(59, 2),
			math.LegacyNewDecWithPrec(51, 2),
		),
		MinSelfDelegation: math.NewInt(1),
	}
	setValidator(f, t, validator)

	return validator
}

// createDelegationAndDelegate funds the delegator account with a random delegation in range 100-1000 and delegates.
func createDelegationAndDelegate(rt *rapid.T, f *deterministicFixture, t *testing.T, delegator sdk.AccAddress, validator stakingtypes.Validator) (newShares math.LegacyDec, err error) {
	// Ensure the delegator account exists before trying to delegate
	acc := f.accountKeeper.GetAccount(f.ctx, delegator)
	if acc == nil {
		acc = f.accountKeeper.NewAccountWithAddress(f.ctx, delegator)
		f.accountKeeper.SetAccount(f.ctx, acc)

		// Verify the account was created properly
		assert.Assert(t, f.accountKeeper.GetAccount(f.ctx, delegator) != nil, "Failed to create account for delegator")
	}

	amt := f.stakingKeeper.TokensFromConsensusPower(f.ctx, rapid.Int64Range(100, 1000).Draw(rt, "amount"))
	return fundAccountAndDelegate(f, t, delegator, validator, amt)
}

// fundAccountAndDelegate funds the delegator account with the specified delegation and delegates.
func fundAccountAndDelegate(f *deterministicFixture, t *testing.T, delegator sdk.AccAddress, validator stakingtypes.Validator, amt math.Int) (newShares math.LegacyDec, err error) {
	coins := sdk.NewCoins(sdk.NewCoin(sdk.DefaultBondDenom, amt))

	// Create account if it doesn't exist
	acc := f.accountKeeper.GetAccount(f.ctx, delegator)
	if acc == nil {
		acc = f.accountKeeper.NewAccountWithAddress(f.ctx, delegator)
		f.accountKeeper.SetAccount(f.ctx, acc)
	}

	// Make sure the account exists before attempting to fund it
	assert.Assert(t, f.accountKeeper.GetAccount(f.ctx, delegator) != nil, "Account should exist at this point")

	assert.NilError(t, f.bankKeeper.MintCoins(f.ctx, minttypes.ModuleName, coins))
	assert.NilError(t, banktestutil.FundAccount(f.ctx, f.bankKeeper, delegator, coins))

	// Verify the account has the funds before delegating
	balance := f.bankKeeper.GetBalance(f.ctx, delegator, sdk.DefaultBondDenom)
	assert.Assert(t, balance.Amount.GTE(amt), "Account should have sufficient funds")

	shares, err := f.stakingKeeper.Delegate(f.ctx, delegator, amt, stakingtypes.Unbonded, validator, true)
	if err != nil {
		t.Logf("Delegation failed: %v, delegator: %s, validator: %s", err, delegator, validator.OperatorAddress)
	}
	return shares, err
}

// safeAddressGenerator generates an address and ensures its account is created in the state
func safeAddressGenerator(rt *rapid.T, f *deterministicFixture, t *testing.T) sdk.AccAddress {
	addr := testdata.AddressGenerator(rt).Draw(rt, "address")

	// Create account if it doesn't exist
	acc := f.accountKeeper.GetAccount(f.ctx, addr)
	if acc == nil {
		acc = f.accountKeeper.NewAccountWithAddress(f.ctx, addr)
		f.accountKeeper.SetAccount(f.ctx, acc)
	}

	return addr
}

func TestGRPCValidator(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Create and set validator with a properly initialized account
		val := createAndSetValidator(rt, f, t)
		req := &stakingtypes.QueryValidatorRequest{
			ValidatorAddr: val.OperatorAddress,
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Validator, 0, true)
	})

	f = initDeterministicFixture(t) // reset
	val := getStaticValidator(f, t)
	req := &stakingtypes.QueryValidatorRequest{
		ValidatorAddr: val.OperatorAddress,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Validator, 1915, false)
}

func TestGRPCValidators(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	validatorStatus := []string{stakingtypes.BondStatusBonded, stakingtypes.BondStatusUnbonded, stakingtypes.BondStatusUnbonding}
	rapid.Check(t, func(rt *rapid.T) {
		valsCount := rapid.IntRange(1, 3).Draw(rt, "num-validators")
		for i := 0; i < valsCount; i++ {
			createAndSetValidator(rt, f, t)
		}

		req := &stakingtypes.QueryValidatorsRequest{
			Status:     validatorStatus[rapid.IntRange(0, 2).Draw(rt, "status")],
			Pagination: testdata.PaginationGenerator(rt, uint64(valsCount)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Validators, 0, true)
	})

	f = initDeterministicFixture(t) // reset
	getStaticValidator(f, t)
	getStaticValidator2(f, t)

	testdata.DeterministicIterations(f.ctx, t, &stakingtypes.QueryValidatorsRequest{}, f.queryClient.Validators, 2862, false)
}

func TestGRPCValidatorDelegations(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		numDels := rapid.IntRange(1, 5).Draw(rt, "num-dels")

		for i := 0; i < numDels; i++ {
			delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")
			_, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
			assert.NilError(t, err)
		}

		req := &stakingtypes.QueryValidatorDelegationsRequest{
			ValidatorAddr: validator.OperatorAddress,
			Pagination:    testdata.PaginationGenerator(rt, uint64(numDels)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.ValidatorDelegations, 14474, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)

	_, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	_, err = fundAccountAndDelegate(f, t, delegatorAddr2, validator, f.amt2)
	assert.NilError(t, err)

	req := &stakingtypes.QueryValidatorDelegationsRequest{
		ValidatorAddr: validator.OperatorAddress,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.ValidatorDelegations, 14475, false)
}

func TestGRPCValidatorUnbondingDelegations(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		numDels := rapid.IntRange(1, 3).Draw(rt, "num-dels")

		for i := 0; i < numDels; i++ {
			delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")
			shares, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
			assert.NilError(t, err)
			valbz, err := f.stakingKeeper.ValidatorAddressCodec().StringToBytes(validator.GetOperator())
			assert.NilError(t, err)
			_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegator, valbz, shares)
			assert.NilError(t, err)
		}

		req := &stakingtypes.QueryValidatorUnbondingDelegationsRequest{
			ValidatorAddr: validator.OperatorAddress,
			Pagination:    testdata.PaginationGenerator(rt, uint64(numDels)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.ValidatorUnbondingDelegations, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)
	shares1, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegatorAddr1, validatorAddr1, shares1)
	assert.NilError(t, err)

	shares2, err := fundAccountAndDelegate(f, t, delegatorAddr2, validator, f.amt2)
	assert.NilError(t, err)

	_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegatorAddr2, validatorAddr1, shares2)
	assert.NilError(t, err)

	req := &stakingtypes.QueryValidatorUnbondingDelegationsRequest{
		ValidatorAddr: validator.OperatorAddress,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.ValidatorUnbondingDelegations, 3719, false)
}

func TestGRPCDelegation(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		delegator := safeAddressGenerator(rt, f, t)
		_, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
		assert.NilError(t, err)

		req := &stakingtypes.QueryDelegationRequest{
			ValidatorAddr: validator.OperatorAddress,
			DelegatorAddr: delegator.String(),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Delegation, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)
	_, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	req := &stakingtypes.QueryDelegationRequest{
		ValidatorAddr: validator.OperatorAddress,
		DelegatorAddr: delegator1,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Delegation, 4635, false)
}

func TestGRPCUnbondingDelegation(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		delegator := safeAddressGenerator(rt, f, t)
		shares, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
		assert.NilError(t, err)

		valbz, err := f.stakingKeeper.ValidatorAddressCodec().StringToBytes(validator.GetOperator())
		assert.NilError(t, err)
		_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegator, valbz, shares)
		assert.NilError(t, err)

		req := &stakingtypes.QueryUnbondingDelegationRequest{
			ValidatorAddr: validator.OperatorAddress,
			DelegatorAddr: delegator.String(),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.UnbondingDelegation, 0, true)
	})

	f = initDeterministicFixture(t) // reset
	validator := getStaticValidator(f, t)

	shares1, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegatorAddr1, validatorAddr1, shares1)
	assert.NilError(t, err)

	req := &stakingtypes.QueryUnbondingDelegationRequest{
		ValidatorAddr: validator.OperatorAddress,
		DelegatorAddr: delegator1,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.UnbondingDelegation, 1621, false)
}

func TestGRPCDelegatorDelegations(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		numVals := rapid.IntRange(1, 3).Draw(rt, "num-dels")
		delegator := safeAddressGenerator(rt, f, t)

		for i := 0; i < numVals; i++ {
			validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
			_, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
			assert.NilError(t, err)
		}

		req := &stakingtypes.QueryDelegatorDelegationsRequest{
			DelegatorAddr: delegator.String(),
			Pagination:    testdata.PaginationGenerator(rt, uint64(numVals)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorDelegations, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)
	_, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	req := &stakingtypes.QueryDelegatorDelegationsRequest{
		DelegatorAddr: delegator1,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorDelegations, 4238, false)
}

func TestGRPCDelegatorValidator(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)

		delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")
		_, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
		assert.NilError(t, err)

		req := &stakingtypes.QueryDelegatorValidatorRequest{
			DelegatorAddr: delegator.String(),
			ValidatorAddr: validator.OperatorAddress,
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorValidator, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)
	_, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)

	assert.NilError(t, err)

	req := &stakingtypes.QueryDelegatorValidatorRequest{
		DelegatorAddr: delegator1,
		ValidatorAddr: validator.OperatorAddress,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorValidator, 3563, false)
}

func TestGRPCDelegatorUnbondingDelegations(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		numVals := rapid.IntRange(1, 5).Draw(rt, "num-vals")
		delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")

		for i := 0; i < numVals; i++ {
			validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
			shares, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
			assert.NilError(t, err)
			valbz, err := f.stakingKeeper.ValidatorAddressCodec().StringToBytes(validator.GetOperator())
			assert.NilError(t, err)
			_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegator, valbz, shares)
			assert.NilError(t, err)
		}

		req := &stakingtypes.QueryDelegatorUnbondingDelegationsRequest{
			DelegatorAddr: delegator.String(),
			Pagination:    testdata.PaginationGenerator(rt, uint64(numVals)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorUnbondingDelegations, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)
	shares1, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	_, _, err = f.stakingKeeper.Undelegate(f.ctx, delegatorAddr1, validatorAddr1, shares1)
	assert.NilError(t, err)

	req := &stakingtypes.QueryDelegatorUnbondingDelegationsRequest{
		DelegatorAddr: delegator1,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorUnbondingDelegations, 1302, false)
}

func TestGRPCHistoricalInfo(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		numVals := rapid.IntRange(1, 5).Draw(rt, "num-vals")
		vals := stakingtypes.Validators{}
		for i := 0; i < numVals; i++ {
			validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
			vals.Validators = append(vals.Validators, validator)
		}

		historicalInfo := stakingtypes.HistoricalInfo{
			Header: cmtproto.Header{},
			Valset: vals.Validators,
		}

		height := rapid.Int64Min(0).Draw(rt, "height")

		assert.NilError(t, f.stakingKeeper.SetHistoricalInfo(
			f.ctx,
			height,
			&historicalInfo,
		))

		req := &stakingtypes.QueryHistoricalInfoRequest{
			Height: height,
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.HistoricalInfo, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)

	historicalInfo := stakingtypes.HistoricalInfo{
		Header: cmtproto.Header{},
		Valset: []stakingtypes.Validator{validator},
	}

	height := int64(127)

	assert.NilError(t, f.stakingKeeper.SetHistoricalInfo(
		f.ctx,
		height,
		&historicalInfo,
	))

	req := &stakingtypes.QueryHistoricalInfoRequest{
		Height: height,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.HistoricalInfo, 1945, false)
}

func TestGRPCDelegatorValidators(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		numVals := rapid.IntRange(1, 3).Draw(rt, "num-dels")
		delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")

		for i := 0; i < numVals; i++ {
			validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
			_, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
			assert.NilError(t, err)
		}

		req := &stakingtypes.QueryDelegatorValidatorsRequest{
			DelegatorAddr: delegator.String(),
			Pagination:    testdata.PaginationGenerator(rt, uint64(numVals)).Draw(rt, "pagination"),
		}

		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorValidators, 0, true)
	})

	f = initDeterministicFixture(t) // reset

	validator := getStaticValidator(f, t)

	_, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	req := &stakingtypes.QueryDelegatorValidatorsRequest{DelegatorAddr: delegator1}
	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.DelegatorValidators, 3166, false)
}

func TestGRPCPool(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		createAndSetValidator(rt, f, t)

		testdata.DeterministicIterations(f.ctx, t, &stakingtypes.QueryPoolRequest{}, f.queryClient.Pool, 0, true)
	})

	f = initDeterministicFixture(t) // reset
	getStaticValidator(f, t)
	testdata.DeterministicIterations(f.ctx, t, &stakingtypes.QueryPoolRequest{}, f.queryClient.Pool, 6242, false)
}

func TestGRPCRedelegations(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		validator := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		srcValAddr, err := sdk.ValAddressFromBech32(validator.OperatorAddress)
		assert.NilError(t, err)

		validator2 := createAndSetValidatorWithStatus(rt, f, t, stakingtypes.Bonded)
		dstValAddr, err := sdk.ValAddressFromBech32(validator2.OperatorAddress)
		assert.NilError(t, err)

		numDels := rapid.IntRange(1, 5).Draw(rt, "num-dels")

		delegator := testdata.AddressGenerator(rt).Draw(rt, "delegator")
		shares, err := createDelegationAndDelegate(rt, f, t, delegator, validator)
		assert.NilError(t, err)

		_, err = f.stakingKeeper.BeginRedelegation(f.ctx, delegator, srcValAddr, dstValAddr, shares)
		assert.NilError(t, err)

		var req *stakingtypes.QueryRedelegationsRequest

		reqType := rapid.IntRange(0, 2).Draw(rt, "req-type")
		switch reqType {
		case 0: // queries redelegation with delegator, source and destination validators combination.
			req = &stakingtypes.QueryRedelegationsRequest{
				DelegatorAddr:    delegator.String(),
				SrcValidatorAddr: srcValAddr.String(),
				DstValidatorAddr: dstValAddr.String(),
			}
		case 1: // queries redelegations of source validator.
			req = &stakingtypes.QueryRedelegationsRequest{
				SrcValidatorAddr: srcValAddr.String(),
			}
		case 2: // queries all redelegations of a delegator.
			req = &stakingtypes.QueryRedelegationsRequest{
				DelegatorAddr: delegator.String(),
			}
		}

		req.Pagination = testdata.PaginationGenerator(rt, uint64(numDels)).Draw(rt, "pagination")
		testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Redelegations, 0, true)
	})

	f = initDeterministicFixture(t) // reset
	validator := getStaticValidator(f, t)
	_ = getStaticValidator2(f, t)

	shares, err := fundAccountAndDelegate(f, t, delegatorAddr1, validator, f.amt1)
	assert.NilError(t, err)

	_, err = f.stakingKeeper.BeginRedelegation(f.ctx, delegatorAddr1, validatorAddr1, validatorAddr2, shares)
	assert.NilError(t, err)

	req := &stakingtypes.QueryRedelegationsRequest{
		DelegatorAddr:    delegator1,
		SrcValidatorAddr: validator1,
		DstValidatorAddr: validator2,
	}

	testdata.DeterministicIterations(f.ctx, t, req, f.queryClient.Redelegations, 3920, false)
}

func TestGRPCParams(t *testing.T) {
	t.Parallel()
	f := initDeterministicFixture(t)

	rapid.Check(t, func(rt *rapid.T) {
		params := stakingtypes.Params{
			BondDenom:         rapid.StringMatching(sdk.DefaultCoinDenomRegex()).Draw(rt, "bond-denom"),
			UnbondingTime:     durationGenerator().Draw(rt, "duration"),
			MaxValidators:     rapid.Uint32Min(1).Draw(rt, "max-validators"),
			MaxEntries:        rapid.Uint32Min(1).Draw(rt, "max-entries"),
			HistoricalEntries: rapid.Uint32Min(1).Draw(rt, "historical-entries"),
			MinCommissionRate: math.LegacyNewDecWithPrec(rapid.Int64Range(0, 100).Draw(rt, "commission"), 2),
		}

		err := f.stakingKeeper.SetParams(f.ctx, params)
		assert.NilError(t, err)

		testdata.DeterministicIterations(f.ctx, t, &stakingtypes.QueryParamsRequest{}, f.queryClient.Params, 0, true)
	})

	params := stakingtypes.Params{
		BondDenom:         "denom",
		UnbondingTime:     time.Hour,
		MaxValidators:     85,
		MaxEntries:        5,
		HistoricalEntries: 5,
		MinCommissionRate: math.LegacyNewDecWithPrec(5, 2),
	}

	err := f.stakingKeeper.SetParams(f.ctx, params)
	assert.NilError(t, err)

	testdata.DeterministicIterations(f.ctx, t, &stakingtypes.QueryParamsRequest{}, f.queryClient.Params, 1114, false)
}
