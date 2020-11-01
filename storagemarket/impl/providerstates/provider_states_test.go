// +build ignore

package providerstates_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-multistore"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-statemachine/fsm"
	fsmtest "github.com/filecoin-project/go-statemachine/fsm/testutil"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	satesting "github.com/filecoin-project/specs-actors/support/testing"

	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/shared"
	tut "github.com/filecoin-project/go-fil-markets/shared_testutil"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/blockrecorder"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/funds"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/providerstates"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-fil-markets/storagemarket/testnodes"
)

func TestValidateDealProposal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runValidateDealProposal := makeExecutor(ctx, eventProcessor, providerstates.ValidateDealProposal, storagemarket.StorageDealValidating)
	otherAddr, err := address.NewActorAddress([]byte("applesauce"))
	require.NoError(t, err)
	bigDataCap := big.NewIntUnsigned(uint64(defaultPieceSize))
	smallDataCap := big.NewIntUnsigned(uint64(defaultPieceSize - 1))

	invalidLabelBytes := make([]byte, 257)
	rand.Read(invalidLabelBytes)
	invalidLabel := base64.StdEncoding.EncodeToString(invalidLabelBytes)

	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealAcceptWait, deal.State)
				require.Len(t, env.peerTagger.TagCalls, 1)
				require.Equal(t, deal.Client, env.peerTagger.TagCalls[0])
			},
		},
		"verify signature fails": {
			nodeParams: nodeParams{
				VerifySignatureFails: true,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: verifying StorageDealProposal: could not verify signature", deal.Message)
			},
		},
		"provider address does not match": {
			environmentParams: environmentParams{
				Address: otherAddr,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: incorrect provider for deal", deal.Message)
			},
		},
		"MostRecentStateID errors": {
			nodeParams: nodeParams{
				MostRecentStateIDError: errors.New("couldn't get id"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: node error getting most recent state id: couldn't get id", deal.Message)
			},
		},
		"PricePerEpoch too low": {
			dealParams: dealParams{
				StoragePricePerEpoch: abi.NewTokenAmount(5000),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: storage price per epoch less than asking price: 5000 < 9765", deal.Message)
			},
		},
		"PieceSize < MinPieceSize": {
			dealParams: dealParams{
				PieceSize: abi.PaddedPieceSize(128),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: piece size less than minimum required size: 128 < 256", deal.Message)
			},
		},
		"Get balance error": {
			nodeParams: nodeParams{
				ClientMarketBalanceError: errors.New("could not get balance"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: node error getting client market balance failed: could not get balance", deal.Message)
			},
		},
		"Not enough funds": {
			nodeParams: nodeParams{
				ClientMarketBalance: big.NewInt(200*10000 - 1),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.True(t, strings.Contains(deal.Message, "deal rejected: clientMarketBalance.Available too small"))
			},
		},
		"Not enough funds due to client collateral": {
			nodeParams: nodeParams{
				ClientMarketBalance: big.NewInt(200*10000 + 99),
			},
			dealParams: dealParams{
				ClientCollateral: big.NewInt(100),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.True(t, strings.Contains(deal.Message, "deal rejected: clientMarketBalance.Available too small"))
			},
		},
		"verified deal succeeds": {
			dealParams: dealParams{
				VerifiedDeal: true,
			},
			nodeParams: nodeParams{
				DataCap: &bigDataCap,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.True(t, deal.Proposal.VerifiedDeal)
				tut.AssertDealState(t, storagemarket.StorageDealAcceptWait, deal.State)
			},
		},
		"verified deal fails getting client data cap": {
			dealParams: dealParams{
				VerifiedDeal: true,
			},
			nodeParams: nodeParams{
				GetDataCapError: xerrors.Errorf("failure getting data cap"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.True(t, deal.Proposal.VerifiedDeal)
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: node error fetching verified data cap: failure getting data cap", deal.Message)
			},
		},
		"verified deal fails data cap not found": {
			dealParams: dealParams{
				VerifiedDeal: true,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.True(t, deal.Proposal.VerifiedDeal)
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: node error fetching verified data cap: data cap missing -- client not verified", deal.Message)
			},
		},
		"verified deal fails with insufficient data cap": {
			dealParams: dealParams{
				VerifiedDeal: true,
			},
			nodeParams: nodeParams{
				DataCap: &smallDataCap,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.True(t, deal.Proposal.VerifiedDeal)
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: verified deal DataCap too small for proposed piece size", deal.Message)
			},
		},
		"label is too long": {
			dealParams: dealParams{
				Label: invalidLabel,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: deal label can be at most 256 bytes, is 344", deal.Message)
			},
		},
		"invalid piece size": {
			dealParams: dealParams{
				PieceSize: 129,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: proposal piece size is invalid: padded piece size must be a power of 2", deal.Message)
			},
		},
		"invalid piece cid prefix": {
			dealParams: dealParams{
				PieceCid: &tut.GenerateCids(1)[0],
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: proposal PieceCID had wrong prefix", deal.Message)
			},
		},
		"end epoch before start": {
			dealParams: dealParams{
				StartEpoch: 1000,
				EndEpoch:   900,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: proposal end before proposal start", deal.Message)
			},
		},
		"start epoch has already passed": {
			dealParams: dealParams{
				StartEpoch: defaultHeight - 1,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: deal start epoch has already elapsed", deal.Message)
			},
		},
		"deal duration too short (less than 180 days)": {
			dealParams: dealParams{
				StartEpoch: defaultHeight,
				EndEpoch:   defaultHeight + builtin.EpochsInDay*180 - 1,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.True(t, strings.Contains(deal.Message, "deal rejected: deal duration out of bounds"))
			},
		},
		"deal duration too long (more than 540 days)": {
			nodeParams: nodeParams{
				ClientMarketBalance: big.Mul(abi.NewTokenAmount(builtin.EpochsInDay*54+1), defaultStoragePricePerEpoch),
			},
			dealParams: dealParams{
				StartEpoch: defaultHeight,
				EndEpoch:   defaultHeight + builtin.EpochsInDay*540 + 1,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.True(t, strings.Contains(deal.Message, "deal rejected: deal duration out of bounds"))
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runValidateDealProposal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestDecideOnProposal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runDecideOndeal := makeExecutor(ctx, eventProcessor, providerstates.DecideOnProposal, storagemarket.StorageDealAcceptWait)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealWaitingForData, deal.State)
			},
		},
		"Custom Decision Rejects Deal": {
			environmentParams: environmentParams{
				RejectDeal:   true,
				RejectReason: "I just don't like it",
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: I just don't like it", deal.Message)
			},
		},
		"Custom Decision Errors": {
			environmentParams: environmentParams{
				DecisionError: errors.New("I can't make up my mind"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealRejecting, deal.State)
				require.Equal(t, "deal rejected: custom deal decision logic failed: I can't make up my mind", deal.Message)
			},
		},
		"SendSignedResponse errors": {
			environmentParams: environmentParams{
				SendSignedResponseError: errors.New("could not send"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "sending response to deal: could not send", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runDecideOndeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestVerifyData(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	expPath := filestore.Path("applesauce.txt")
	expMetaPath := filestore.Path("somemetadata.txt")
	runVerifyData := makeExecutor(ctx, eventProcessor, providerstates.VerifyData, storagemarket.StorageDealVerifyData)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			environmentParams: environmentParams{
				Path:         expPath,
				MetadataPath: expMetaPath,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealEnsureProviderFunds, deal.State)
				require.Equal(t, expPath, deal.PiecePath)
				require.Equal(t, expMetaPath, deal.MetadataPath)

			},
		},
		"generate piece CID fails": {
			environmentParams: environmentParams{
				GenerateCommPError: errors.New("could not generate CommP"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "deal data verification failed: error generating CommP: could not generate CommP", deal.Message)
			},
		},
		"piece CIDs do not match": {
			environmentParams: environmentParams{
				PieceCid: tut.GenerateCids(1)[0],
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "deal data verification failed: proposal CommP doesn't match calculated CommP", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runVerifyData(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestWaitForFunding(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runWaitForFunding := makeExecutor(ctx, eventProcessor, providerstates.WaitForFunding, storagemarket.StorageDealProviderFunding)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			nodeParams: nodeParams{
				WaitForMessageExitCode: exitcode.Ok,
				WaitForMessageRetBytes: []byte{},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealPublish, deal.State)
			},
		},
		"AddFunds returns non-ok exit code": {
			nodeParams: nodeParams{
				WaitForMessageExitCode: exitcode.ErrInsufficientFunds,
				WaitForMessageRetBytes: []byte{},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, fmt.Sprintf("error calling node: AddFunds exit code: %s", exitcode.ErrInsufficientFunds), deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runWaitForFunding(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestEnsureProviderFunds(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runEnsureProviderFunds := makeExecutor(ctx, eventProcessor, providerstates.EnsureProviderFunds, storagemarket.StorageDealEnsureProviderFunds)
	cids := tut.GenerateCids(1)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds immediately": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealPublish, deal.State)
				require.Equal(t, env.dealFunds.ReserveCalls[0], deal.Proposal.ProviderBalanceRequirement())
				require.Len(t, env.dealFunds.ReleaseCalls, 0)
				require.Equal(t, deal.Proposal.ProviderBalanceRequirement(), deal.FundsReserved)
			},
		},
		"succeeds, funds already reserved": {
			dealParams: dealParams{
				ReserveFunds: true,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealPublish, deal.State)
				require.Len(t, env.dealFunds.ReserveCalls, 0)
				require.Len(t, env.dealFunds.ReleaseCalls, 0)
			},
		},
		"succeeds by sending an AddBalance message": {
			dealParams: dealParams{
				ProviderCollateral: abi.NewTokenAmount(1),
			},
			nodeParams: nodeParams{
				AddFundsCid: cids[0],
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealProviderFunding, deal.State)
				require.Equal(t, &cids[0], deal.AddFundsCid)
				require.Equal(t, env.dealFunds.ReserveCalls[0], deal.Proposal.ProviderBalanceRequirement())
				require.Len(t, env.dealFunds.ReleaseCalls, 0)
			},
		},
		"get miner worker fails": {
			nodeParams: nodeParams{
				MinerWorkerError: errors.New("could not get worker"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "error calling node: looking up miner worker: could not get worker", deal.Message)
			},
		},
		"ensureFunds errors": {
			nodeParams: nodeParams{
				EnsureFundsError: errors.New("not enough funds"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "error calling node: ensuring funds: not enough funds", deal.Message)
				require.Equal(t, env.dealFunds.ReserveCalls[0], deal.Proposal.ProviderBalanceRequirement())
				require.Len(t, env.dealFunds.ReleaseCalls, 0)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runEnsureProviderFunds(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestRestartDataTransfer(t *testing.T) {
	channelId := datatransfer.ChannelID{Initiator: peer.ID("1"), Responder: peer.ID("2")}
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runRestartDataTransfer := makeExecutor(ctx, eventProcessor, providerstates.RestartDataTransfer, storagemarket.StorageDealProviderTransferRestart)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealParams: dealParams{
				TransferChannelId: &channelId,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.Eventually(t, func() bool {
					return len(env.restartDataTransferCalls) == 1
				}, 5*time.Second, 200*time.Millisecond)
				require.Equal(t, channelId, env.restartDataTransferCalls[0].chId)
				tut.AssertDealState(t, storagemarket.StorageDealProviderTransferRestart, deal.State)
			},
		},
		// TODO FIXME
		/*"RestartDataTransfer errors": {
			dealParams: dealParams{
				TransferChannelId: &channelId,
			},
			environmentParams: environmentParams{
				RestartDataTransferError: xerrors.New("some error"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				require.Eventually(t, func() bool {
					fmt.Printf("\n deal state is %s", storagemarket.DealStates[deal.State])
					return deal.State == storagemarket.StorageDealFailing
				}, 5*time.Second, 200*time.Millisecond)

				require.Equal(t, "error restarting data transfer: some error", deal.Message)
			},
		},*/
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runRestartDataTransfer(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestPublishDeal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runPublishDeal := makeExecutor(ctx, eventProcessor, providerstates.PublishDeal, storagemarket.StorageDealPublish)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealPublishing, deal.State)
			},
		},
		"PublishDealsErrors errors": {
			nodeParams: nodeParams{
				PublishDealsError: errors.New("could not post to chain"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "error calling node: publishing deal: could not post to chain", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runPublishDeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestWaitForPublish(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runWaitForPublish := makeExecutor(ctx, eventProcessor, providerstates.WaitForPublish, storagemarket.StorageDealPublishing)
	expDealID, psdReturnBytes := generatePublishDealsReturn(t)
	finalCid := tut.GenerateCids(10)[9]

	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealParams: dealParams{
				ReserveFunds: true,
			},
			nodeParams: nodeParams{
				WaitForMessageRetBytes:   psdReturnBytes,
				WaitForMessagePublishCid: finalCid,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealStaged, deal.State)
				require.Equal(t, expDealID, deal.DealID)
				assert.Equal(t, env.dealFunds.ReleaseCalls[0], deal.Proposal.ProviderBalanceRequirement())
				assert.True(t, deal.FundsReserved.Nil() || deal.FundsReserved.IsZero())
				assert.Equal(t, deal.PublishCid, &finalCid)
			},
		},
		"succeeds, funds already released": {
			nodeParams: nodeParams{
				WaitForMessageRetBytes: psdReturnBytes,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealStaged, deal.State)
				require.Equal(t, expDealID, deal.DealID)
				assert.Len(t, env.dealFunds.ReleaseCalls, 0)
			},
		},
		"PublishStorageDeal errors": {
			nodeParams: nodeParams{
				WaitForMessageExitCode: exitcode.SysErrForbidden,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "PublishStorageDeal error: PublishStorageDeals exit code: SysErrForbidden(8)", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runWaitForPublish(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestHandoffDeal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runHandoffDeal := makeExecutor(ctx, eventProcessor, providerstates.HandoffDeal, storagemarket.StorageDealStaged)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealParams: dealParams{
				PiecePath:     defaultPath,
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSealing, deal.State)
				require.Len(t, env.node.OnDealCompleteCalls, 1)
				require.True(t, env.node.OnDealCompleteCalls[0].FastRetrieval)
				require.True(t, deal.AvailableForRetrieval)
			},
		},
		"succeeds w metadata": {
			dealParams: dealParams{
				PiecePath:     defaultPath,
				MetadataPath:  defaultMetadataPath,
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile, defaultMetadataFile},
				ExpectedOpens: []filestore.Path{defaultPath, defaultMetadataPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSealing, deal.State)
				require.Len(t, env.node.OnDealCompleteCalls, 1)
				require.True(t, env.node.OnDealCompleteCalls[0].FastRetrieval)
				require.True(t, deal.AvailableForRetrieval)
			},
		},
		"reading metadata fails": {
			dealParams: dealParams{
				PiecePath:     defaultPath,
				MetadataPath:  filestore.Path("Missing.txt"),
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSealing, deal.State)
				require.Equal(t, fmt.Sprintf("recording piece for retrieval: failed to load block locations: file not found"), deal.Message)
			},
		},
		"add piece block locations errors": {
			dealParams: dealParams{
				PiecePath:     defaultPath,
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			pieceStoreParams: tut.TestPieceStoreParams{
				AddPieceBlockLocationsError: errors.New("could not add block locations"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSealing, deal.State)
				require.Equal(t, "recording piece for retrieval: failed to add piece block locations: could not add block locations", deal.Message)
			},
		},
		"add deal for piece errors": {
			dealParams: dealParams{
				PiecePath:     defaultPath,
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			pieceStoreParams: tut.TestPieceStoreParams{
				AddDealForPieceError: errors.New("could not add deal info"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSealing, deal.State)
				require.Equal(t, "recording piece for retrieval: failed to add deal for piece: could not add deal info", deal.Message)
			},
		},
		"deleting store fails": {
			environmentParams: environmentParams{
				DeleteStoreError: errors.New("something awful has happened"),
			},
			dealParams: dealParams{
				PiecePath:     defaultPath,
				FastRetrieval: true,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, deal.Message, fmt.Sprintf("operating on multistore: unable to delete store %d: something awful has happened", *deal.StoreID))
			},
		},
		"opening file errors": {
			dealParams: dealParams{
				PiecePath: filestore.Path("missing.txt"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, fmt.Sprintf("accessing file store: reading piece at path missing.txt: %s", tut.TestErrNotFound.Error()), deal.Message)
			},
		},
		"OnDealComplete errors": {
			dealParams: dealParams{
				PiecePath: defaultPath,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:         []filestore.File{defaultDataFile},
				ExpectedOpens: []filestore.Path{defaultPath},
			},
			nodeParams: nodeParams{
				OnDealCompleteError: errors.New("failed building sector"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "handing off deal to node: failed building sector", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runHandoffDeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestVerifyDealActivated(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runVerifyDealActivated := makeExecutor(ctx, eventProcessor, providerstates.VerifyDealActivated, storagemarket.StorageDealSealing)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFinalizing, deal.State)
			},
		},
		"sync error": {
			nodeParams: nodeParams{
				DealCommittedSyncError: errors.New("couldn't check deal commitment"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "error activating deal: couldn't check deal commitment", deal.Message)
			},
		},
		"async error": {
			nodeParams: nodeParams{
				DealCommittedAsyncError: errors.New("deal did not appear on chain"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, "error activating deal: deal did not appear on chain", deal.Message)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runVerifyDealActivated(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestCleanupDeal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runCleanupDeal := makeExecutor(ctx, eventProcessor, providerstates.CleanupDeal, storagemarket.StorageDealFinalizing)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealParams: dealParams{
				PiecePath: defaultPath,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:             []filestore.File{defaultDataFile},
				ExpectedDeletions: []filestore.Path{defaultPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealActive, deal.State)
			},
		},
		"succeeds w metadata": {
			dealParams: dealParams{
				PiecePath:    defaultPath,
				MetadataPath: defaultMetadataPath,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:             []filestore.File{defaultDataFile, defaultMetadataFile},
				ExpectedDeletions: []filestore.Path{defaultMetadataPath, defaultPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealActive, deal.State)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runCleanupDeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestWaitForDealCompletion(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runWaitForDealCompletion := makeExecutor(ctx, eventProcessor, providerstates.WaitForDealCompletion, storagemarket.StorageDealActive)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"slashing succeeds": {
			nodeParams: nodeParams{OnDealSlashedEpoch: abi.ChainEpoch(5)},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealSlashed, deal.State)
				require.Equal(t, abi.ChainEpoch(5), deal.SlashEpoch)
				require.Len(t, env.peerTagger.UntagCalls, 1)
				require.Equal(t, deal.Client, env.peerTagger.UntagCalls[0])
			},
		},
		"expiration succeeds": {
			// OnDealSlashedEpoch of zero signals to test node to call onDealExpired()
			nodeParams: nodeParams{OnDealSlashedEpoch: abi.ChainEpoch(0)},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealExpired, deal.State)
				require.Len(t, env.peerTagger.UntagCalls, 1)
				require.Equal(t, deal.Client, env.peerTagger.UntagCalls[0])
			},
		},
		"slashing fails": {
			nodeParams: nodeParams{OnDealSlashedError: errors.New("an err")},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
				require.Equal(t, "error waiting for deal completion: deal slashing err: an err", deal.Message)
			},
		},
		"expiration fails": {
			nodeParams: nodeParams{OnDealExpiredError: errors.New("an err")},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
				require.Equal(t, "error waiting for deal completion: deal expiration err: an err", deal.Message)
			},
		},
		"fails synchronously": {
			nodeParams: nodeParams{WaitForDealCompletionError: errors.New("an err")},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
				require.Equal(t, "error waiting for deal completion: an err", deal.Message)
			},
		},
	}

	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runWaitForDealCompletion(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestRejectDeal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runRejectDeal := makeExecutor(ctx, eventProcessor, providerstates.RejectDeal, storagemarket.StorageDealRejecting)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, 1, env.disconnectCalls)
			},
		},
		"fails if it cannot send a response": {
			environmentParams: environmentParams{
				SendSignedResponseError: xerrors.New("error sending response"),
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealFailing, deal.State)
				require.Equal(t, deal.Message, "sending response to deal: error sending response")
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runRejectDeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

func TestFailDeal(t *testing.T) {
	ctx := context.Background()
	eventProcessor, err := fsm.NewEventProcessor(storagemarket.MinerDeal{}, "State", providerstates.ProviderEvents)
	require.NoError(t, err)
	runFailDeal := makeExecutor(ctx, eventProcessor, providerstates.FailDeal, storagemarket.StorageDealFailing)
	tests := map[string]struct {
		nodeParams        nodeParams
		dealParams        dealParams
		environmentParams environmentParams
		fileStoreParams   tut.TestFileStoreParams
		pieceStoreParams  tut.TestPieceStoreParams
		dealInspector     func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)
	}{
		"succeeds": {
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
			},
		},
		"succeeds, funds released": {
			dealParams: dealParams{
				ReserveFunds: true,
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
				assert.Equal(t, env.dealFunds.ReleaseCalls[0], deal.Proposal.ProviderBalanceRequirement())
				assert.True(t, deal.FundsReserved.Nil() || deal.FundsReserved.IsZero())
			},
		},
		"succeeds, file deletions": {
			dealParams: dealParams{
				PiecePath:    defaultPath,
				MetadataPath: defaultMetadataPath,
			},
			fileStoreParams: tut.TestFileStoreParams{
				Files:             []filestore.File{defaultDataFile, defaultMetadataFile},
				ExpectedDeletions: []filestore.Path{defaultPath, defaultMetadataPath},
			},
			dealInspector: func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment) {
				tut.AssertDealState(t, storagemarket.StorageDealError, deal.State)
			},
		},
	}
	for test, data := range tests {
		t.Run(test, func(t *testing.T) {
			runFailDeal(t, data.nodeParams, data.environmentParams, data.dealParams, data.fileStoreParams, data.pieceStoreParams, data.dealInspector)
		})
	}
}

// all of these default parameters are setup to allow a deal to complete each handler with no errors
var defaultHeight = abi.ChainEpoch(50)
var defaultTipSetToken = []byte{1, 2, 3}
var defaultStoragePricePerEpoch = abi.NewTokenAmount(10000)
var defaultPieceSize = abi.PaddedPieceSize(1048576)
var defaultStartEpoch = abi.ChainEpoch(200)
var defaultEndEpoch = defaultStartEpoch + ((24*3600)/30)*200 // 200 days
var defaultPieceCid = satesting.MakeCID("piece cid", &market.PieceCIDPrefix)
var defaultPath = filestore.Path("file.txt")
var defaultMetadataPath = filestore.Path("metadataPath.txt")
var defaultClientAddress = address.TestAddress
var defaultProviderAddress = address.TestAddress2
var defaultMinerAddr, _ = address.NewActorAddress([]byte("miner"))
var defaultClientCollateral = abi.NewTokenAmount(0)
var defaultProviderCollateral = abi.NewTokenAmount(10000)
var defaultDataRef = storagemarket.DataRef{
	Root:         tut.GenerateCids(1)[0],
	TransferType: storagemarket.TTGraphsync,
}
var defaultClientMarketBalance = big.Mul(big.NewInt(int64(defaultEndEpoch-defaultStartEpoch)), defaultStoragePricePerEpoch)

var defaultAsk = storagemarket.StorageAsk{
	Price:         abi.NewTokenAmount(10000000),
	VerifiedPrice: abi.NewTokenAmount(1000000),
	MinPieceSize:  abi.PaddedPieceSize(256),
	MaxPieceSize:  1 << 20,
}

var testData = tut.NewTestIPLDTree()
var dataBuf = new(bytes.Buffer)
var blockLocationBuf = new(bytes.Buffer)
var _ error = testData.DumpToCar(dataBuf, blockrecorder.RecordEachBlockTo(blockLocationBuf))
var defaultDataFile = tut.NewTestFile(tut.TestFileParams{
	Buffer: dataBuf,
	Path:   defaultPath,
	Size:   400,
})
var defaultMetadataFile = tut.NewTestFile(tut.TestFileParams{
	Buffer: blockLocationBuf,
	Path:   defaultMetadataPath,
	Size:   400,
})

func generatePublishDealsReturn(t *testing.T) (abi.DealID, []byte) {
	dealId := abi.DealID(rand.Uint64())

	psdReturn := market.PublishStorageDealsReturn{IDs: []abi.DealID{dealId}}
	psdReturnBytes := bytes.NewBuffer([]byte{})
	err := psdReturn.MarshalCBOR(psdReturnBytes)
	require.NoError(t, err)

	return dealId, psdReturnBytes.Bytes()
}

type nodeParams struct {
	MinerAddr                           address.Address
	MinerWorkerError                    error
	EnsureFundsError                    error
	Height                              abi.ChainEpoch
	TipSetToken                         shared.TipSetToken
	ClientMarketBalance                 abi.TokenAmount
	ClientMarketBalanceError            error
	AddFundsCid                         cid.Cid
	VerifySignatureFails                bool
	MostRecentStateIDError              error
	PieceLength                         uint64
	PieceSectorID                       uint64
	PublishDealsError                   error
	OnDealCompleteError                 error
	LocatePieceForDealWithinSectorError error
	DealCommittedSyncError              error
	DealCommittedAsyncError             error
	WaitForMessageBlocks                bool
	WaitForMessagePublishCid            cid.Cid
	WaitForMessageError                 error
	WaitForMessageExitCode              exitcode.ExitCode
	WaitForMessageRetBytes              []byte
	WaitForDealCompletionError          error
	OnDealExpiredError                  error
	OnDealSlashedError                  error
	OnDealSlashedEpoch                  abi.ChainEpoch
	DataCap                             *verifreg.DataCap
	GetDataCapError                     error
}

type dealParams struct {
	PieceCid             *cid.Cid
	PiecePath            filestore.Path
	MetadataPath         filestore.Path
	DealID               abi.DealID
	DataRef              *storagemarket.DataRef
	StoragePricePerEpoch abi.TokenAmount
	ProviderCollateral   abi.TokenAmount
	ClientCollateral     abi.TokenAmount
	PieceSize            abi.PaddedPieceSize
	StartEpoch           abi.ChainEpoch
	EndEpoch             abi.ChainEpoch
	FastRetrieval        bool
	VerifiedDeal         bool
	ReserveFunds         bool
	TransferChannelId    *datatransfer.ChannelID
	Label                string
}

type environmentParams struct {
	Address                  address.Address
	Ask                      storagemarket.StorageAsk
	DataTransferError        error
	PieceCid                 cid.Cid
	Path                     filestore.Path
	MetadataPath             filestore.Path
	GenerateCommPError       error
	SendSignedResponseError  error
	DisconnectError          error
	TagsProposal             bool
	RejectDeal               bool
	RejectReason             string
	DecisionError            error
	DeleteStoreError         error
	RestartDataTransferError error
}

type executor func(t *testing.T,
	node nodeParams,
	params environmentParams,
	dealParams dealParams,
	fileStoreParams tut.TestFileStoreParams,
	pieceStoreParams tut.TestPieceStoreParams,
	dealInspector func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment))

func makeExecutor(ctx context.Context,
	eventProcessor fsm.EventProcessor,
	stateEntryFunc providerstates.ProviderStateEntryFunc,
	initialState storagemarket.StorageDealStatus) executor {
	return func(t *testing.T,
		nodeParams nodeParams,
		params environmentParams,
		dealParams dealParams,
		fileStoreParams tut.TestFileStoreParams,
		pieceStoreParams tut.TestPieceStoreParams,
		dealInspector func(t *testing.T, deal storagemarket.MinerDeal, env *fakeEnvironment)) {

		smstate := testnodes.NewStorageMarketState()
		if nodeParams.Height != abi.ChainEpoch(0) {
			smstate.Epoch = nodeParams.Height
			smstate.TipSetToken = nodeParams.TipSetToken
		} else {
			smstate.Epoch = defaultHeight
			smstate.TipSetToken = defaultTipSetToken
		}
		if !nodeParams.ClientMarketBalance.Nil() {
			smstate.AddFunds(defaultClientAddress, nodeParams.ClientMarketBalance)
		} else {
			smstate.AddFunds(defaultClientAddress, defaultClientMarketBalance)
		}

		common := testnodes.FakeCommonNode{
			SMState:                    smstate,
			GetChainHeadError:          nodeParams.MostRecentStateIDError,
			GetBalanceError:            nodeParams.ClientMarketBalanceError,
			VerifySignatureFails:       nodeParams.VerifySignatureFails,
			EnsureFundsError:           nodeParams.EnsureFundsError,
			DealCommittedSyncError:     nodeParams.DealCommittedSyncError,
			DealCommittedAsyncError:    nodeParams.DealCommittedAsyncError,
			AddFundsCid:                nodeParams.AddFundsCid,
			WaitForMessageBlocks:       nodeParams.WaitForMessageBlocks,
			WaitForMessageError:        nodeParams.WaitForMessageError,
			WaitForMessageFinalCid:     nodeParams.WaitForMessagePublishCid,
			WaitForMessageExitCode:     nodeParams.WaitForMessageExitCode,
			WaitForMessageRetBytes:     nodeParams.WaitForMessageRetBytes,
			WaitForDealCompletionError: nodeParams.WaitForDealCompletionError,
			OnDealExpiredError:         nodeParams.OnDealExpiredError,
			OnDealSlashedError:         nodeParams.OnDealSlashedError,
			OnDealSlashedEpoch:         nodeParams.OnDealSlashedEpoch,
		}

		node := &testnodes.FakeProviderNode{
			FakeCommonNode:                      common,
			MinerAddr:                           nodeParams.MinerAddr,
			MinerWorkerError:                    nodeParams.MinerWorkerError,
			PieceLength:                         nodeParams.PieceLength,
			PieceSectorID:                       nodeParams.PieceSectorID,
			PublishDealsError:                   nodeParams.PublishDealsError,
			OnDealCompleteError:                 nodeParams.OnDealCompleteError,
			LocatePieceForDealWithinSectorError: nodeParams.LocatePieceForDealWithinSectorError,
			DataCap:                             nodeParams.DataCap,
			GetDataCapErr:                       nodeParams.GetDataCapError,
		}

		if nodeParams.MinerAddr == address.Undef {
			node.MinerAddr = defaultMinerAddr
		}

		proposal := market.DealProposal{
			PieceCID:             defaultPieceCid,
			PieceSize:            defaultPieceSize,
			Client:               defaultClientAddress,
			Provider:             defaultProviderAddress,
			StartEpoch:           defaultStartEpoch,
			EndEpoch:             defaultEndEpoch,
			StoragePricePerEpoch: defaultStoragePricePerEpoch,
			ProviderCollateral:   defaultProviderCollateral,
			ClientCollateral:     defaultClientCollateral,
			Label:                dealParams.Label,
		}
		if dealParams.PieceCid != nil {
			proposal.PieceCID = *dealParams.PieceCid
		}
		if !dealParams.StoragePricePerEpoch.Nil() {
			proposal.StoragePricePerEpoch = dealParams.StoragePricePerEpoch
		}
		if !dealParams.ProviderCollateral.Nil() {
			proposal.ProviderCollateral = dealParams.ProviderCollateral
		}
		if !dealParams.ClientCollateral.Nil() {
			proposal.ClientCollateral = dealParams.ClientCollateral
		}
		if dealParams.StartEpoch != abi.ChainEpoch(0) {
			proposal.StartEpoch = dealParams.StartEpoch
		}
		if dealParams.EndEpoch != abi.ChainEpoch(0) {
			proposal.EndEpoch = dealParams.EndEpoch
		}
		if dealParams.PieceSize != abi.PaddedPieceSize(0) {
			proposal.PieceSize = dealParams.PieceSize
		}
		proposal.VerifiedDeal = dealParams.VerifiedDeal
		signedProposal := &market.ClientDealProposal{
			Proposal:        proposal,
			ClientSignature: *tut.MakeTestSignature(),
		}
		dataRef := &defaultDataRef
		if dealParams.DataRef != nil {
			dataRef = dealParams.DataRef
		}
		dealState, err := tut.MakeTestMinerDeal(initialState,
			signedProposal, dataRef)
		require.NoError(t, err)
		dealState.AddFundsCid = &tut.GenerateCids(1)[0]
		dealState.PublishCid = &tut.GenerateCids(1)[0]
		if dealParams.PiecePath != filestore.Path("") {
			dealState.PiecePath = dealParams.PiecePath
		}
		if dealParams.MetadataPath != filestore.Path("") {
			dealState.MetadataPath = dealParams.MetadataPath
		}
		if dealParams.DealID != abi.DealID(0) {
			dealState.DealID = dealParams.DealID
		}
		dealState.FastRetrieval = dealParams.FastRetrieval
		if dealParams.ReserveFunds {
			dealState.FundsReserved = proposal.ProviderCollateral
		}
		if dealParams.TransferChannelId != nil {
			dealState.TransferChannelId = dealParams.TransferChannelId
		}

		fs := tut.NewTestFileStore(fileStoreParams)
		pieceStore := tut.NewTestPieceStoreWithParams(pieceStoreParams)
		expectedTags := make(map[string]struct{})
		if params.TagsProposal {
			expectedTags[dealState.ProposalCid.String()] = struct{}{}
		}
		environment := &fakeEnvironment{
			expectedTags:            expectedTags,
			receivedTags:            make(map[string]struct{}),
			address:                 params.Address,
			node:                    node,
			ask:                     params.Ask,
			dataTransferError:       params.DataTransferError,
			pieceCid:                params.PieceCid,
			path:                    params.Path,
			metadataPath:            params.MetadataPath,
			generateCommPError:      params.GenerateCommPError,
			sendSignedResponseError: params.SendSignedResponseError,
			disconnectError:         params.DisconnectError,
			rejectDeal:              params.RejectDeal,
			rejectReason:            params.RejectReason,
			decisionError:           params.DecisionError,
			deleteStoreError:        params.DeleteStoreError,
			fs:                      fs,
			pieceStore:              pieceStore,
			dealFunds:               tut.NewTestDealFunds(),
			peerTagger:              tut.NewTestPeerTagger(),

			restartDataTransferError: params.RestartDataTransferError,
		}
		if environment.pieceCid == cid.Undef {
			environment.pieceCid = defaultPieceCid
		}
		if environment.path == filestore.Path("") {
			environment.path = defaultPath
		}
		if environment.metadataPath == filestore.Path("") {
			environment.metadataPath = defaultMetadataPath
		}
		if environment.address == address.Undef {
			environment.address = defaultProviderAddress
		}
		if environment.ask == storagemarket.StorageAskUndefined {
			environment.ask = defaultAsk
		}

		fsmCtx := fsmtest.NewTestContext(ctx, eventProcessor)
		err = stateEntryFunc(fsmCtx, environment, *dealState)
		require.NoError(t, err)
		fsmCtx.ReplayEvents(t, dealState)
		dealInspector(t, *dealState, environment)

		fs.VerifyExpectations(t)
		pieceStore.VerifyExpectations(t)
		environment.VerifyExpectations(t)
	}
}

type restartDataTransferCall struct {
	chId datatransfer.ChannelID
}

type fakeEnvironment struct {
	address                 address.Address
	node                    *testnodes.FakeProviderNode
	ask                     storagemarket.StorageAsk
	dataTransferError       error
	pieceCid                cid.Cid
	path                    filestore.Path
	metadataPath            filestore.Path
	generateCommPError      error
	sendSignedResponseError error
	disconnectCalls         int
	disconnectError         error
	rejectDeal              bool
	rejectReason            string
	decisionError           error
	deleteStoreError        error
	fs                      filestore.FileStore
	pieceStore              piecestore.PieceStore
	expectedTags            map[string]struct{}
	receivedTags            map[string]struct{}
	dealFunds               *tut.TestDealFunds
	peerTagger              *tut.TestPeerTagger

	restartDataTransferCalls []restartDataTransferCall
	restartDataTransferError error
}

func (fe *fakeEnvironment) RestartDataTransfer(_ context.Context, chId datatransfer.ChannelID) error {
	fe.restartDataTransferCalls = append(fe.restartDataTransferCalls, restartDataTransferCall{chId})
	return fe.restartDataTransferError
}

func (fe *fakeEnvironment) Address() address.Address {
	return fe.address
}

func (fe *fakeEnvironment) Node() storagemarket.StorageProviderNode {
	return fe.node
}

func (fe *fakeEnvironment) Ask() storagemarket.StorageAsk {
	return fe.ask
}

func (fe *fakeEnvironment) DeleteStore(storeID multistore.StoreID) error {
	return fe.deleteStoreError
}

func (fe *fakeEnvironment) GeneratePieceCommitmentToFile(storeID *multistore.StoreID, payloadCid cid.Cid, selector ipld.Node) (cid.Cid, filestore.Path, filestore.Path, error) {
	return fe.pieceCid, fe.path, fe.metadataPath, fe.generateCommPError
}

func (fe *fakeEnvironment) SendSignedResponse(ctx context.Context, response *network.Response) error {
	return fe.sendSignedResponseError
}

func (fe *fakeEnvironment) VerifyExpectations(t *testing.T) {
	require.Equal(t, fe.expectedTags, fe.receivedTags)
}

func (fe *fakeEnvironment) Disconnect(proposalCid cid.Cid) error {
	fe.disconnectCalls += 1
	return fe.disconnectError
}

func (fe *fakeEnvironment) FileStore() filestore.FileStore {
	return fe.fs
}

func (fe *fakeEnvironment) PieceStore() piecestore.PieceStore {
	return fe.pieceStore
}

func (fe *fakeEnvironment) RunCustomDecisionLogic(context.Context, storagemarket.MinerDeal) (bool, string, error) {
	return !fe.rejectDeal, fe.rejectReason, fe.decisionError
}

func (fe *fakeEnvironment) DealFunds() funds.DealFunds {
	return fe.dealFunds
}

func (fe *fakeEnvironment) TagPeer(id peer.ID, s string) {
	fe.peerTagger.TagPeer(id, s)
}

func (fe *fakeEnvironment) UntagPeer(id peer.ID, s string) {
	fe.peerTagger.UntagPeer(id, s)
}

var _ providerstates.ProviderDealEnvironment = &fakeEnvironment{}
