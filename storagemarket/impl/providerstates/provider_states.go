package providerstates

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-multistore"
	padreader "github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"

	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/providerutils"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
)

var log = logging.Logger("providerstates")

// TODO: These are copied from spec-actors master, use spec-actors exports when we update
const DealMaxLabelSize = 256

// ProviderDealEnvironment are the dependencies needed for processing deals
// with a ProviderStateEntryFunc
type ProviderDealEnvironment interface {
	RestartDataTransfer(ctx context.Context, chID datatransfer.ChannelID) error
	Address() address.Address
	Node() storagemarket.StorageProviderNode
	Ask() storagemarket.StorageAsk
	DeleteStore(storeID multistore.StoreID) error
	GeneratePieceCommitment(storeID *multistore.StoreID, payloadCid cid.Cid, selector ipld.Node) (cid.Cid, filestore.Path, error)
	GeneratePieceReader(storeID *multistore.StoreID, payloadCid cid.Cid, selector ipld.Node) (io.ReadCloser, uint64, error, <-chan error)
	SendSignedResponse(ctx context.Context, response *network.Response) error
	Disconnect(proposalCid cid.Cid) error
	FileStore() filestore.FileStore
	PieceStore() piecestore.PieceStore
	RunCustomDecisionLogic(context.Context, storagemarket.MinerDeal) (bool, string, error)
	network.PeerTagger
}

// ProviderStateEntryFunc is the signature for a StateEntryFunc in the provider FSM
type ProviderStateEntryFunc func(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error

// ValidateDealProposal validates a proposed deal against the provider criteria
func ValidateDealProposal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	environment.TagPeer(deal.Client, deal.ProposalCid.String())

	tok, curEpoch, err := environment.Node().GetChainHead(ctx.Context())
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("node error getting most recent state id: %w", err))
	}

	if err := providerutils.VerifyProposal(ctx.Context(), deal.ClientDealProposal, tok, environment.Node().VerifySignature); err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("verifying StorageDealProposal: %w", err))
	}

	proposal := deal.Proposal

	if proposal.Provider != environment.Address() {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("incorrect provider for deal"))
	}

	if len(proposal.Label) > DealMaxLabelSize {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("deal label can be at most %d bytes, is %d", DealMaxLabelSize, len(proposal.Label)))
	}

	if err := proposal.PieceSize.Validate(); err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposal piece size is invalid: %w", err))
	}

	if !proposal.PieceCID.Defined() {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposal PieceCID undefined"))
	}

	if proposal.PieceCID.Prefix() != market.PieceCIDPrefix {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposal PieceCID had wrong prefix"))
	}

	if proposal.EndEpoch <= proposal.StartEpoch {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposal end before proposal start"))
	}

	if curEpoch > proposal.StartEpoch {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("deal start epoch has already elapsed"))
	}

	minDuration, maxDuration := market2.DealDurationBounds(proposal.PieceSize)
	if proposal.Duration() < minDuration || proposal.Duration() > maxDuration {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("deal duration out of bounds (min, max, provided): %d, %d, %d", minDuration, maxDuration, proposal.Duration()))
	}

	pcMin, pcMax, err := environment.Node().DealProviderCollateralBounds(ctx.Context(), proposal.PieceSize, proposal.VerifiedDeal)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("node error getting collateral bounds: %w", err))
	}

	if proposal.ProviderCollateral.LessThan(pcMin) {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposed provider collateral below minimum: %s < %s", proposal.ProviderCollateral, pcMin))
	}

	if proposal.ProviderCollateral.GreaterThan(pcMax) {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("proposed provider collateral above maximum: %s > %s", proposal.ProviderCollateral, pcMax))
	}

	askPrice := environment.Ask().Price
	if deal.Proposal.VerifiedDeal {
		askPrice = environment.Ask().VerifiedPrice
	}

	minPrice := big.Div(big.Mul(askPrice, abi.NewTokenAmount(int64(proposal.PieceSize))), abi.NewTokenAmount(1<<30))
	if proposal.StoragePricePerEpoch.LessThan(minPrice) {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected,
			xerrors.Errorf("storage price per epoch less than asking price: %s < %s", proposal.StoragePricePerEpoch, minPrice))
	}

	if proposal.PieceSize < environment.Ask().MinPieceSize {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected,
			xerrors.Errorf("piece size less than minimum required size: %d < %d", proposal.PieceSize, environment.Ask().MinPieceSize))
	}

	if proposal.PieceSize > environment.Ask().MaxPieceSize {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected,
			xerrors.Errorf("piece size more than maximum allowed size: %d > %d", proposal.PieceSize, environment.Ask().MaxPieceSize))
	}

	// check market funds
	clientMarketBalance, err := environment.Node().GetBalance(ctx.Context(), proposal.Client, tok)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("node error getting client market balance failed: %w", err))
	}

	// This doesn't guarantee that the client won't withdraw / lock those funds
	// but it's a decent first filter
	if clientMarketBalance.Available.LessThan(proposal.ClientBalanceRequirement()) {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("clientMarketBalance.Available too small: %d < %d", clientMarketBalance.Available, proposal.ClientBalanceRequirement()))
	}

	// Verified deal checks
	if proposal.VerifiedDeal {
		dataCap, err := environment.Node().GetDataCap(ctx.Context(), proposal.Client, tok)
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("node error fetching verified data cap: %w", err))
		}
		if dataCap == nil {
			return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("node error fetching verified data cap: data cap missing -- client not verified"))
		}
		pieceSize := big.NewIntUnsigned(uint64(proposal.PieceSize))
		if dataCap.LessThan(pieceSize) {
			return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("verified deal DataCap too small for proposed piece size"))
		}
	}

	return ctx.Trigger(storagemarket.ProviderEventDealDeciding)
}

// DecideOnProposal allows custom decision logic to run before accepting a deal, such as allowing a manual
// operator to decide whether or not to accept the deal
func DecideOnProposal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	accept, reason, err := environment.RunCustomDecisionLogic(ctx.Context(), deal)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, xerrors.Errorf("custom deal decision logic failed: %w", err))
	}

	if !accept {
		return ctx.Trigger(storagemarket.ProviderEventDealRejected, fmt.Errorf(reason))
	}

	// Send intent to accept
	err = environment.SendSignedResponse(ctx.Context(), &network.Response{
		State:    storagemarket.StorageDealWaitingForData,
		Proposal: deal.ProposalCid,
	})

	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventSendResponseFailed, err)
	}

	if err := environment.Disconnect(deal.ProposalCid); err != nil {
		log.Warnf("closing client connection: %+v", err)
	}

	return ctx.Trigger(storagemarket.ProviderEventDataRequested)
}

// VerifyData verifies that data received for a deal matches the pieceCID
// in the proposal
func VerifyData(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	pieceCid, metadataPath, err := environment.GeneratePieceCommitment(deal.StoreID, deal.Ref.Root, shared.AllSelector())
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDataVerificationFailed, xerrors.Errorf("error generating CommP: %w", err), filestore.Path(""), filestore.Path(""))
	}

	// Verify CommP matches
	if pieceCid != deal.Proposal.PieceCID {
		return ctx.Trigger(storagemarket.ProviderEventDataVerificationFailed, xerrors.Errorf("proposal CommP doesn't match calculated CommP"), filestore.Path(""), metadataPath)
	}

	return ctx.Trigger(storagemarket.ProviderEventVerifiedData, filestore.Path(""), metadataPath)
}

// ReserveProviderFunds adds funds, as needed to the StorageMarketActor, so the miner has adequate collateral for the deal
func ReserveProviderFunds(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	node := environment.Node()

	tok, _, err := node.GetChainHead(ctx.Context())
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("acquiring chain head: %w", err))
	}

	waddr, err := node.GetMinerWorkerAddress(ctx.Context(), deal.Proposal.Provider, tok)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("looking up miner worker: %w", err))
	}

	mcid, err := node.ReserveFunds(ctx.Context(), waddr, deal.Proposal.Provider, deal.Proposal.ProviderCollateral)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("reserving funds: %w", err))
	}

	_ = ctx.Trigger(storagemarket.ProviderEventFundsReserved, deal.Proposal.ProviderCollateral)

	// if no message was sent, and there was no error, funds were already available
	if mcid == cid.Undef {
		return ctx.Trigger(storagemarket.ProviderEventFunded)
	}

	return ctx.Trigger(storagemarket.ProviderEventFundingInitiated, mcid)
}

// WaitForFunding waits for a message posted to add funds to the StorageMarketActor to appear on chain
func WaitForFunding(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	node := environment.Node()

	return node.WaitForMessage(ctx.Context(), *deal.AddFundsCid, func(code exitcode.ExitCode, bytes []byte, finalCid cid.Cid, err error) error {
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("AddFunds errored: %w", err))
		}
		if code != exitcode.Ok {
			return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("AddFunds exit code: %s", code.String()))
		}
		return ctx.Trigger(storagemarket.ProviderEventFunded)
	})
}

// PublishDeal sends a message to publish a deal on chain
func PublishDeal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	smDeal := storagemarket.MinerDeal{
		Client:             deal.Client,
		ClientDealProposal: deal.ClientDealProposal,
		ProposalCid:        deal.ProposalCid,
		State:              deal.State,
		Ref:                deal.Ref,
	}

	mcid, err := environment.Node().PublishDeals(ctx.Context(), smDeal)
	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventNodeErrored, xerrors.Errorf("publishing deal: %w", err))
	}

	return ctx.Trigger(storagemarket.ProviderEventDealPublishInitiated, mcid)
}

// RestartDataTransfer restarts a data transfer that was earlier initiated by the client
func RestartDataTransfer(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	log.Infof("restarting data transfer for deal %s", deal.ProposalCid)

	if deal.TransferChannelId == nil {
		return ctx.Trigger(storagemarket.ProviderEventDataTransferRestartFailed, xerrors.New("channelId on provider deal is nil"))
	}

	// We need to do this in a goroutine as `environment.RestartDataTransfer` calls `GetSync` on the state machine under the hood
	// and we should NEVER call `GetSync` in the call stack for a state handler as it causes a deadlock.
	go func() {
		// restart the push data transfer. This will complete asynchronously and the
		// completion of the data transfer will trigger a change in deal state
		err := environment.RestartDataTransfer(ctx.Context(),
			*deal.TransferChannelId,
		)
		if err != nil {
			ctx.Trigger(storagemarket.ProviderEventDataTransferRestartFailed, err)
		}
	}()

	return nil
}

// WaitForPublish waits for the publish message on chain and sends the deal id back to the client
func WaitForPublish(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	return environment.Node().WaitForMessage(ctx.Context(), *deal.PublishCid, func(code exitcode.ExitCode, retBytes []byte, finalCid cid.Cid, err error) error {
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealPublishError, xerrors.Errorf("PublishStorageDeals errored: %w", err))
		}
		if code != exitcode.Ok {
			return ctx.Trigger(storagemarket.ProviderEventDealPublishError, xerrors.Errorf("PublishStorageDeals exit code: %s", code.String()))
		}
		var retval market.PublishStorageDealsReturn
		err = retval.UnmarshalCBOR(bytes.NewReader(retBytes))
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealPublishError, xerrors.Errorf("PublishStorageDeals error unmarshalling result: %w", err))
		}

		releaseReservedFunds(ctx, environment, deal)

		return ctx.Trigger(storagemarket.ProviderEventDealPublished, retval.IDs[0], finalCid)
	})
}

// HandoffDeal hands off a published deal for sealing and commitment in a sector
func HandoffDeal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	var packingInfo *storagemarket.PackingResult
	var packingErr error
	if deal.PiecePath != filestore.Path("") {
		file, err := environment.FileStore().Open(deal.PiecePath)
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventFileStoreErrored, xerrors.Errorf("reading piece at path %s: %w", deal.PiecePath, err))
		}
		packingInfo, packingErr = handoffDeal(ctx.Context(), environment, deal, file, uint64(file.Size()))
	} else {
		pieceReader, pieceSize, err, writeErrChan := environment.GeneratePieceReader(deal.StoreID, deal.Ref.Root, shared.AllSelector())
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, err)
		}
		packingInfo, packingErr = handoffDeal(ctx.Context(), environment, deal, pieceReader, pieceSize)
		err = pieceReader.Close()
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, err)
		}
		select {
		case <-ctx.Context().Done():
			return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, errors.New("write never finished"))
		case err = <-writeErrChan:
		}
		if err != nil {
			return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, err)
		}
	}

	if packingErr != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealHandoffFailed, packingErr)
	}

	if err := recordPiece(environment, deal, packingInfo.SectorNumber, packingInfo.Offset, packingInfo.Size); err != nil {
		log.Errorf("failed to register deal data for retrieval: %s", err)
		_ = ctx.Trigger(storagemarket.ProviderEventPieceStoreErrored, err)
	}

	return ctx.Trigger(storagemarket.ProviderEventDealHandedOff)
}

func handoffDeal(ctx context.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal, reader io.Reader, size uint64) (*storagemarket.PackingResult, error) {
	paddedReader, paddedSize := padreader.New(reader, size)
	return environment.Node().OnDealComplete(
		ctx,
		storagemarket.MinerDeal{
			Client:             deal.Client,
			ClientDealProposal: deal.ClientDealProposal,
			ProposalCid:        deal.ProposalCid,
			State:              deal.State,
			Ref:                deal.Ref,
			PublishCid:         deal.PublishCid,
			DealID:             deal.DealID,
			FastRetrieval:      deal.FastRetrieval,
		},
		paddedSize,
		paddedReader,
	)
}

func recordPiece(environment ProviderDealEnvironment, deal storagemarket.MinerDeal, sectorID abi.SectorNumber, offset, length abi.PaddedPieceSize) error {

	var blockLocations map[cid.Cid]piecestore.BlockLocation
	if deal.MetadataPath != filestore.Path("") {
		var err error
		blockLocations, err = providerutils.LoadBlockLocations(environment.FileStore(), deal.MetadataPath)
		if err != nil {
			return xerrors.Errorf("failed to load block locations: %w", err)
		}
	} else {
		blockLocations = map[cid.Cid]piecestore.BlockLocation{
			deal.Ref.Root: {},
		}
	}

	if err := environment.PieceStore().AddPieceBlockLocations(deal.Proposal.PieceCID, blockLocations); err != nil {
		return xerrors.Errorf("failed to add piece block locations: %s", err)
	}

	err := environment.PieceStore().AddDealForPiece(deal.Proposal.PieceCID, piecestore.DealInfo{
		DealID:   deal.DealID,
		SectorID: sectorID,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return xerrors.Errorf("failed to add deal for piece: %s", err)
	}

	return nil
}

// CleanupDeal clears the filestore once we know the mining component has read the data and it is in a sealed sector
func CleanupDeal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	if deal.PiecePath != "" {
		err := environment.FileStore().Delete(deal.PiecePath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.PiecePath, err)
		}
	}
	if deal.MetadataPath != "" {
		err := environment.FileStore().Delete(deal.MetadataPath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.MetadataPath, err)
		}
	}
	if deal.StoreID != nil {
		err := environment.DeleteStore(*deal.StoreID)
		if err != nil {
			log.Warnf("deleting store %d: %w", deal.StoreID, err)
		}
	}

	return ctx.Trigger(storagemarket.ProviderEventFinalized)
}

// VerifyDealPreCommitted verifies that a deal has been pre-committed
func VerifyDealPreCommitted(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	cb := func(sectorNumber abi.SectorNumber, isActive bool, err error) {
		// It's possible that
		// - we miss the pre-commit message and have to wait for prove-commit
		// - the deal is already active (for example if the node is restarted
		//   while waiting for pre-commit)
		// In either of these two cases, isActive will be true.
		switch {
		case err != nil:
			_ = ctx.Trigger(storagemarket.ProviderEventDealPrecommitFailed, err)
		case isActive:
			_ = ctx.Trigger(storagemarket.ProviderEventDealActivated)
		default:
			_ = ctx.Trigger(storagemarket.ProviderEventDealPrecommitted, sectorNumber)
		}
	}

	err := environment.Node().OnDealSectorPreCommitted(ctx.Context(), deal.Proposal.Provider, deal.DealID, deal.Proposal, deal.PublishCid, cb)

	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealPrecommitFailed, err)
	}
	return nil
}

// VerifyDealActivated verifies that a deal has been committed to a sector and activated
func VerifyDealActivated(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	// TODO: consider waiting for seal to happen
	cb := func(err error) {
		if err != nil {
			_ = ctx.Trigger(storagemarket.ProviderEventDealActivationFailed, err)
		} else {
			_ = ctx.Trigger(storagemarket.ProviderEventDealActivated)
		}
	}

	err := environment.Node().OnDealSectorCommitted(ctx.Context(), deal.Proposal.Provider, deal.DealID, deal.SectorNumber, deal.Proposal, deal.PublishCid, cb)

	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealActivationFailed, err)
	}
	return nil
}

// WaitForDealCompletion waits for the deal to be slashed or to expire
func WaitForDealCompletion(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	// At this point we have all the data so we can unprotect the connection
	environment.UntagPeer(deal.Client, deal.ProposalCid.String())

	node := environment.Node()

	// Called when the deal expires
	expiredCb := func(err error) {
		if err != nil {
			_ = ctx.Trigger(storagemarket.ProviderEventDealCompletionFailed, xerrors.Errorf("deal expiration err: %w", err))
		} else {
			_ = ctx.Trigger(storagemarket.ProviderEventDealExpired)
		}
	}

	// Called when the deal is slashed
	slashedCb := func(slashEpoch abi.ChainEpoch, err error) {
		if err != nil {
			_ = ctx.Trigger(storagemarket.ProviderEventDealCompletionFailed, xerrors.Errorf("deal slashing err: %w", err))
		} else {
			_ = ctx.Trigger(storagemarket.ProviderEventDealSlashed, slashEpoch)
		}
	}

	if err := node.OnDealExpiredOrSlashed(ctx.Context(), deal.DealID, expiredCb, slashedCb); err != nil {
		return ctx.Trigger(storagemarket.ProviderEventDealCompletionFailed, err)
	}

	return nil
}

// RejectDeal sends a failure response before terminating a deal
func RejectDeal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	err := environment.SendSignedResponse(ctx.Context(), &network.Response{
		State:    storagemarket.StorageDealFailing,
		Message:  deal.Message,
		Proposal: deal.ProposalCid,
	})

	if err != nil {
		return ctx.Trigger(storagemarket.ProviderEventSendResponseFailed, err)
	}

	if err := environment.Disconnect(deal.ProposalCid); err != nil {
		log.Warnf("closing client connection: %+v", err)
	}

	return ctx.Trigger(storagemarket.ProviderEventRejectionSent)
}

// FailDeal cleans up before terminating a deal
func FailDeal(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) error {
	log.Warnf("deal %s failed: %s", deal.ProposalCid, deal.Message)

	environment.UntagPeer(deal.Client, deal.ProposalCid.String())

	if deal.PiecePath != filestore.Path("") {
		err := environment.FileStore().Delete(deal.PiecePath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.PiecePath, err)
		}
	}
	if deal.MetadataPath != filestore.Path("") {
		err := environment.FileStore().Delete(deal.MetadataPath)
		if err != nil {
			log.Warnf("deleting piece at path %s: %w", deal.MetadataPath, err)
		}
	}
	if deal.StoreID != nil {
		err := environment.DeleteStore(*deal.StoreID)
		if err != nil {
			log.Warnf("deleting store id %d: %w", *deal.StoreID, err)
		}
	}
	releaseReservedFunds(ctx, environment, deal)

	return ctx.Trigger(storagemarket.ProviderEventFailed)
}

func releaseReservedFunds(ctx fsm.Context, environment ProviderDealEnvironment, deal storagemarket.MinerDeal) {
	if !deal.FundsReserved.Nil() && !deal.FundsReserved.IsZero() {
		err := environment.Node().ReleaseFunds(ctx.Context(), deal.Proposal.Provider, deal.FundsReserved)
		if err != nil {
			// nonfatal error
			log.Warnf("failed to release funds: %s", err)
		}
		_ = ctx.Trigger(storagemarket.ProviderEventFundsReleased, deal.FundsReserved)
	}
}
