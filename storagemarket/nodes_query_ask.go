// +build clientqueryask

package storagemarket

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"

	"github.com/filecoin-project/go-fil-markets/shared"
)

// DealSectorCommittedCallback is a callback that runs when a sector is committed
type DealSectorCommittedCallback func(err error)

// DealExpiredCallback is a callback that runs when a deal expires
type DealExpiredCallback func(err error)

// DealSlashedCallback is a callback that runs when a deal gets slashed
type DealSlashedCallback func(slashEpoch abi.ChainEpoch, err error)

// StorageCommon are common interfaces provided by a filecoin Node to both StorageClient and StorageProvider
type StorageCommon interface {

	// GetChainHead returns a tipset token for the current chain head
	GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error)

	// VerifySignature verifies a given set of data was signed properly by a given address's private key
	VerifySignature(ctx context.Context, signature crypto.Signature, signer address.Address, plaintext []byte, tok shared.TipSetToken) (bool, error)
}

// PackingResult returns information about how a deal was put into a sector
type PackingResult struct {
	SectorNumber abi.SectorNumber
	Offset       abi.PaddedPieceSize
	Size         abi.PaddedPieceSize
}

// StorageProviderNode are node dependencies for a StorageProvider
type StorageProviderNode interface {
	StorageCommon

	// PublishDeals publishes a deal on chain, returns the message cid, but does not wait for message to appear
	PublishDeals(ctx context.Context, deal MinerDeal) (cid.Cid, error)

	// OnDealComplete is called when a deal is complete and on chain, and data has been transferred and is ready to be added to a sector
	OnDealComplete(ctx context.Context, deal MinerDeal, pieceSize abi.UnpaddedPieceSize, pieceReader io.Reader) (*PackingResult, error)

	// GetMinerWorkerAddress returns the worker address associated with a miner
	GetMinerWorkerAddress(ctx context.Context, addr address.Address, tok shared.TipSetToken) (address.Address, error)

	// LocatePieceForDealWithinSector looks up a given dealID in the miners sectors, and returns its sectorID and location
	LocatePieceForDealWithinSector(ctx context.Context, dealID abi.DealID, tok shared.TipSetToken) (sectorID abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize, err error)

	// GetDataCap gets the current data cap for addr
	GetDataCap(ctx context.Context, addr address.Address, tok shared.TipSetToken) (*verifreg.DataCap, error)
}

// StorageClientNode are node dependencies for a StorageClient
type StorageClientNode interface {
	StorageCommon
}
