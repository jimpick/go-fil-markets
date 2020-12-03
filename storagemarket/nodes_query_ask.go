// +build clientqueryask

package storagemarket

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/go-fil-markets/shared"
)

// StorageCommon are common interfaces provided by a filecoin Node to both StorageClient and StorageProvider
type StorageCommon interface {

	// GetChainHead returns a tipset token for the current chain head
	GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error)

	// VerifySignature verifies a given set of data was signed properly by a given address's private key
	VerifySignature(ctx context.Context, signature crypto.Signature, signer address.Address, plaintext []byte, tok shared.TipSetToken) (bool, error)
}

// StorageClientNode are node dependencies for a StorageClient
type StorageClientNode interface {
	StorageCommon
}
