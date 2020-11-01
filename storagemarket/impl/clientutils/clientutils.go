// Package clientutils provides utility functions for the storage client & client FSM
package clientutils

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
)

// VerifyFunc is a function that can validate a signature for a given address and bytes
type VerifyFunc func(context.Context, crypto.Signature, address.Address, []byte, shared.TipSetToken) (bool, error)

// VerifyResponse verifies the signature on the given signed response matches
// the given miner address, using the given signature verification function
func VerifyResponse(ctx context.Context, resp network.SignedResponse, minerAddr address.Address, tok shared.TipSetToken, verifier VerifyFunc) error {
	b, err := cborutil.Dump(&resp.Response)
	if err != nil {
		return err
	}
	verified, err := verifier(ctx, *resp.Signature, minerAddr, b, tok)
	if err != nil {
		return err
	}

	if !verified {
		return xerrors.New("could not verify signature")
	}

	return nil
}

// LabelField makes a label field for a deal proposal as a multibase encoding
// of the payload CID (B58BTC for V0, B64 for V1)
//
func LabelField(payloadCID cid.Cid) (string, error) {
	if payloadCID.Version() == 0 {
		return payloadCID.StringOfBase(multibase.Base58BTC)
	}
	return payloadCID.StringOfBase(multibase.Base64)
}
