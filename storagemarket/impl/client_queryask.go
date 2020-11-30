// +build clientqueryask

package storageimpl

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
)

var log = logging.Logger("storagemarket_impl")

var _ storagemarket.StorageClient = &Client{}

// Client is the production implementation of the StorageClient interface
type Client struct {
	net  network.StorageMarketNetwork
	node storagemarket.StorageClientNode
}

// StorageClientOption allows custom configuration of a storage client
type StorageClientOption func(c *Client)

// NewClient creates a new storage client
func NewClient(
	net network.StorageMarketNetwork,
	scn storagemarket.StorageClientNode,
	options ...StorageClientOption,
) (*Client, error) {
	c := &Client{
		net:  net,
		node: scn,
	}

	return c, nil
}

// GetAsk queries a provider for its current storage ask
//
// The client creates a new `StorageAskStream` for the chosen peer ID,
// and calls WriteAskRequest on it, which constructs a message and writes it to the Ask stream.
// When it receives a response, it verifies the signature and returns the validated
// StorageAsk if successful
func (c *Client) GetAsk(ctx context.Context, info storagemarket.StorageProviderInfo) (*storagemarket.StorageAsk, error) {
	fmt.Printf("Jim GetAsk info %v\n", info)
	fmt.Printf("Jim GetAsk addr %v\n", info.Addrs[0].String())
	if len(info.Addrs) > 0 {
		c.net.AddAddrs(info.PeerID, info.Addrs)
	}
	s, err := c.net.NewAskStream(ctx, info.PeerID)
	if err != nil {
		return nil, xerrors.Errorf("failed to open stream to miner: %w", err)
	}

	request := network.AskRequest{Miner: info.Address}
	if err := s.WriteAskRequest(request); err != nil {
		return nil, xerrors.Errorf("failed to send ask request: %w", err)
	}

	// out, origBytes, err := s.ReadAskResponse()
	out, _, err := s.ReadAskResponse()
	if err != nil {
		return nil, xerrors.Errorf("failed to read ask response: %w", err)
	}

	if out.Ask == nil {
		return nil, xerrors.Errorf("got no ask back")
	}

	if out.Ask.Ask.Miner != info.Address {
		return nil, xerrors.Errorf("got back ask for wrong miner")
	}

	/*
		tok, _, err := c.node.GetChainHead(ctx)
		if err != nil {
			return nil, err
		}

			isValid, err := c.node.VerifySignature(ctx, *out.Ask.Signature, info.Worker, origBytes, tok)
			if err != nil {
				return nil, err
			}

			if !isValid {
				return nil, xerrors.Errorf("ask was not properly signed")
			}
	*/

	return out.Ask.Ask, nil
}

// Configure applies the given list of StorageClientOptions after a StorageClient
// is initialized
func (c *Client) Configure(options ...StorageClientOption) {
	for _, option := range options {
		option(c)
	}
}

func (c *Client) start(ctx context.Context) error {
	return nil
}

// var _ clientstates.ClientDealEnvironment = &clientDealEnvironment{}
