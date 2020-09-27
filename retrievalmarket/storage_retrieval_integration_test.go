package retrievalmarket_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-multistore"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/paych"

	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/discovery"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	testnodes2 "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/testnodes"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/shared_testutil"
	tut "github.com/filecoin-project/go-fil-markets/shared_testutil"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	stormkt "github.com/filecoin-project/go-fil-markets/storagemarket/impl"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/funds"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/storedask"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	stornet "github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-fil-markets/storagemarket/testnodes"
)

func TestStorageRetrieval(t *testing.T) {
	bgCtx := context.Background()
	sh := newStorageHarness(bgCtx, t)
	require.NoError(t, sh.Client.Start(bgCtx))
	require.NoError(t, sh.Provider.Start(bgCtx))

	// set up a subscriber
	providerDealChan := make(chan storagemarket.MinerDeal)
	subscriber := func(event storagemarket.ProviderEvent, deal storagemarket.MinerDeal) {
		providerDealChan <- deal
	}
	_ = sh.Provider.SubscribeToEvents(subscriber)

	clientDealChan := make(chan storagemarket.ClientDeal)
	clientSubscriber := func(event storagemarket.ClientEvent, deal storagemarket.ClientDeal) {
		clientDealChan <- deal
	}
	_ = sh.Client.SubscribeToEvents(clientSubscriber)

	// set ask price where we'll accept any price
	err := sh.Provider.SetAsk(big.NewInt(0), big.NewInt(0), 50_000)
	assert.NoError(t, err)

	result := sh.ProposeStorageDeal(t, &storagemarket.DataRef{TransferType: storagemarket.TTGraphsync, Root: sh.PayloadCid})
	require.False(t, result.ProposalCid.Equals(cid.Undef))

	time.Sleep(time.Millisecond * 200)

	ctxTimeout, canc := context.WithTimeout(bgCtx, 25*time.Second)
	defer canc()

	var storageProviderSeenDeal storagemarket.MinerDeal
	var storageClientSeenDeal storagemarket.ClientDeal
	for storageProviderSeenDeal.State != storagemarket.StorageDealExpired ||
		storageClientSeenDeal.State != storagemarket.StorageDealExpired {
		select {
		case storageProviderSeenDeal = <-providerDealChan:
		case storageClientSeenDeal = <-clientDealChan:
		case <-ctxTimeout.Done():
			t.Fatalf("never saw completed deal, client deal state: %s (%d), provider deal state: %s (%d)",
				storagemarket.DealStates[storageClientSeenDeal.State],
				storageClientSeenDeal.State,
				storagemarket.DealStates[storageProviderSeenDeal.State],
				storageProviderSeenDeal.State,
			)
		}
	}

	rh := newRetrievalHarness(ctxTimeout, t, sh, storageClientSeenDeal)

	clientDealStateChan := make(chan retrievalmarket.ClientDealState)
	rh.Client.SubscribeToEvents(func(event retrievalmarket.ClientEvent, state retrievalmarket.ClientDealState) {
		switch event {
		case retrievalmarket.ClientEventComplete:
			clientDealStateChan <- state
		default:
			msg := `
			Client:
			Event:           %s
			Status:          %s
			TotalReceived:   %d
			BytesPaidFor:    %d
			CurrentInterval: %d
			TotalFunds:      %s
			Message:         %s
			`
			t.Logf(msg, retrievalmarket.ClientEvents[event], retrievalmarket.DealStatuses[state.Status], state.TotalReceived, state.BytesPaidFor, state.CurrentInterval,
				state.TotalFunds.String(), state.Message)
		}
	})

	providerDealStateChan := make(chan retrievalmarket.ProviderDealState)
	rh.Provider.SubscribeToEvents(func(event retrievalmarket.ProviderEvent, state retrievalmarket.ProviderDealState) {
		switch event {
		case retrievalmarket.ProviderEventCleanupComplete:
			providerDealStateChan <- state
		default:
			msg := `
			Provider:
			Event:           %s
			Status:          %s
			TotalSent:       %d
			FundsReceived:   %s
			Message:		 %s
			CurrentInterval: %d
			`
			t.Logf(msg, retrievalmarket.ProviderEvents[event], retrievalmarket.DealStatuses[state.Status], state.TotalSent, state.FundsReceived.String(), state.Message,
				state.CurrentInterval)
		}
	})

	// **** Send the query for the Piece
	// set up retrieval params
	peers := rh.Client.FindProviders(sh.PayloadCid)
	require.Len(t, peers, 1)
	retrievalPeer := peers[0]
	require.NotNil(t, retrievalPeer.PieceCID)

	rh.ClientNode.ExpectKnownAddresses(retrievalPeer, nil)

	resp, err := rh.Client.Query(bgCtx, retrievalPeer, sh.PayloadCid, retrievalmarket.QueryParams{})
	require.NoError(t, err)
	require.Equal(t, retrievalmarket.QueryResponseAvailable, resp.Status)

	// testing V1 only
	rmParams, err := retrievalmarket.NewParamsV1(rh.RetrievalParams.PricePerByte, rh.RetrievalParams.PaymentInterval, rh.RetrievalParams.PaymentIntervalIncrease, shared.AllSelector(), nil, big.Zero())
	require.NoError(t, err)

	voucherAmts := []abi.TokenAmount{abi.NewTokenAmount(10136000), abi.NewTokenAmount(9784000)}
	proof := []byte("")
	for _, voucherAmt := range voucherAmts {
		require.NoError(t, rh.ProviderNode.ExpectVoucher(*rh.ExpPaych, rh.ExpVoucher, proof, voucherAmt, voucherAmt, nil))
	}
	// just make sure there is enough to cover the transfer
	fsize := 19000 // this is the known file size of the test file lorem.txt
	expectedTotal := big.Mul(rh.RetrievalParams.PricePerByte, abi.NewTokenAmount(int64(fsize*2)))

	// *** Retrieve the piece

	clientStoreID := sh.TestData.MultiStore1.Next()
	did, err := rh.Client.Retrieve(bgCtx, sh.PayloadCid, rmParams, expectedTotal, retrievalPeer, *rh.ExpPaych, retrievalPeer.Address, &clientStoreID)
	assert.Equal(t, did, retrievalmarket.DealID(0))
	require.NoError(t, err)

	ctxTimeout, cancel := context.WithTimeout(bgCtx, 10*time.Second)
	defer cancel()

	// verify that client subscribers will be notified of state changes
	var clientDealState retrievalmarket.ClientDealState
	select {
	case <-ctxTimeout.Done():
		t.Error("deal never completed")
		t.FailNow()
	case clientDealState = <-clientDealStateChan:
	}

	ctxTimeout, cancel = context.WithTimeout(bgCtx, 5*time.Second)
	defer cancel()
	var providerDealState retrievalmarket.ProviderDealState
	select {
	case <-ctxTimeout.Done():
		t.Error("provider never saw completed deal")
		t.FailNow()
	case providerDealState = <-providerDealStateChan:
	}

	require.Equal(t, retrievalmarket.DealStatusCompleted, providerDealState.Status)
	require.Equal(t, retrievalmarket.DealStatusCompleted, clientDealState.Status)

	rh.ClientNode.VerifyExpectations(t)
	sh.TestData.VerifyFileTransferredIntoStore(t, sh.PieceLink, clientStoreID, false, uint64(fsize))

}

type storageHarness struct {
	Ctx          context.Context
	Epoch        abi.ChainEpoch
	PieceLink    ipld.Link
	PayloadCid   cid.Cid
	StoreID      multistore.StoreID
	ProviderAddr address.Address
	DTClient     datatransfer.Manager
	Client       storagemarket.StorageClient
	ClientNode   *testnodes.FakeClientNode
	DTProvider   datatransfer.Manager
	Provider     storagemarket.StorageProvider
	ProviderNode *testnodes.FakeProviderNode
	ProviderInfo storagemarket.StorageProviderInfo
	TestData     *shared_testutil.Libp2pTestData
	PieceStore   piecestore.PieceStore
	PeerResolver retrievalmarket.PeerResolver
}

func newStorageHarness(ctx context.Context, t *testing.T) *storageHarness {
	epoch := abi.ChainEpoch(100)
	td := shared_testutil.NewLibp2pTestData(ctx, t)
	fpath := filepath.Join("retrievalmarket", "impl", "fixtures", "lorem.txt")
	rootLink, storeID := td.LoadUnixFSFileToStore(t, fpath, false)
	payloadCid := rootLink.(cidlink.Link).Cid
	clientAddr := address.TestAddress
	providerAddr := address.TestAddress2

	smState := testnodes.NewStorageMarketState()
	clientNode := testnodes.FakeClientNode{
		FakeCommonNode: testnodes.FakeCommonNode{SMState: smState},
		ClientAddr:     clientAddr,
		MinerAddr:      providerAddr,
		WorkerAddr:     providerAddr,
	}

	expDealID := abi.DealID(rand.Uint64())
	psdReturn := market.PublishStorageDealsReturn{IDs: []abi.DealID{expDealID}}
	psdReturnBytes := bytes.NewBuffer([]byte{})
	require.NoError(t, psdReturn.MarshalCBOR(psdReturnBytes))

	tempPath, err := ioutil.TempDir("", "storagemarket_test")
	require.NoError(t, err)
	ps, err := piecestoreimpl.NewPieceStore(td.Ds2)
	tut.StartAndWaitForReady(ctx, t, ps)
	providerNode := &testnodes.FakeProviderNode{
		FakeCommonNode: testnodes.FakeCommonNode{
			SMState:                smState,
			WaitForMessageRetBytes: psdReturnBytes.Bytes(),
		},
		MinerAddr: providerAddr,
	}
	fs, err := filestore.NewLocalFileStore(filestore.OsPath(tempPath))
	require.NoError(t, err)

	// create provider and client
	dtTransport1 := dtgstransport.NewTransport(td.Host1.ID(), td.GraphSync1)
	dt1, err := dtimpl.NewDataTransfer(td.DTStore1, td.DTNet1, dtTransport1, td.DTStoredCounter1)
	require.NoError(t, err)
	err = dt1.Start(ctx)
	require.NoError(t, err)

	peerResolver := discovery.NewLocal(td.Ds1)

	clientDealFunds, err := funds.NewDealFunds(td.Ds1, datastore.NewKey("storage/client/dealfunds"))
	require.NoError(t, err)

	client, err := stormkt.NewClient(
		stornet.NewFromLibp2pHost(td.Host1, stornet.RetryParameters(0, 0, 0)),
		td.Bs1,
		td.MultiStore1,
		dt1,
		peerResolver,
		td.Ds1,
		&clientNode,
		clientDealFunds,
		stormkt.DealPollingInterval(0),
	)
	require.NoError(t, err)

	dtTransport2 := dtgstransport.NewTransport(td.Host2.ID(), td.GraphSync2)
	dt2, err := dtimpl.NewDataTransfer(td.DTStore2, td.DTNet2, dtTransport2, td.DTStoredCounter2)
	require.NoError(t, err)
	err = dt2.Start(ctx)
	require.NoError(t, err)
	storedAsk, err := storedask.NewStoredAsk(td.Ds2, datastore.NewKey("latest-ask"), providerNode, providerAddr)
	require.NoError(t, err)
	providerDealFunds, err := funds.NewDealFunds(td.Ds1, datastore.NewKey("storage/provider/dealfunds"))
	require.NoError(t, err)

	provider, err := stormkt.NewProvider(
		stornet.NewFromLibp2pHost(td.Host2, network.RetryParameters(0, 0, 0)),
		td.Ds2,
		fs,
		td.MultiStore2,
		ps,
		dt2,
		providerNode,
		providerAddr,
		abi.RegisteredSealProof_StackedDrg2KiBV1,
		storedAsk,
		providerDealFunds,
	)
	require.NoError(t, err)

	// set ask price where we'll accept any price
	require.NoError(t, provider.SetAsk(big.NewInt(0), big.NewInt(0), 50_000))
	require.NoError(t, provider.Start(ctx))

	// Closely follows the MinerInfo struct in the spec
	providerInfo := storagemarket.StorageProviderInfo{
		Address:    providerAddr,
		Owner:      providerAddr,
		Worker:     providerAddr,
		SectorSize: 1 << 20,
		PeerID:     td.Host2.ID(),
	}

	smState.Providers = map[address.Address]*storagemarket.StorageProviderInfo{providerAddr: &providerInfo}
	return &storageHarness{
		Ctx:          ctx,
		Epoch:        epoch,
		PayloadCid:   payloadCid,
		StoreID:      storeID,
		ProviderAddr: providerAddr,
		DTClient:     dt1,
		Client:       client,
		ClientNode:   &clientNode,
		PieceLink:    rootLink,
		PieceStore:   ps,
		DTProvider:   dt2,
		Provider:     provider,
		ProviderNode: providerNode,
		ProviderInfo: providerInfo,
		TestData:     td,
		PeerResolver: peerResolver,
	}
}

func (sh *storageHarness) ProposeStorageDeal(t *testing.T, dataRef *storagemarket.DataRef) *storagemarket.ProposeStorageDealResult {
	var dealDuration = abi.ChainEpoch(180 * builtin.EpochsInDay)

	result, err := sh.Client.ProposeStorageDeal(sh.Ctx, storagemarket.ProposeStorageDealParams{
		Addr:          sh.ProviderAddr,
		Info:          &sh.ProviderInfo,
		Data:          dataRef,
		StartEpoch:    sh.Epoch + 100,
		EndEpoch:      sh.Epoch + 100 + dealDuration,
		Price:         big.NewInt(1),
		Collateral:    big.NewInt(0),
		Rt:            abi.RegisteredSealProof_StackedDrg2KiBV1,
		FastRetrieval: false,
		VerifiedDeal:  false,
		StoreID:       &sh.StoreID,
	})
	assert.NoError(t, err)
	return result
}

var _ datatransfer.RequestValidator = (*fakeDTValidator)(nil)

type retrievalHarness struct {
	Ctx                         context.Context
	Epoch                       abi.ChainEpoch
	Client                      retrievalmarket.RetrievalClient
	ClientNode                  *testnodes2.TestRetrievalClientNode
	Provider                    retrievalmarket.RetrievalProvider
	ProviderNode                *testnodes2.TestRetrievalProviderNode
	PieceStore                  piecestore.PieceStore
	ExpPaych, NewLaneAddr       *address.Address
	ExpPaychAmt, ActualPaychAmt *abi.TokenAmount
	ExpVoucher, ActualVoucher   *paych.SignedVoucher
	RetrievalParams             retrievalmarket.Params
}

func newRetrievalHarness(ctx context.Context, t *testing.T, sh *storageHarness, deal storagemarket.ClientDeal) *retrievalHarness {

	var newPaychAmt abi.TokenAmount
	paymentChannelRecorder := func(client, miner address.Address, amt abi.TokenAmount) {
		newPaychAmt = amt
	}

	var newLaneAddr address.Address
	laneRecorder := func(paymentChannel address.Address) {
		newLaneAddr = paymentChannel
	}

	var newVoucher paych.SignedVoucher
	paymentVoucherRecorder := func(v *paych.SignedVoucher) {
		newVoucher = *v
	}

	cids := tut.GenerateCids(2)
	clientPaymentChannel, err := address.NewActorAddress([]byte("a"))

	expectedVoucher := tut.MakeTestSignedVoucher()
	require.NoError(t, err)
	clientNode := testnodes2.NewTestRetrievalClientNode(testnodes2.TestRetrievalClientNodeParams{
		Lane:                   expectedVoucher.Lane,
		PayCh:                  clientPaymentChannel,
		Voucher:                expectedVoucher,
		PaymentChannelRecorder: paymentChannelRecorder,
		AllocateLaneRecorder:   laneRecorder,
		PaymentVoucherRecorder: paymentVoucherRecorder,
		CreatePaychCID:         cids[0],
		AddFundsCID:            cids[1],
		IntegrationTest:        true,
	})

	nw1 := rmnet.NewFromLibp2pHost(sh.TestData.Host1, rmnet.RetryParameters(0, 0, 0))
	clientDs := namespace.Wrap(sh.TestData.Ds1, datastore.NewKey("/retrievals/client"))
	client, err := retrievalimpl.NewClient(nw1, sh.TestData.MultiStore1, sh.DTClient, clientNode, sh.PeerResolver, clientDs, sh.TestData.RetrievalStoredCounter1)
	require.NoError(t, err)
	tut.StartAndWaitForReady(ctx, t, client)
	payloadCID := deal.DataRef.Root
	providerPaymentAddr := deal.MinerWorker
	providerNode := testnodes2.NewTestRetrievalProviderNode()

	carData := sh.ProviderNode.LastOnDealCompleteBytes
	sectorID := abi.SectorNumber(100000)
	offset := abi.PaddedPieceSize(1000)
	pieceInfo := piecestore.PieceInfo{
		PieceCID: tut.GenerateCids(1)[0],
		Deals: []piecestore.DealInfo{
			{
				SectorID: sectorID,
				Offset:   offset,
				Length:   abi.UnpaddedPieceSize(uint64(len(carData))).Padded(),
			},
		},
	}
	providerNode.ExpectUnseal(sectorID, offset.Unpadded(), abi.UnpaddedPieceSize(uint64(len(carData))), carData)
	// clear out provider blockstore
	allCids, err := sh.TestData.Bs2.AllKeysChan(sh.Ctx)
	require.NoError(t, err)
	for c := range allCids {
		err = sh.TestData.Bs2.DeleteBlock(c)
		require.NoError(t, err)
	}

	nw2 := rmnet.NewFromLibp2pHost(sh.TestData.Host2, rmnet.RetryParameters(0, 0, 0))
	pieceStore := tut.NewTestPieceStore()
	expectedPiece := tut.GenerateCids(1)[0]
	cidInfo := piecestore.CIDInfo{
		PieceBlockLocations: []piecestore.PieceBlockLocation{
			{
				PieceCID: expectedPiece,
			},
		},
	}
	pieceStore.ExpectCID(payloadCID, cidInfo)
	pieceStore.ExpectPiece(expectedPiece, pieceInfo)
	providerDs := namespace.Wrap(sh.TestData.Ds2, datastore.NewKey("/retrievals/provider"))
	provider, err := retrievalimpl.NewProvider(providerPaymentAddr, providerNode, nw2, pieceStore, sh.TestData.MultiStore2, sh.DTProvider, providerDs)
	require.NoError(t, err)
	tut.StartAndWaitForReady(ctx, t, provider)

	params := retrievalmarket.Params{
		PricePerByte:            abi.NewTokenAmount(1000),
		PaymentInterval:         uint64(10000),
		PaymentIntervalIncrease: uint64(1000),
		UnsealPrice:             big.Zero(),
	}

	ask := provider.GetAsk()
	ask.PaymentInterval = params.PaymentInterval
	ask.PaymentIntervalIncrease = params.PaymentIntervalIncrease
	ask.PricePerByte = params.PricePerByte
	provider.SetAsk(ask)

	return &retrievalHarness{
		Ctx:             ctx,
		Client:          client,
		ClientNode:      clientNode,
		Epoch:           sh.Epoch,
		ExpPaych:        &clientPaymentChannel,
		NewLaneAddr:     &newLaneAddr,
		ActualPaychAmt:  &newPaychAmt,
		ExpVoucher:      expectedVoucher,
		ActualVoucher:   &newVoucher,
		Provider:        provider,
		ProviderNode:    providerNode,
		PieceStore:      sh.PieceStore,
		RetrievalParams: params,
	}
}

type fakeDTValidator struct{}

func (v *fakeDTValidator) ValidatePush(sender peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	return nil, nil
}

func (v *fakeDTValidator) ValidatePull(receiver peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	return nil, nil
}
