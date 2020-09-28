package clientstates

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-statemachine/fsm"

	rm "github.com/filecoin-project/go-fil-markets/retrievalmarket"
)

func recordReceived(deal *rm.ClientDealState, totalReceived uint64) error {
	deal.TotalReceived = totalReceived
	return nil
}

var paymentChannelCreationStates = []fsm.StateKey{
	rm.DealStatusWaitForAcceptance,
	rm.DealStatusWaitForAcceptanceLegacy,
	rm.DealStatusAccepted,
	rm.DealStatusPaymentChannelCreating,
	rm.DealStatusPaymentChannelAllocatingLane,
}

// ClientEvents are the events that can happen in a retrieval client
var ClientEvents = fsm.Events{
	fsm.Event(rm.ClientEventOpen).
		From(rm.DealStatusNew).ToNoChange(),

	// ProposeDeal handler events
	fsm.Event(rm.ClientEventWriteDealProposalErrored).
		FromAny().To(rm.DealStatusErrored).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = xerrors.Errorf("proposing deal: %w", err).Error()
			return nil
		}),
	fsm.Event(rm.ClientEventDealProposed).
		From(rm.DealStatusNew).To(rm.DealStatusWaitForAcceptance).
		From(rm.DealStatusRetryLegacy).To(rm.DealStatusWaitForAcceptanceLegacy).
		Action(func(deal *rm.ClientDealState, channelID datatransfer.ChannelID) error {
			deal.ChannelID = channelID
			deal.Message = ""
			return nil
		}),

	// Initial deal acceptance events
	fsm.Event(rm.ClientEventDealRejected).
		From(rm.DealStatusWaitForAcceptance).To(rm.DealStatusRetryLegacy).
		From(rm.DealStatusWaitForAcceptanceLegacy).To(rm.DealStatusRejected).
		Action(func(deal *rm.ClientDealState, message string) error {
			deal.Message = fmt.Sprintf("deal rejected: %s", message)
			deal.LegacyProtocol = true
			return nil
		}),
	fsm.Event(rm.ClientEventDealNotFound).
		FromMany(rm.DealStatusWaitForAcceptance, rm.DealStatusWaitForAcceptanceLegacy).To(rm.DealStatusDealNotFound).
		Action(func(deal *rm.ClientDealState, message string) error {
			deal.Message = fmt.Sprintf("deal not found: %s", message)
			return nil
		}),
	fsm.Event(rm.ClientEventDealAccepted).
		FromMany(rm.DealStatusWaitForAcceptance, rm.DealStatusWaitForAcceptanceLegacy).To(rm.DealStatusAccepted),
	fsm.Event(rm.ClientEventUnknownResponseReceived).
		FromAny().To(rm.DealStatusFailing).
		Action(func(deal *rm.ClientDealState, status rm.DealStatus) error {
			deal.Message = fmt.Sprintf("Unexpected deal response status: %s", rm.DealStatuses[status])
			return nil
		}),

	// Payment channel setup
	fsm.Event(rm.ClientEventPaymentChannelErrored).
		FromMany(rm.DealStatusAccepted, rm.DealStatusPaymentChannelCreating, rm.DealStatusPaymentChannelAddingFunds).To(rm.DealStatusFailing).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = xerrors.Errorf("error from payment channel: %w", err).Error()
			return nil
		}),
	fsm.Event(rm.ClientEventPaymentChannelCreateInitiated).
		From(rm.DealStatusAccepted).To(rm.DealStatusPaymentChannelCreating).
		Action(func(deal *rm.ClientDealState, msgCID cid.Cid) error {
			deal.WaitMsgCID = &msgCID
			return nil
		}),
	fsm.Event(rm.ClientEventPaymentChannelAddingFunds).
		FromMany(rm.DealStatusAccepted).To(rm.DealStatusPaymentChannelAllocatingLane).
		FromMany(rm.DealStatusCheckFunds).To(rm.DealStatusPaymentChannelAddingFunds).
		Action(func(deal *rm.ClientDealState, msgCID cid.Cid, payCh address.Address) error {
			deal.WaitMsgCID = &msgCID
			if deal.PaymentInfo == nil {
				deal.PaymentInfo = &rm.PaymentInfo{
					PayCh: payCh,
				}
			}
			return nil
		}),
	fsm.Event(rm.ClientEventPaymentChannelReady).
		From(rm.DealStatusPaymentChannelCreating).To(rm.DealStatusPaymentChannelAllocatingLane).
		From(rm.DealStatusPaymentChannelAddingFunds).To(rm.DealStatusOngoing).
		From(rm.DealStatusCheckFunds).To(rm.DealStatusOngoing).
		Action(func(deal *rm.ClientDealState, payCh address.Address) error {
			if deal.PaymentInfo == nil {
				deal.PaymentInfo = &rm.PaymentInfo{
					PayCh: payCh,
				}
			}
			deal.WaitMsgCID = nil
			// remove any insufficient funds message
			deal.Message = ""
			return nil
		}),
	fsm.Event(rm.ClientEventAllocateLaneErrored).
		FromMany(rm.DealStatusPaymentChannelAllocatingLane).
		To(rm.DealStatusFailing).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = xerrors.Errorf("allocating payment lane: %w", err).Error()
			return nil
		}),

	fsm.Event(rm.ClientEventLaneAllocated).
		From(rm.DealStatusPaymentChannelAllocatingLane).To(rm.DealStatusOngoing).
		Action(func(deal *rm.ClientDealState, lane uint64) error {
			deal.PaymentInfo.Lane = lane
			return nil
		}),

	// Transfer Channel Errors
	fsm.Event(rm.ClientEventDataTransferError).
		FromAny().To(rm.DealStatusErrored).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = fmt.Sprintf("error generated by data transfer: %s", err.Error())
			return nil
		}),

	// Receiving requests for payment
	fsm.Event(rm.ClientEventLastPaymentRequested).
		FromMany(
			rm.DealStatusOngoing,
			rm.DealStatusFundsNeededLastPayment,
			rm.DealStatusFundsNeeded).To(rm.DealStatusFundsNeededLastPayment).
		From(rm.DealStatusBlocksComplete).To(rm.DealStatusSendFundsLastPayment).
		FromMany(
			paymentChannelCreationStates...).ToJustRecord().
		Action(func(deal *rm.ClientDealState, paymentOwed abi.TokenAmount) error {
			deal.PaymentRequested = big.Add(deal.PaymentRequested, paymentOwed)
			deal.LastPaymentRequested = true
			return nil
		}),
	fsm.Event(rm.ClientEventPaymentRequested).
		FromMany(
			rm.DealStatusOngoing,
			rm.DealStatusBlocksComplete,
			rm.DealStatusFundsNeeded).To(rm.DealStatusFundsNeeded).
		FromMany(
			paymentChannelCreationStates...).ToJustRecord().
		Action(func(deal *rm.ClientDealState, paymentOwed abi.TokenAmount) error {
			deal.PaymentRequested = big.Add(deal.PaymentRequested, paymentOwed)
			return nil
		}),

	fsm.Event(rm.ClientEventUnsealPaymentRequested).
		FromMany(rm.DealStatusWaitForAcceptance, rm.DealStatusWaitForAcceptanceLegacy).To(rm.DealStatusAccepted).
		Action(func(deal *rm.ClientDealState, paymentOwed abi.TokenAmount) error {
			deal.PaymentRequested = big.Add(deal.PaymentRequested, paymentOwed)
			return nil
		}),

	// Receiving data
	fsm.Event(rm.ClientEventAllBlocksReceived).
		FromMany(
			rm.DealStatusOngoing,
			rm.DealStatusBlocksComplete,
		).To(rm.DealStatusBlocksComplete).
		FromMany(paymentChannelCreationStates...).ToJustRecord().
		FromMany(rm.DealStatusSendFunds, rm.DealStatusFundsNeeded).ToJustRecord().
		From(rm.DealStatusFundsNeededLastPayment).To(rm.DealStatusSendFundsLastPayment).
		Action(func(deal *rm.ClientDealState) error {
			deal.AllBlocksReceived = true
			return nil
		}),
	fsm.Event(rm.ClientEventBlocksReceived).
		FromMany(rm.DealStatusOngoing,
			rm.DealStatusFundsNeeded,
			rm.DealStatusFundsNeededLastPayment).ToNoChange().
		FromMany(paymentChannelCreationStates...).ToJustRecord().
		Action(recordReceived),

	fsm.Event(rm.ClientEventSendFunds).
		From(rm.DealStatusFundsNeeded).To(rm.DealStatusSendFunds).
		From(rm.DealStatusFundsNeededLastPayment).To(rm.DealStatusSendFundsLastPayment),

	// Sending Payments
	fsm.Event(rm.ClientEventFundsExpended).
		FromMany(rm.DealStatusCheckFunds).To(rm.DealStatusInsufficientFunds).
		Action(func(deal *rm.ClientDealState, shortfall abi.TokenAmount) error {
			deal.Message = fmt.Sprintf("not enough current or pending funds in payment channel, shortfall of %s", shortfall.String())
			return nil
		}),
	fsm.Event(rm.ClientEventBadPaymentRequested).
		FromMany(rm.DealStatusSendFunds, rm.DealStatusSendFundsLastPayment).To(rm.DealStatusFailing).
		Action(func(deal *rm.ClientDealState, message string) error {
			deal.Message = message
			return nil
		}),
	fsm.Event(rm.ClientEventCreateVoucherFailed).
		FromMany(rm.DealStatusSendFunds, rm.DealStatusSendFundsLastPayment).To(rm.DealStatusFailing).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = xerrors.Errorf("creating payment voucher: %w", err).Error()
			return nil
		}),
	fsm.Event(rm.ClientEventVoucherShortfall).
		FromMany(rm.DealStatusSendFunds, rm.DealStatusSendFundsLastPayment).To(rm.DealStatusCheckFunds).
		Action(func(deal *rm.ClientDealState, shortfall abi.TokenAmount) error {
			return nil
		}),

	fsm.Event(rm.ClientEventWriteDealPaymentErrored).
		FromAny().To(rm.DealStatusErrored).
		Action(func(deal *rm.ClientDealState, err error) error {
			deal.Message = xerrors.Errorf("writing deal payment: %w", err).Error()
			return nil
		}),
	fsm.Event(rm.ClientEventPaymentSent).
		From(rm.DealStatusSendFunds).To(rm.DealStatusOngoing).
		From(rm.DealStatusSendFundsLastPayment).To(rm.DealStatusFinalizing).
		Action(func(deal *rm.ClientDealState) error {
			// paymentRequested = 0
			// fundsSpent = fundsSpent + paymentRequested
			// if paymentRequested / pricePerByte >= currentInterval
			// currentInterval = currentInterval + proposal.intervalIncrease
			// bytesPaidFor = bytesPaidFor + (paymentRequested / pricePerByte)
			deal.FundsSpent = big.Add(deal.FundsSpent, deal.PaymentRequested)

			paymentForUnsealing := big.Min(deal.PaymentRequested, big.Sub(deal.UnsealPrice, deal.UnsealFundsPaid))

			bytesPaidFor := big.Div(big.Sub(deal.PaymentRequested, paymentForUnsealing), deal.PricePerByte).Uint64()
			if bytesPaidFor >= deal.CurrentInterval {
				deal.CurrentInterval += deal.DealProposal.PaymentIntervalIncrease
			}
			deal.BytesPaidFor += bytesPaidFor
			deal.UnsealFundsPaid = big.Add(deal.UnsealFundsPaid, paymentForUnsealing)
			deal.PaymentRequested = abi.NewTokenAmount(0)
			return nil
		}),

	// completing deals
	fsm.Event(rm.ClientEventComplete).
		From(rm.DealStatusOngoing).To(rm.DealStatusCheckComplete).
		From(rm.DealStatusFinalizing).To(rm.DealStatusCompleted),
	fsm.Event(rm.ClientEventCompleteVerified).
		From(rm.DealStatusCheckComplete).To(rm.DealStatusCompleted),
	fsm.Event(rm.ClientEventEarlyTermination).
		From(rm.DealStatusCheckComplete).To(rm.DealStatusErrored).
		Action(func(deal *rm.ClientDealState) error {
			deal.Message = "Provider sent complete status without sending all data"
			return nil
		}),

	// after cancelling a deal is complete
	fsm.Event(rm.ClientEventCancelComplete).
		From(rm.DealStatusFailing).To(rm.DealStatusErrored).
		From(rm.DealStatusCancelling).To(rm.DealStatusCancelled),

	// receiving a cancel indicating most likely that the provider experienced something wrong on their
	// end, unless we are already failing or cancelling
	fsm.Event(rm.ClientEventProviderCancelled).
		From(rm.DealStatusFailing).ToJustRecord().
		From(rm.DealStatusCancelling).ToJustRecord().
		FromAny().To(rm.DealStatusErrored).Action(
		func(deal *rm.ClientDealState) error {
			if deal.Status != rm.DealStatusFailing && deal.Status != rm.DealStatusCancelling {
				deal.Message = "Provider cancelled retrieval due to error"
			}
			return nil
		},
	),

	// user manually cancells retrieval
	fsm.Event(rm.ClientEventCancel).FromAny().To(rm.DealStatusCancelling).Action(func(deal *rm.ClientDealState) error {
		deal.Message = "Retrieval Cancelled"
		return nil
	}),

	// payment channel receives more money, we believe there may be reason to recheck the funds for this channel
	fsm.Event(rm.ClientEventRecheckFunds).From(rm.DealStatusInsufficientFunds).To(rm.DealStatusCheckFunds),
}

// ClientFinalityStates are terminal states after which no further events are received
var ClientFinalityStates = []fsm.StateKey{
	rm.DealStatusErrored,
	rm.DealStatusCompleted,
	rm.DealStatusCancelled,
	rm.DealStatusRejected,
	rm.DealStatusDealNotFound,
}

// ClientStateEntryFuncs are the handlers for different states in a retrieval client
var ClientStateEntryFuncs = fsm.StateEntryFuncs{
	rm.DealStatusNew:                          ProposeDeal,
	rm.DealStatusRetryLegacy:                  ProposeDeal,
	rm.DealStatusAccepted:                     SetupPaymentChannelStart,
	rm.DealStatusPaymentChannelCreating:       WaitPaymentChannelReady,
	rm.DealStatusPaymentChannelAllocatingLane: AllocateLane,
	rm.DealStatusOngoing:                      Ongoing,
	rm.DealStatusFundsNeeded:                  ProcessPaymentRequested,
	rm.DealStatusFundsNeededLastPayment:       ProcessPaymentRequested,
	rm.DealStatusSendFunds:                    SendFunds,
	rm.DealStatusSendFundsLastPayment:         SendFunds,
	rm.DealStatusCheckFunds:                   CheckFunds,
	rm.DealStatusPaymentChannelAddingFunds:    WaitPaymentChannelReady,
	rm.DealStatusFailing:                      CancelDeal,
	rm.DealStatusCancelling:                   CancelDeal,
	rm.DealStatusCheckComplete:                CheckComplete,
}
