package serve

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/services/regulated-assets-approval-server/internal/serve/httperror"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/http/httpdecode"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
)

type txApproveHandler struct {
	issuerKP          *keypair.Full
	assetCode         string
	horizonClient     horizonclient.ClientInterface
	networkPassphrase string
	db                *sqlx.DB
	kycThreshold      int64
	baseURL           string
}

type txApproveRequest struct {
	Tx string `json:"tx" form:"tx"`
}

// validate performs some validations on the provided handler data.
func (h txApproveHandler) validate() error {
	if h.issuerKP == nil {
		return errors.New("issuer keypair cannot be nil")
	}
	if h.assetCode == "" {
		return errors.New("asset code cannot be empty")
	}
	if h.horizonClient == nil {
		return errors.New("horizon client cannot be nil")
	}
	if h.networkPassphrase == "" {
		return errors.New("network passphrase cannot be empty")
	}
	if h.db == nil {
		return errors.New("database cannot be nil")
	}
	if h.kycThreshold <= 0 {
		return errors.New("kyc threshold cannot be less than or equal to zero")
	}
	if h.baseURL == "" {
		return errors.New("base url cannot be empty")
	}
	return nil
}

func convertThresholdToReadableString(threshold int64) (string, error) {
	thresholdStr := amount.StringFromInt64(threshold)
	res, err := strconv.ParseFloat(thresholdStr, 1)
	if err != nil {
		return "", errors.Wrap(err, "converting threshold amount from string to float")
	}
	return fmt.Sprintf("%.2f", res), nil
}

func (h txApproveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := h.validate()
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "validating txApproveHandler"))
		httperror.InternalServer.Render(w)
		return
	}

	in := txApproveRequest{}
	err = httpdecode.Decode(r, &in)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "decoding txApproveRequest"))
		httperror.BadRequest.Render(w)
		return
	}

	txApproveResp, err := h.txApprove(ctx, in)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "validating the input transaction for approval"))
		httperror.InternalServer.Render(w)
		return
	}

	txApproveResp.Render(w)
}

// validateInput performs some validations on the provided transaction. It can
// reject the transaction based on general criteria that would be applied in any
// approval server.
func (h txApproveHandler) validateInput(ctx context.Context, in txApproveRequest) (*txApprovalResponse, *txnbuild.Transaction) {
	if in.Tx == "" {
		log.Ctx(ctx).Error(`request is missing parameter "tx".`)
		return NewRejectedTxApprovalResponse(`Missing parameter "tx".`), nil
	}

	genericTx, err := txnbuild.TransactionFromXDR(in.Tx)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "parsing transaction xdr"))
		return NewRejectedTxApprovalResponse(`Invalid parameter "tx".`), nil
	}

	tx, ok := genericTx.Transaction()
	if !ok {
		log.Ctx(ctx).Error(`invalid parameter "tx", generic transaction not given.`)
		return NewRejectedTxApprovalResponse(`Invalid parameter "tx".`), nil
	}

	if tx.SourceAccount().AccountID == h.issuerKP.Address() {
		log.Ctx(ctx).Errorf("transaction %s sourceAccount is the same as the server issuer account %s",
			in.Tx,
			h.issuerKP.Address())
		return NewRejectedTxApprovalResponse("The source account is invalid."), nil
	}

	// The server's rules state that only one operation must be in the transaction.
	// However if there are 5 operations we should skip this reject step to evaluate if it's an incoming revised transaction.
	if len(tx.Operations()) != 1 && len(tx.Operations()) != 5 {
		return NewRejectedTxApprovalResponse("Please submit a transaction with exactly one operation of type payment."), nil
	}

	for _, op := range tx.Operations() {
		if op.GetSourceAccount() == h.issuerKP.Address() {
			log.Ctx(ctx).Error(`transaction contains one or more operations where sourceAccount is issuer account.`)
			return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
		}
	}

	return nil, tx
}

// checkIfRevisedTransaction inspects incoming transaction if it's already has been revised.
// A revised transaction can be built by wallets preemptively or by the server in order to make it compliant.
// The transaction must have the following operations in the following order.
// Operation 1: AllowTrust op where issuer fully authorizes account A, asset X
// Operation 2: AllowTrust op where issuer fully authorizes account B, asset X
// Operation 3: Payment from A to B
// Operation 4: AllowTrust op where issuer fully deauthorizes account B, asset X
// Operation 5: AllowTrust op where issuer fully deauthorizes account A, asset X
func (h txApproveHandler) checkIfRevisedTransaction(ctx context.Context, tx *txnbuild.Transaction) (resp *txApprovalResponse, err error) {
	if len(tx.Operations()) != 5 {
		return nil, nil
	}
	// Extract the payment operation and source account
	paymentOp, ok := tx.Operations()[2].(*txnbuild.Payment)
	if !ok {
		log.Ctx(ctx).Error(`third operation is not of type payment`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}
	paymentSource := paymentOp.SourceAccount
	if paymentSource == "" {
		paymentSource = tx.SourceAccount().AccountID
	}

	// Check if the transaction given it equivalent to a transaction revised by the server.
	expectedOperations := []txnbuild.Operation{
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: h.issuerKP.Address(),
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: h.issuerKP.Address(),
		},
		paymentOp,
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: h.issuerKP.Address(),
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: h.issuerKP.Address(),
		},
	}
	if !reflect.DeepEqual(expectedOperations, tx.Operations()) {
		return nil, errors.New("incoming transaction's operations are not compliant")
	}

	// Check if issuer's and or paymentSource's and or an unknown signature is included in transaction.
	var paymentSourceSigExists, issuerSigExists, unknownSigExists bool
	paymentSourceKP := keypair.MustParseAddress(paymentSource)
	for _, sig := range tx.Signatures() {
		if sig.Hint == paymentSourceKP.Hint() {
			paymentSourceSigExists = true
		} else if sig.Hint == h.issuerKP.Hint() {
			issuerSigExists = true
		} else {
			unknownSigExists = true
			break
		}
	}

	// Reject incoming transaction with unknown signature(s).
	if unknownSigExists {
		return NewRejectedTxApprovalResponse("One or more signatures in the provided transaction are unauthorized."), nil
	}
	// Reject incoming transaction without payment source account's signature.
	if !paymentSourceSigExists {
		return NewRejectedTxApprovalResponse("Transaction must be signed by the transaction's source account in order to be compliant."), nil
	}
	// Issuer signs incoming transaction that doesn't have issuer's signature.
	if !issuerSigExists {
		tx, err = tx.Sign(h.networkPassphrase, h.issuerKP)
		if err != nil {
			return nil, errors.Wrap(err, "signing transaction")
		}
		issuerSigExists = true
	}

	// Reject incoming transaction with more than two signatures.
	if len(tx.Signatures()) > 2 {
		return NewRejectedTxApprovalResponse("Amount of signatures in the provided transaction exceeds limit."), nil
	}

	// Check if sender account needs to submit KYC on the incoming transaction.
	kycRequiredResponse, err := h.handleKYCRequiredOperationIfNeeded(ctx, paymentSource, paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "handling KYC required payment")
	}
	if kycRequiredResponse != nil {
		return kycRequiredResponse, nil
	}

	acc, err := h.horizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: paymentSource})
	if err != nil {
		return nil, errors.Wrapf(err, "getting detail for payment source account %s", paymentSource)
	}
	// validate the sequence number
	accountSequence, err := strconv.ParseInt(acc.Sequence, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing account sequence number %q from string to int64", acc.Sequence)
	}
	if tx.SourceAccount().Sequence != accountSequence+1 {
		log.Ctx(ctx).Errorf(`invalid transaction sequence number tx.SourceAccount().Sequence: %d, accountSequence+1:%d`, tx.SourceAccount().Sequence, accountSequence+1)
		return NewRejectedTxApprovalResponse("Invalid transaction sequence number."), nil
	}

	// Encode revised transaction for response.
	txe, err := tx.Base64()
	if err != nil {
		return nil, errors.Wrap(err, "encoding revised transaction")
	}

	// Generate message if the transaction still requires a signature from the payment source account.
	var message strings.Builder
	message.WriteString("Transaction is compliant and signed by the issuer")
	if paymentSourceSigExists {
		message.WriteString(". Ready to submit!")
	} else {
		message.WriteString(", please add payment sender's signature before submitting!")
	}

	return NewSuccessTxApprovalResponse(txe, message.String()), err
}

// txApprove is called to validate the input transaction.
func (h txApproveHandler) txApprove(ctx context.Context, in txApproveRequest) (resp *txApprovalResponse, err error) {
	defer func() {
		log.Ctx(ctx).Debug("==== will log responses ====")
		log.Ctx(ctx).Debugf("req: %+v", in)
		log.Ctx(ctx).Debugf("resp: %+v", resp)
		log.Ctx(ctx).Debugf("err: %+v", err)
		log.Ctx(ctx).Debug("====  did log responses ====")
	}()

	txRejectedResp, tx := h.validateInput(ctx, in)
	if txRejectedResp != nil {
		return txRejectedResp, nil
	}

	txSuccessResp, err := h.checkIfRevisedTransaction(ctx, tx)
	if err != nil {
		return nil, errors.Wrap(err, "checking if transaction in request was revised")
	}
	if txSuccessResp != nil {
		return txSuccessResp, nil
	}

	paymentOp, ok := tx.Operations()[0].(*txnbuild.Payment)
	if !ok {
		log.Ctx(ctx).Error(`transaction contains one or more operations is not of type payment`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}
	paymentSource := paymentOp.SourceAccount
	if paymentSource == "" {
		paymentSource = tx.SourceAccount().AccountID
	}

	issuerAddress := h.issuerKP.Address()
	if paymentOp.Asset.GetCode() != h.assetCode || paymentOp.Asset.GetIssuer() != issuerAddress {
		log.Ctx(ctx).Error(`the payment asset is not supported by this issuer`)
		return NewRejectedTxApprovalResponse("The payment asset is not supported by this issuer."), nil
	}

	acc, err := h.horizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: paymentSource})
	if err != nil {
		return nil, errors.Wrapf(err, "getting detail for payment source account %s", paymentSource)
	}
	// validate the sequence number
	accountSequence, err := strconv.ParseInt(acc.Sequence, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing account sequence number %q from string to int64", acc.Sequence)
	}
	if tx.SourceAccount().Sequence != accountSequence+1 {
		log.Ctx(ctx).Errorf(`invalid transaction sequence number tx.SourceAccount().Sequence: %d, accountSequence+1:%d`, tx.SourceAccount().Sequence, accountSequence+1)
		return NewRejectedTxApprovalResponse("Invalid transaction sequence number."), nil
	}
	// Validate if payment operation requires KYC.
	var kycRequiredResponse *txApprovalResponse
	kycRequiredResponse, err = h.handleKYCRequiredOperationIfNeeded(ctx, paymentSource, paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "handling KYC required payment")
	}
	if kycRequiredResponse != nil {
		return kycRequiredResponse, nil
	}
	// build the transaction
	revisedOperations := []txnbuild.Operation{
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: issuerAddress,
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: issuerAddress,
		},
		paymentOp,
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: issuerAddress,
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: issuerAddress,
		},
	}
	revisedTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &acc,
		IncrementSequenceNum: true,
		Operations:           revisedOperations,
		BaseFee:              300,
		Timebounds:           txnbuild.NewTimeout(300),
	})
	if err != nil {
		return nil, errors.Wrap(err, "building transaction")
	}

	revisedTx, err = revisedTx.Sign(h.networkPassphrase, h.issuerKP)
	if err != nil {
		return nil, errors.Wrap(err, "signing transaction")
	}

	txe, err := revisedTx.Base64()
	if err != nil {
		return nil, errors.Wrap(err, "encoding revised transaction")
	}

	return NewRevisedTxApprovalResponse(txe), nil
}

// handleKYCRequiredOperationIfNeeded validates and returns an action_required response if the payment requires KYC.
func (h txApproveHandler) handleKYCRequiredOperationIfNeeded(ctx context.Context, stellarAddress string, paymentOp *txnbuild.Payment) (*txApprovalResponse, error) {
	// validate payment operation against KYC condition(s).
	KYCRequiredMessage, err := h.kycRequiredMessageIfNeeded(paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "validating KYC")
	}
	if KYCRequiredMessage == "" {
		return nil, nil
	}

	intendedCallbackID := uuid.New().String()
	const q = `
		WITH new_row AS (
			INSERT INTO accounts_kyc_status (stellar_address, callback_id)
			VALUES ($1, $2)
			ON CONFLICT(stellar_address) DO NOTHING
			RETURNING *
		)
		SELECT callback_id, approved_at, rejected_at FROM new_row
		UNION
		SELECT callback_id, approved_at, rejected_at
		FROM accounts_kyc_status
		WHERE stellar_address = $1
	`
	var (
		callbackID             string
		approvedAt, rejectedAt sql.NullTime
	)
	err = h.db.QueryRowContext(ctx, q, stellarAddress, intendedCallbackID).Scan(&callbackID, &approvedAt, &rejectedAt)
	if err != nil {
		return nil, errors.Wrap(err, "inserting new row into accounts_kyc_status table")
	}

	if approvedAt.Valid {
		return nil, nil
	}
	if rejectedAt.Valid {
		kycThreshold, err := convertThresholdToReadableString(h.kycThreshold)
		if err != nil {
			return nil, errors.Wrap(err, "converting kycThreshold to human readable string")
		}
		return NewRejectedTxApprovalResponse(fmt.Sprintf("Your KYC was rejected and you're not authorized for operations above %s %s.", kycThreshold, h.assetCode)), nil
	}

	return NewActionRequiredTxApprovalResponse(
		KYCRequiredMessage,
		fmt.Sprintf("%s/kyc-status/%s", h.baseURL, callbackID),
		[]string{"email_address"},
	), nil
}

// kycRequiredMessageIfNeeded returns a "action_required" message for the NewActionRequiredTxApprovalResponse if the payment operation meets KYC conditions.
// Currently rule(s) are, checking if payment amount is > KYCThreshold amount.
func (h txApproveHandler) kycRequiredMessageIfNeeded(paymentOp *txnbuild.Payment) (string, error) {
	paymentAmount, err := amount.ParseInt64(paymentOp.Amount)
	if err != nil {
		return "", errors.Wrap(err, "parsing account payment amount from string to Int64")
	}
	if paymentAmount > h.kycThreshold {
		kycThreshold, err := convertThresholdToReadableString(h.kycThreshold)
		if err != nil {
			return "", errors.Wrap(err, "converting kycThreshold to human readable string")
		}
		return fmt.Sprintf(`Payments exceeding %s %s requires KYC approval. Please provide an email address.`, kycThreshold, h.assetCode), nil
	}
	return "", nil
}
