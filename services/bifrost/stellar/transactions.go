package stellar

import (
	"strconv"

	"github.com/stellar/go/build"
	"github.com/stellar/go/clients/horizon"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
)

func (ac *AccountConfigurator) createAccount(destination string) error {
	err := ac.submitTransaction(
		build.CreateAccount(
			build.SourceAccount{ac.IssuerPublicKey},
			build.Destination{destination},
			build.NativeAmount{NewAccountXLMBalance},
		),
	)
	if err != nil {
		return errors.Wrap(err, "Error submitting transaction")
	}

	return nil
}

func (ac *AccountConfigurator) sendToken(destination, assetCode, amount string) error {
	err := ac.submitTransaction(
		build.Payment(
			build.SourceAccount{ac.IssuerPublicKey},
			build.Destination{destination},
			build.CreditAmount{
				Code:   assetCode,
				Issuer: ac.IssuerPublicKey,
				Amount: amount,
			},
		),
	)
	if err != nil {
		return errors.Wrap(err, "Error submitting transaction")
	}

	return nil
}

func (ac *AccountConfigurator) submitTransaction(mutator build.TransactionMutator) error {
	tx, err := ac.buildTransaction(mutator)
	if err != nil {
		return errors.Wrap(err, "Error building transaction")
	}

	localLog := log.WithField("tx", tx)
	localLog.Info("Submitting transaction")

	_, err = ac.Horizon.SubmitTransaction(tx)
	if err != nil {
		fields := log.F{"err": err}
		if err, ok := err.(*horizon.Error); ok {
			fields["result"] = string(err.Problem.Extras["result_xdr"])
			ac.updateSequence()
		}
		localLog.WithFields(fields).Error("Error submitting transaction")
		return errors.Wrap(err, "Error submitting transaction")
	}

	localLog.Info("Transaction successfully submitted")
	return nil
}

func (ac *AccountConfigurator) buildTransaction(mutator build.TransactionMutator) (string, error) {
	tx := build.Transaction(
		build.SourceAccount{ac.signerPublicKey},
		build.Sequence{ac.getSequence()},
		build.Network{ac.NetworkPassphrase},
		mutator,
	)

	txe := tx.Sign(ac.SignerSecretKey)
	return txe.Base64()
}

func (ac *AccountConfigurator) updateSequence() error {
	ac.sequenceMutex.Lock()
	defer ac.sequenceMutex.Unlock()

	account, err := ac.Horizon.LoadAccount(ac.signerPublicKey)
	if err != nil {
		err = errors.Wrap(err, "Error loading issuing account")
		ac.log.Error(err)
		return err
	}

	ac.sequence, err = strconv.ParseUint(account.Sequence, 10, 64)
	if err != nil {
		err = errors.Wrap(err, "Invalid account.Sequence")
		ac.log.Error(err)
		return err
	}

	return nil
}

func (ac *AccountConfigurator) getSequence() uint64 {
	ac.sequenceMutex.Lock()
	defer ac.sequenceMutex.Unlock()
	ac.sequence++
	sequence := ac.sequence
	return sequence
}
