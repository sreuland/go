package actions

import (
	"encoding/hex"
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

type testTransaction struct {
	index         uint32
	envelopeXDR   string
	resultXDR     string
	feeChangesXDR string
	metaXDR       string
	hash          string
}

func buildLedgerTransaction(t *testing.T, tx testTransaction) ingest.LedgerTransaction {
	transaction := ingest.LedgerTransaction{
		Index:      tx.index,
		Envelope:   xdr.TransactionEnvelope{},
		Result:     xdr.TransactionResultPair{},
		FeeChanges: xdr.LedgerEntryChanges{},
		UnsafeMeta: xdr.TransactionMeta{},
	}

	tt := assert.New(t)

	err := xdr.SafeUnmarshalBase64(tx.envelopeXDR, &transaction.Envelope)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(tx.resultXDR, &transaction.Result.Result)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(tx.metaXDR, &transaction.UnsafeMeta)
	tt.NoError(err)
	err = xdr.SafeUnmarshalBase64(tx.feeChangesXDR, &transaction.FeeChanges)
	tt.NoError(err)

	_, err = hex.Decode(transaction.Result.TransactionHash[:], []byte(tx.hash))
	tt.NoError(err)

	return transaction
}
