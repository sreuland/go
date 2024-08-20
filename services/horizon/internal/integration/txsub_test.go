package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/services/horizon/internal/test/integration"
	"github.com/stellar/go/txnbuild"
)

func TestTxSub(t *testing.T) {
	tt := assert.New(t)

	t.Run("transaction submission is successful when DISABLE_TX_SUB=false", func(t *testing.T) {
		itest := integration.NewTest(t, integration.Config{})
		master := itest.Master()

		op := txnbuild.Payment{
			Destination: master.Address(),
			Amount:      "10",
			Asset:       txnbuild.NativeAsset{},
		}

		txResp, err := itest.SubmitOperations(itest.MasterAccount(), master, &op)
		assert.NoError(t, err)

		var seq int64
		tt.Equal(itest.MasterAccount().GetAccountID(), txResp.Account)
		seq, err = itest.MasterAccount().GetSequenceNumber()
		assert.NoError(t, err)
		tt.Equal(seq, txResp.AccountSequence)
		t.Logf("Done")
	})

	t.Run("transaction submission is not successful when DISABLE_TX_SUB=true", func(t *testing.T) {
		itest := integration.NewTest(t, integration.Config{
			HorizonEnvironment: map[string]string{
				"DISABLE_TX_SUB": "true",
			},
		})
		master := itest.Master()

		op := txnbuild.Payment{
			Destination: master.Address(),
			Amount:      "10",
			Asset:       txnbuild.NativeAsset{},
		}

		_, err := itest.SubmitOperations(itest.MasterAccount(), master, &op)
		assert.Error(t, err)
	})
}

func TestTxSubLimitsBodySize(t *testing.T) {
	// the base64 tx blob posted for tx with just 'op1' is 289, with both ops, 365
	// setup the test so that it given one active size threshold configured,
	// it passes with just 'op1' and will fail with two ops that surpass size threshold.
	itest := integration.NewTest(t, integration.Config{
		HorizonEnvironment: map[string]string{
			"MAX_HTTP_REQUEST_SIZE": "300",
			"LOG_LEVEL":             "Debug",
		},
	})

	master := itest.Master()
	op1 := txnbuild.Payment{
		Destination: master.Address(),
		Amount:      "10",
		Asset:       txnbuild.NativeAsset{},
	}

	op2 := txnbuild.Payment{
		Destination: master.Address(),
		Amount:      "10",
		Asset:       txnbuild.NativeAsset{},
	}

	_, err := itest.SubmitOperations(itest.MasterAccount(), master, &op1, &op2)

	assert.EqualError(
		t, err,
		"horizon error: \"Transaction Malformed\" - check horizon.Error.Problem for more information",
	)

	// assert that the single op payload is under the limit and still works.
	tx, err := itest.SubmitOperations(itest.MasterAccount(), master, &op1)
	require.NoError(t, err)
	require.True(t, tx.Successful)
}
