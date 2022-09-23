package integration

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/test/integration"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestInvokeHostFunction(t *testing.T) {
	tt := assert.New(t)
	itest := integration.NewTest(t, integration.Config{})

	tx, err := itest.SubmitOperations(itest.MasterAccount(), itest.Master(),
		&txnbuild.InvokeHostFunction{
			Function:   xdr.HostFunctionHostFnCall,
			Footprint:  xdr.LedgerFootprint{},
			Parameters: xdr.ScVec{},
		},
	)

	tt.NoError(err)
	clientTx, err := itest.Client().TransactionDetail(tx.Hash)
	tt.NoError(err)
	tt.Equal(tx.Hash, clientTx.Hash)
}
