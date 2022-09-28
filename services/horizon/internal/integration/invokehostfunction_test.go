package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/services/horizon/internal/test/integration"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvokeHostFunctionCreateContract(t *testing.T) {
	os.Setenv("HORIZON_INTEGRATION_TESTS_ENABLED", "true")
	//os.Setenv("HORIZON_INTEGRATION_TESTS_ENABLE_CAPTIVE_CORE", "true")
	os.Setenv("HORIZON_INTEGRATION_TESTS_CORE_MAX_SUPPORTED_PROTOCOL", "20")
	//os.Setenv("HORIZON_INTEGRATION_TESTS_CAPTIVE_CORE_BIN", "/usr/local/bin/stellar-core")
	os.Setenv("HORIZON_INTEGRATION_TESTS_DOCKER_IMG", "bartekno/stellar-core:19.4.1-1078.c4dee576f.focal-soroban2")
	//integration.RunWithCaptiveCore = true

	if integration.GetCoreMaxSupportedProtocol() < 20 {
		t.Skip("This test run does not support Protocol 20")
	}

	itest := integration.NewTest(t, integration.Config{
		ProtocolVersion: 20,
	})

	// get the account and it's current seq
	sourceAccount, err := itest.Client().AccountDetail(horizonclient.AccountRequest{
		AccountID: itest.Master().Address(),
	})
	require.NoError(t, err)

	// Assemble the InvokeHostFunction CreateContract operation, this is supposed to follow the
	// specs in CAP-0047 - https://github.com/stellar/stellar-protocol/blob/master/core/cap-0047.md#creating-a-contract-using-invokehostfunctionop
	// also using soroban-cli as a reference for InvokeHostFunction tx creation - https://github.com/stellar/soroban-cli/pull/152/files#diff-a1009ce51ac98a8e648338a8315b8e3e75ea9849daf84c572f1600a03a6a94b9R111
	sha256Hash := sha256.New()
	contract, err := os.ReadFile(filepath.Join("testdata", "example_add_i32.wasm"))
	require.NoError(t, err)
	t.Logf("Contract File Contents: %v", hex.EncodeToString(contract))
	salt := sha256.Sum256([]byte("a1"))
	t.Logf("Salt hash: %v", hex.EncodeToString(salt[:]))
	separator := []byte("create_contract_from_ed25519(contract: Vec<u8>, salt: u256, key: u256, sig: Vec<u8>)")

	sha256Hash.Write(separator)
	sha256Hash.Write(salt[:])
	sha256Hash.Write(contract)

	contractHash := sha256Hash.Sum([]byte{})
	t.Logf("hash to sign: %v", hex.EncodeToString(contractHash))
	contractSig, err := itest.Master().Sign(contractHash)
	require.NoError(t, err)

	t.Logf("Signature of Hash: %v", hex.EncodeToString(contractSig))
	var publicKeyXDR xdr.Uint256
	copy(publicKeyXDR[:], itest.Master().PublicKey())
	preImage := xdr.HashIdPreimage{
		Type: xdr.EnvelopeTypeEnvelopeTypeContractIdFromEd25519,
		Ed25519ContractId: &xdr.HashIdPreimageEd25519ContractId{
			Salt:    salt,
			Ed25519: publicKeyXDR,
		},
	}
	xdrPreImageBytes, err := preImage.MarshalBinary()
	require.NoError(t, err)
	hashedContractID := sha256.Sum256(xdrPreImageBytes)
	t.Log(hashedContractID)

	contractNameParameterAddr := &xdr.ScObject{
		Type: xdr.ScObjectTypeScoBytes,
		Bin:  &contract,
	}
	contractNameParameter := xdr.ScVal{
		Type: xdr.ScValTypeScvObject,
		Obj:  &contractNameParameterAddr,
	}

	saltySlice := salt[:]
	saltParameterAddr := &xdr.ScObject{
		Type: xdr.ScObjectTypeScoBytes,
		Bin:  &saltySlice,
	}
	saltParameter := xdr.ScVal{
		Type: xdr.ScValTypeScvObject,
		Obj:  &saltParameterAddr,
	}

	publicKeySlice := []byte(itest.Master().PublicKey())
	publicKeyParameterAddr := &xdr.ScObject{
		Type: xdr.ScObjectTypeScoBytes,
		Bin:  &publicKeySlice,
	}
	publicKeyParameter := xdr.ScVal{
		Type: xdr.ScValTypeScvObject,
		Obj:  &publicKeyParameterAddr,
	}

	contractSignatureParaeterAddr := &xdr.ScObject{
		Type: xdr.ScObjectTypeScoBytes,
		Bin:  &contractSig,
	}
	contractSignatureParameter := xdr.ScVal{
		Type: xdr.ScValTypeScvObject,
		Obj:  &contractSignatureParaeterAddr,
	}

	ledgerKeyContractCodeAddr := xdr.ScStaticScsLedgerKeyContractCode
	ledgerKey := xdr.LedgerKeyContractData{
		ContractId: xdr.Hash(hashedContractID),
		Key: xdr.ScVal{
			Type: xdr.ScValTypeScvStatic,
			Ic:   &ledgerKeyContractCodeAddr,
		},
	}

	params := xdr.ScVec{
		contractNameParameter,
		saltParameter,
		publicKeyParameter,
		contractSignatureParameter,
	}
	paramsBin, err := params.MarshalBinary()
	require.NoError(t, err)
	t.Log("XDR args to Submit:", hex.EncodeToString(paramsBin))

	tx, err := itest.SubmitOperations(&sourceAccount, itest.Master(),
		&txnbuild.InvokeHostFunction{
			Function: xdr.HostFunctionHostFnCreateContract,
			Footprint: xdr.LedgerFootprint{
				ReadWrite: []xdr.LedgerKey{
					{
						Type:         xdr.LedgerEntryTypeContractData,
						ContractData: &ledgerKey,
					},
				},
			},
			Parameters: []xdr.ScVal{
				contractNameParameter,
				saltParameter,
				publicKeyParameter,
				contractSignatureParameter,
			},
		},
	)
	require.NoError(t, err)

	clientTx, err := itest.Client().TransactionDetail(tx.Hash)
	require.NoError(t, err)

	assert.Equal(t, tx.Hash, clientTx.Hash)
	var txResult xdr.TransactionResult
	xdr.SafeUnmarshalBase64(clientTx.ResultXdr, &txResult)
	opResults, ok := txResult.OperationResults()
	assert.True(t, ok)
	assert.Equal(t, len(opResults), 1)
	invokeHostFunctionResult, ok := opResults[0].MustTr().GetInvokeHostFunctionResult()
	assert.True(t, ok)
	assert.Equal(t, invokeHostFunctionResult.Code, xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)
}
