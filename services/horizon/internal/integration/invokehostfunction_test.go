package integration

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"testing"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/services/horizon/internal/test/integration"
	"github.com/stellar/go/strkey"

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

	var rawSeed [32]byte
	_, err := io.ReadFull(rand.Reader, rawSeed[:])
	require.NoError(t, err)
	strSeed, err := strkey.Encode(strkey.VersionByteSeed, rawSeed[:])
	require.NoError(t, err)

	reader := bytes.NewReader(rawSeed[:])
	publicKey, privateKey, err := ed25519.GenerateKey(reader)
	require.NoError(t, err)

	privateKeyBytes := [32]byte{}
	copy(privateKeyBytes[:], privateKey[:32])
	publicKeyBytes := [32]byte{}
	copy(publicKeyBytes[:], publicKey[:32])

	sourceKp := keypair.MustParseFull(strSeed)

	// fund the contract owner account on network
	createAccountOp := txnbuild.CreateAccount{
		Destination: sourceKp.Address(),
		Amount:      "5000",
	}

	tx, err := itest.SubmitOperations(itest.MasterAccount(), itest.Master(), &createAccountOp)
	require.NoError(t, err)

	// get the account and it's current seq
	sourceAccount, err := itest.Client().AccountDetail(horizonclient.AccountRequest{
		AccountID: sourceKp.Address(),
	})
	require.NoError(t, err)

	sha256Hash := sha256.New()
	contract := []byte("test_contract")
	salt := sha256.Sum256(([]byte("a1")))
	separator := []byte("create_contract_from_ed25519(contract: Vec<u8>, salt: u256, key: u256, sig: Vec<u8>)")

	sha256Hash.Write(separator)
	sha256Hash.Write(salt[:])
	sha256Hash.Write(contract)

	contractHash := sha256Hash.Sum([]byte{})
	contractSig, err := privateKey.Sign(nil, contractHash, crypto.Hash(0))
	require.NoError(t, err)

	preImage := xdr.HashIdPreimageEd25519ContractId{
		Ed25519: xdr.Uint256(privateKeyBytes),
		Salt:    xdr.Uint256(salt),
	}
	xdrPreImageBytes, err := preImage.MarshalBinary()
	require.NoError(t, err)
	hashedContractID := sha256.Sum256(xdrPreImageBytes)

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

	publicKeySlice := publicKeyBytes[:]
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

	tx, err = itest.SubmitOperations(&sourceAccount, sourceKp,
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
