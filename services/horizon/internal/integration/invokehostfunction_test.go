package integration

import (
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/services/horizon/internal/test/integration"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestInvokeHostFunction(t *testing.T) {
	tt := assert.New(t)
	itest := integration.NewTest(t, integration.Config{})

	seed := "SDHOAMBNLGCE2MV5ZKIVZAQD3VCLGP53P3OBSBI6UN5L5XZI5TKHFQL4"
	seedBytes := [32]byte{}
	copy(seedBytes[:], []byte(seed)[:32])
	privateKey := ed25519.NewKeyFromSeed(seedBytes[:])
	privateKeyBytes := [32]byte{}
	copy(privateKeyBytes[:], privateKey[:32])
	publicKeyBytes := [32]byte{}
	copy(publicKeyBytes[:], privateKey[32:])

	sourceKp, err := keypair.FromRawSeed([32]byte(seedBytes))
	tt.NoError(err)
	sourceAccount := txnbuild.NewSimpleAccount(sourceKp.Address(), 5)

	/*
		kp2 := keypair.MustParseFull("SDHOAMBNLGCE2MV5ZKIVZAQD3VCLGP53P3OBSBI6UN5L5XZI5TKHFQL4")
		publicKey2 := ed25519.PublicKey{98, 252, 29, 11, 208, 145, 178, 182, 28, 13, 214, 86, 52, 107, 42, 104, 215, 211, 71, 198, 242, 194, 200, 238, 109, 4, 71, 2, 86, 252, 5, 247}
		privateKey2 := ed25519.PrivateKey{206, 224, 48, 45, 89, 132, 77, 50, 189, 202, 145, 92, 130, 3, 221, 68, 179, 63, 187, 126, 220, 25, 5, 30, 163, 122, 190, 223, 40, 236, 212, 114, 98, 252, 29, 11, 208, 145, 178, 182, 28, 13, 214, 86, 52, 107, 42, 104, 215, 211, 71, 198, 242, 194, 200, 238, 109, 4, 71, 2, 86, 252, 5, 247}
	*/

	sha256Hash := sha256.New()
	contract := []byte("test_contract")
	salt := sha256.Sum256(([]byte("a1")))
	separator := []byte("create_contract_from_ed25519(contract: Vec<u8>, salt: u256, key: u256, sig: Vec<u8>)")

	sha256Hash.Write(separator)
	sha256Hash.Write(salt[:])
	sha256Hash.Write(contract)

	contractHash := sha256Hash.Sum([]byte{})
	contractSig, err := privateKey.Sign(nil, contractHash, crypto.Hash(0))
	tt.NoError(err)

	preImage := xdr.HashIdPreimageEd25519ContractId{
		Ed25519: xdr.Uint256(privateKeyBytes),
		Salt:    xdr.Uint256(salt),
	}
	xdrPreImageBytes, err := preImage.MarshalBinary()
	tt.NoError(err)
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

	tt.NoError(err)
	clientTx, err := itest.Client().TransactionDetail(tx.Hash)
	tt.NoError(err)
	tt.Equal(tx.Hash, clientTx.Hash)

	var txResult xdr.TransactionResult
	xdr.SafeUnmarshalBase64(clientTx.ResultXdr, &txResult)
	opResults, ok := txResult.OperationResults()
	tt.True(ok)
	tt.Equal(len(opResults), 1)
	invokeHostFunctionResult, ok := opResults[0].MustTr().GetInvokeHostFunctionResult()
	tt.True(ok)
	tt.Equal(invokeHostFunctionResult.Code, xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess)
}
