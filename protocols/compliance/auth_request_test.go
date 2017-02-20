package compliance

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stellar/go/build"
	"github.com/stellar/go/protocols/attachment"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPopulate(t *testing.T) {
	request := &http.Request{
		PostForm: url.Values{
			"data": []string{"data"},
			"sig":  []string{"sig"},
		},
	}

	authRequest := &AuthRequest{}
	authRequest.Populate(request)

	assert.Equal(t, "data", authRequest.DataJSON)
	assert.Equal(t, "sig", authRequest.Signature)
}

func TestValidateSuccess(t *testing.T) {
	attachment := attachment.Attachment{
		Transaction: attachment.Transaction{
			SenderInfo: attachment.SenderInfo{
				FirstName: "Bartek",
			},
			Route: "jed*stellar.org",
		},
	}
	attachment.GenerateNonce()

	attachHash, err := attachment.Hash()
	require.NoError(t, err)
	attachMarshalled, err := attachment.Marshal()
	require.NoError(t, err)

	txBuilder := build.Transaction(
		build.SourceAccount{"GAW77Z6GPWXSODJOMF5L5BMX6VMYGEJRKUNBC2CZ725JTQZORK74HQQD"},
		build.Sequence{0},
		build.TestNetwork,
		build.MemoHash{attachHash},
		build.Payment(
			build.Destination{"GAMVF7G4GJC4A7JMFJWLUAEIBFQD5RT3DCB5DC5TJDEKQBBACQ4JZVEE"},
			build.CreditAmount{"USD", "GAMVF7G4GJC4A7JMFJWLUAEIBFQD5RT3DCB5DC5TJDEKQBBACQ4JZVEE", "20"},
		),
	)

	txB64, err := xdr.MarshalBase64(txBuilder.TX)
	require.NoError(t, err)

	authData := &AuthData{
		Sender:         "bartek*stellar.org",
		NeedInfo:       false,
		Tx:             txB64,
		AttachmentJSON: string(attachMarshalled),
	}

	dataJSON, err := authData.Marshal()
	require.NoError(t, err)

	authRequest := &AuthRequest{
		DataJSON:  string(dataJSON),
		Signature: "test",
	}

	assert.NoError(t, authRequest.Validate())
}

func TestData(t *testing.T) {
	authRequest := &AuthRequest{
		DataJSON: `{"sender": "sender", "need_info": true, "tx": "tx", "attachment": "attachment"}`,
	}

	authData, err := authRequest.Data()
	require.NoError(t, err)
	assert.Equal(t, "sender", authData.Sender)
	assert.Equal(t, true, authData.NeedInfo)
	assert.Equal(t, "tx", authData.Tx)
	assert.Equal(t, "attachment", authData.AttachmentJSON)
}
