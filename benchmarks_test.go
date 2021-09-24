package benchmarks

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stellar/go/gxdr"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"
	goxdr "github.com/xdrpp/goxdr/xdr"
)

const input64 = "AAAAAgAAAADy2f6v1nv9lXdvl5iZvWKywlPQYsZ1JGmmAfewflnbUAAABLACG4bdAADOYQAAAAEAAAAAAAAAAAAAAABhSLZ9AAAAAAAAAAEAAAABAAAAAF8wDgs7+R5R2uftMvvhHliZOyhZOQWsWr18/Fu6S+g0AAAAAwAAAAJHRE9HRQAAAAAAAAAAAAAAUwsPRQlK+jECWsJLURlsP0qsbA/aIaB/z50U79VSRYsAAAAAAAAAAAAAAYMAAA5xAvrwgAAAAAAAAAAAAAAAAAAAAAJ+WdtQAAAAQCTonAxUHyuVsmaSeGYuVsGRXgxs+wXvKgSa+dapZWN4U9sxGPuApjiv/UWb47SwuFQ+q40bfkPYT1Tff4RfLQe6S+g0AAAAQBlFjwF/wpGr+DWbjCyuolgM1VP/e4ubfUlVnDAdFjJUIIzVakZcr5omRSnr7ClrwEoPj49h+vcLusagC4xFJgg="

var input = func() []byte {
	decoded, err := base64.StdEncoding.DecodeString(input64)
	if err != nil {
		panic(err)
	}
	return decoded
}()

func BenchmarkXDRUnmarshal(b *testing.B) {
	te := xdr.TransactionEnvelope{}

	// Make sure the input is valid.
	err := te.UnmarshalBinary(input)
	require.NoError(b, err)

	// Benchmark.
	for i := 0; i < b.N; i++ {
		_ = te.UnmarshalBinary(input)
	}
}

func BenchmarkGXDRUnmarshal(b *testing.B) {
	te := gxdr.TransactionEnvelope{}

	// Make sure the input is valid, note goxdr will panic if there's a
	// marshaling error.
	te.XdrMarshal(&goxdr.XdrIn{In: bytes.NewReader(input)}, "")

	// Benchmark.
	r := bytes.NewReader(input)
	for i := 0; i < b.N; i++ {
		r.Reset(input)
		te.XdrMarshal(&goxdr.XdrIn{In: r}, "")
	}
}

func BenchmarkXDRMarshal(b *testing.B) {
	te := xdr.TransactionEnvelope{}

	// Make sure the input is valid.
	err := te.UnmarshalBinary(input)
	require.NoError(b, err)
	output, err := te.MarshalBinary()
	require.NoError(b, err)
	require.Equal(b, input, output)

	// Benchmark.
	for i := 0; i < b.N; i++ {
		_, _ = te.MarshalBinary()
	}
}

func BenchmarkGXDRMarshal(b *testing.B) {
	te := gxdr.TransactionEnvelope{}

	// Make sure the input is valid, note goxdr will panic if there's a
	// marshaling error.
	te.XdrMarshal(&goxdr.XdrIn{In: bytes.NewReader(input)}, "")
	output := bytes.Buffer{}
	te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")

	// Benchmark.
	for i := 0; i < b.N; i++ {
		output.Reset()
		te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")
	}
}

func TestXDRMarshalLedgerEntryExtensionV1(t *testing.T) {
	te := xdr.LedgerEntryExtensionV1{}
	output, err := te.MarshalBinary()
	require.NoError(t, err)
	t.Logf(base64.StdEncoding.EncodeToString(output))

	address := "GBEUFD3PR6DY3JX3QI76GVNUZBOLDNAS6KODSXXKTLJ7TKIK5RF6HESR"
	accountID := xdr.AccountId{}
	accountID.SetAddress(address)

	te = xdr.LedgerEntryExtensionV1{SponsoringId: &accountID}
	output, err = te.MarshalBinary()
	require.NoError(t, err)
	t.Logf(base64.StdEncoding.EncodeToString(output))
}

func TestGXDRMarshalLedgerEntryExtensionV1(t *testing.T) {
	te := gxdr.LedgerEntryExtensionV1{}
	output := bytes.Buffer{}
	te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")
	t.Logf(base64.StdEncoding.EncodeToString(output.Bytes()))

	address := "GBEUFD3PR6DY3JX3QI76GVNUZBOLDNAS6KODSXXKTLJ7TKIK5RF6HESR"
	accountID := &gxdr.AccountID{Type: gxdr.PUBLIC_KEY_TYPE_ED25519}
	ed25519 := accountID.Ed25519()
	rawEd25519, err := strkey.Decode(strkey.VersionByteAccountID, address)
	require.NoError(t, err)
	copy(ed25519[:], rawEd25519)

	te = gxdr.LedgerEntryExtensionV1{SponsoringID: accountID}
	output = bytes.Buffer{}
	te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")
	t.Logf(base64.StdEncoding.EncodeToString(output.Bytes()))
}

func TestXDRMarshalAccountEntryExtensionV2(t *testing.T) {
	te := xdr.AccountEntryExtensionV2{}
	output, err := te.MarshalBinary()
	require.NoError(t, err)
	t.Logf(base64.StdEncoding.EncodeToString(output))

	address := "GBEUFD3PR6DY3JX3QI76GVNUZBOLDNAS6KODSXXKTLJ7TKIK5RF6HESR"
	accountID := xdr.AccountId{}
	accountID.SetAddress(address)

	te = xdr.AccountEntryExtensionV2{
		SignerSponsoringIDs: []xdr.SponsorshipDescriptor{
			nil,
			&accountID,
			nil,
		},
	}
	output, err = te.MarshalBinary()
	require.NoError(t, err)
	t.Logf(base64.StdEncoding.EncodeToString(output))
}

func TestGXDRMarshalAccountEntryExtensionV2(t *testing.T) {
	te := gxdr.AccountEntryExtensionV2{}
	output := bytes.Buffer{}
	te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")
	t.Logf(base64.StdEncoding.EncodeToString(output.Bytes()))

	address := "GBEUFD3PR6DY3JX3QI76GVNUZBOLDNAS6KODSXXKTLJ7TKIK5RF6HESR"
	accountID := &gxdr.AccountID{Type: gxdr.PUBLIC_KEY_TYPE_ED25519}
	ed25519 := accountID.Ed25519()
	rawEd25519, err := strkey.Decode(strkey.VersionByteAccountID, address)
	require.NoError(t, err)
	copy(ed25519[:], rawEd25519)

	te = gxdr.AccountEntryExtensionV2{
		SignerSponsoringIDs: []gxdr.SponsorshipDescriptor{
			nil,
			accountID,
			nil,
		},
	}
	output = bytes.Buffer{}
	te.XdrMarshal(&goxdr.XdrOut{Out: &output}, "")
	t.Logf(base64.StdEncoding.EncodeToString(output.Bytes()))
}
