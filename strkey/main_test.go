package strkey

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	cases := []struct {
		Name                string
		Address             string
		ExpectedVersionByte VersionByte
	}{
		{
			Name:                "AccountID",
			Address:             "GA3D5KRYM6CB7OWQ6TWYRR3Z4T7GNZLKERYNZGGA5SOAOPIFY6YQHES5",
			ExpectedVersionByte: VersionByteAccountID,
		},
		{
			Name:                "Seed",
			Address:             "SBU2RRGLXH3E5CQHTD3ODLDF2BWDCYUSSBLLZ5GNW7JXHDIYKXZWHOKR",
			ExpectedVersionByte: VersionByteSeed,
		},
		{
			Name:                "HashTx",
			Address:             "TBU2RRGLXH3E5CQHTD3ODLDF2BWDCYUSSBLLZ5GNW7JXHDIYKXZWHXL7",
			ExpectedVersionByte: VersionByteHashTx,
		},
		{
			Name:                "HashX",
			Address:             "XBU2RRGLXH3E5CQHTD3ODLDF2BWDCYUSSBLLZ5GNW7JXHDIYKXZWGTOG",
			ExpectedVersionByte: VersionByteHashX,
		},
		{
			Name:                "Signed Payload",
			Address:             "PDPYP7E6NEYZSVOTV6M23OFM2XRIMPDUJABHGHHH2Y67X7JL25GW6AAAAAAAAAAAAAAJEVA",
			ExpectedVersionByte: VersionByteSignedPayload,
		},
		{
			Name:                "Other (0x60)",
			Address:             "MBU2RRGLXH3E5CQHTD3ODLDF2BWDCYUSSBLLZ5GNW7JXHDIYKXZWGTOG",
			ExpectedVersionByte: VersionByte(0x60),
		},
	}

	for _, kase := range cases {
		actual, err := Version(kase.Address)
		if assert.NoError(t, err, "An error occured decoding case %s", kase.Name) {
			assert.Equal(t, kase.ExpectedVersionByte, actual, "Output mismatch in case %s", kase.Name)
		}
	}
}

func TestIsValidEd25519PublicKey(t *testing.T) {
	validKey := "GDWZCOEQRODFCH6ISYQPWY67L3ULLWS5ISXYYL5GH43W7YFMTLB65PYM"
	isValid := IsValidEd25519PublicKey(validKey)
	assert.Equal(t, true, isValid)

	invalidKey := "GDWZCOEQRODFCH6ISYQPWY67L3ULLWS5ISXYYL5GH43W7Y"
	isValid = IsValidEd25519PublicKey(invalidKey)
	assert.Equal(t, false, isValid)

	invalidKey = ""
	isValid = IsValidEd25519PublicKey(invalidKey)
	assert.Equal(t, false, isValid)

	invalidKey = "SBCVMMCBEDB64TVJZFYJOJAERZC4YVVUOE6SYR2Y76CBTENGUSGWRRVO"
	isValid = IsValidEd25519PublicKey(invalidKey)
	assert.Equal(t, false, isValid)
}

func TestIsValidEd25519SecretSeed(t *testing.T) {
	validKey := "SBCVMMCBEDB64TVJZFYJOJAERZC4YVVUOE6SYR2Y76CBTENGUSGWRRVO"
	isValid := IsValidEd25519SecretSeed(validKey)
	assert.Equal(t, true, isValid)

	invalidKey := "SBCVMMCBEDB64TVJZFYJOJAERZC4YVVUOE6SYR2Y76CBTENGUSG"
	isValid = IsValidEd25519SecretSeed(invalidKey)
	assert.Equal(t, false, isValid)

	invalidKey = ""
	isValid = IsValidEd25519SecretSeed(invalidKey)
	assert.Equal(t, false, isValid)

	invalidKey = "GDWZCOEQRODFCH6ISYQPWY67L3ULLWS5ISXYYL5GH43W7YFMTLB65PYM"
	isValid = IsValidEd25519SecretSeed(invalidKey)
	assert.Equal(t, false, isValid)
}
