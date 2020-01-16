package xdr_test

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthorized(t *testing.T) {
	tt := assert.New(t)

	flag := xdr.TrustLineFlags(1)
	tt.True(flag.IsAuthorized())

	flag = xdr.TrustLineFlags(0)
	tt.False(flag.IsAuthorized())

	flag = xdr.TrustLineFlags(2)
	tt.False(flag.IsAuthorized())
}
