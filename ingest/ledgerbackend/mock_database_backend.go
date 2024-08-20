package ledgerbackend

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/xdr"
)

var _ LedgerBackend = (*MockLedgerBackend)(nil)

type MockLedgerBackend struct {
	mock.Mock
}

func (m *MockLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *MockLedgerBackend) PrepareRange(ctx context.Context, ledgerRange Range) error {
	args := m.Called(ctx, ledgerRange)
	return args.Error(0)
}

func (m *MockLedgerBackend) IsPrepared(ctx context.Context, ledgerRange Range) (bool, error) {
	args := m.Called(ctx, ledgerRange)
	return args.Bool(0), args.Error(1)
}

func (m *MockLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	args := m.Called(ctx, sequence)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Error(1)
}

func (m *MockLedgerBackend) Close() error {
	args := m.Called()
	return args.Error(0)
}
