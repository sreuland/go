package ledgerexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyResumeDatastoreComplete(t *testing.T) {
	ctx := context.Background()
	app := &App{}
	app.config = &Config{StartLedger: 0, EndLedger: 9, Resume: true}
	mockResumableManager := &MockResumableManager{}
	mockResumableManager.On("FindStart", ctx, uint32(0), uint32(9)).Return(uint32(0), true).Once()

	var alreadyExported *DataAlreadyExportedError
	err := app.applyResumability(ctx, mockResumableManager)
	require.ErrorAs(t, err, &alreadyExported)
	mockResumableManager.AssertExpectations(t)
}

func TestApplyResumeInvalidDataStoreLedgersPerFileBoundary(t *testing.T) {
	ctx := context.Background()
	app := &App{}
	app.config = &Config{
		StartLedger:       0,
		EndLedger:         9,
		Resume:            true,
		LedgerBatchConfig: LedgerBatchConfig{LedgersPerFile: 10, FilesPerPartition: 50},
	}
	mockResumableManager := &MockResumableManager{}
	// simulate the datastore has inconsistent data,
	// with last ledger not aligned to starting boundary
	mockResumableManager.On("FindStart", ctx, uint32(0), uint32(9)).Return(uint32(6), false).Once()

	var invalidStore *InvalidDataStoreError
	err := app.applyResumability(ctx, mockResumableManager)
	require.ErrorAs(t, err, &invalidStore)
	mockResumableManager.AssertExpectations(t)
}

func TestApplyResumeWithPartialRemoteDataPresent(t *testing.T) {
	ctx := context.Background()
	app := &App{}
	app.config = &Config{
		StartLedger:       0,
		EndLedger:         99,
		Resume:            true,
		LedgerBatchConfig: LedgerBatchConfig{LedgersPerFile: 10, FilesPerPartition: 50},
	}
	mockResumableManager := &MockResumableManager{}
	// simulates a data store that had ledger files populated up to seq=49, so the first absent ledger would be 50
	mockResumableManager.On("FindStart", ctx, uint32(0), uint32(99)).Return(uint32(50), false).Once()

	err := app.applyResumability(ctx, mockResumableManager)
	require.NoError(t, err)
	require.Equal(t, app.config.StartLedger, uint32(50))
	mockResumableManager.AssertExpectations(t)
}

func TestApplyResumeWithNoRemoteDataPresent(t *testing.T) {
	ctx := context.Background()
	app := &App{}
	app.config = &Config{
		StartLedger:       0,
		EndLedger:         99,
		Resume:            true,
		LedgerBatchConfig: LedgerBatchConfig{LedgersPerFile: 10, FilesPerPartition: 50},
	}
	mockResumableManager := &MockResumableManager{}
	// simulates a data store that had no data in the requested range
	mockResumableManager.On("FindStart", ctx, uint32(0), uint32(99)).Return(uint32(0), false).Once()

	err := app.applyResumability(ctx, mockResumableManager)
	require.NoError(t, err)
	require.Equal(t, app.config.StartLedger, uint32(0))
	mockResumableManager.AssertExpectations(t)
}
