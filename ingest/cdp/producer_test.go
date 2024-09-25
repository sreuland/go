package cdp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDefaultBSBConfigs(t *testing.T) {
	smallConfig := ledgerbackend.BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
		BufferSize: 500,
		NumWorkers: 5,
	}

	mediumConfig := ledgerbackend.BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
		BufferSize: 10,
		NumWorkers: 5,
	}

	largeConfig := ledgerbackend.BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
		BufferSize: 10,
		NumWorkers: 2,
	}

	assert.Equal(t, DefaultBufferedStorageBackendConfig(1), smallConfig)
	assert.Equal(t, DefaultBufferedStorageBackendConfig(2), mediumConfig)
	assert.Equal(t, DefaultBufferedStorageBackendConfig(100), mediumConfig)
	assert.Equal(t, DefaultBufferedStorageBackendConfig(101), largeConfig)
	assert.Equal(t, DefaultBufferedStorageBackendConfig(1000), largeConfig)
}

func TestBSBProducerFn(t *testing.T) {
	startLedger := uint32(2)
	endLedger := uint32(3)
	ctx := context.Background()
	ledgerRange := ledgerbackend.BoundedRange(startLedger, endLedger)
	mockDataStore := createMockdataStore(t, startLedger, endLedger, 64000)
	dsConfig := datastore.DataStoreConfig{}
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       dsConfig,
		BufferedStorageConfig: DefaultBufferedStorageBackendConfig(1),
	}

	// inject the mock datastore using the package private testing factory override
	datastoreFactory = func(ctx context.Context, datastoreConfig datastore.DataStoreConfig) (datastore.DataStore, error) {
		assert.Equal(t, datastoreConfig, dsConfig)
		return mockDataStore, nil
	}

	expectedLcmSeqWasPublished := []bool{false, false}

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		if lcm.MustV0().LedgerHeader.Header.LedgerSeq == 2 {
			if expectedLcmSeqWasPublished[0] {
				assert.Fail(t, "producer fn had multiple callback invocations for same lcm")
			}
			expectedLcmSeqWasPublished[0] = true
		}
		if lcm.MustV0().LedgerHeader.Header.LedgerSeq == 3 {
			if expectedLcmSeqWasPublished[1] {
				assert.Fail(t, "producer fn had multiple callback invocations for same lcm")
			}
			expectedLcmSeqWasPublished[1] = true
		}
		return nil
	}

	resultCh := PublishFromBufferedStorageBackend(ledgerRange, pubConfig, ctx, appCallback)

	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.Failf(t, "", "producer fn should not have stopped with error %v", chErr)
			}
			return true
		default:
		}
		return false
	},
		time.Second*3,
		time.Millisecond*50)

	assert.Equal(t, expectedLcmSeqWasPublished, []bool{true, true}, "producer fn did not invoke callback for all expected lcm")

}

func TestBSBProducerFnDataStoreError(t *testing.T) {
	ctx := context.Background()
	ledgerRange := ledgerbackend.BoundedRange(uint32(2), uint32(3))
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: ledgerbackend.BufferedStorageBackendConfig{},
	}

	datastoreFactory = func(ctx context.Context, datastoreConfig datastore.DataStoreConfig) (datastore.DataStore, error) {
		return &datastore.MockDataStore{}, errors.New("uhoh")
	}

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return nil
	}

	resultCh := PublishFromBufferedStorageBackend(ledgerRange, pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "failed to create datastore:")
			} else {
				assert.Fail(t, "", "producer fn should not have closed the result ch")
			}
			return true
		default:
		}
		return false
	},
		time.Second*3,
		time.Millisecond*50)
}

func TestBSBProducerFnConfigError(t *testing.T) {
	ctx := context.Background()
	ledgerRange := ledgerbackend.BoundedRange(uint32(2), uint32(3))
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: ledgerbackend.BufferedStorageBackendConfig{},
	}
	mockDataStore := new(datastore.MockDataStore)
	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return nil
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(ledgerRange, pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "failed to create buffered storage backend")
			} else {
				assert.Fail(t, "producer fn should not have closed the result ch")
			}
			return true
		default:
		}
		return false
	},
		time.Second*3,
		time.Millisecond*50)
}

func TestBSBProducerFnInvalidRange(t *testing.T) {
	ctx := context.Background()
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: DefaultBufferedStorageBackendConfig(1),
	}
	mockDataStore := new(datastore.MockDataStore)
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 1,
	})

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return nil
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(ledgerbackend.BoundedRange(uint32(3), uint32(2)), pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "invalid end value for bounded range, must be greater than start")
			} else {
				assert.Fail(t, "producer fn should not have closed the result ch")
			}
			return true
		default:
		}
		return false
	},
		time.Second*3,
		time.Millisecond*50)
}

func TestBSBProducerFnGetLedgerError(t *testing.T) {
	ctx := context.Background()
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: DefaultBufferedStorageBackendConfig(1),
	}
	// we don't want to wait for retries, forece the first error to propagate
	pubConfig.BufferedStorageConfig.RetryLimit = 0
	mockDataStore := new(datastore.MockDataStore)
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 1,
	})

	mockDataStore.On("GetFile", mock.Anything, "FFFFFFFD--2.xdr.zstd").Return(nil, os.ErrNotExist).Once()
	mockDataStore.On("GetFile", mock.Anything, "FFFFFFFC--3.xdr.zstd").Return(makeSingleLCMBatch(3), nil).Once()

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return nil
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(ledgerbackend.BoundedRange(uint32(2), uint32(3)), pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "error getting ledger")
			} else {
				assert.Fail(t, "producer fn should not have closed the result ch")
			}
			return true
		default:
		}
		return false
	},
		time.Second*3000,
		time.Millisecond*50)
}

func TestBSBProducerFnCallbackError(t *testing.T) {
	ctx := context.Background()
	pubConfig := ledgerbackend.PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: DefaultBufferedStorageBackendConfig(1),
	}
	mockDataStore := createMockdataStore(t, 2, 3, 64000)

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return errors.New("uhoh")
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(ledgerbackend.BoundedRange(uint32(2), uint32(3)), pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "received an error from callback invocation")
			} else {
				assert.Fail(t, "producer fn should not have closed the result ch")
			}
			return true
		default:
		}
		return false
	},
		time.Second*3,
		time.Millisecond*50)
}

func createMockdataStore(t *testing.T, start, end, partitionSize uint32) *datastore.MockDataStore {
	mockDataStore := new(datastore.MockDataStore)
	partition := partitionSize - 1
	for i := start; i <= end; i++ {
		objectName := fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-i, i)
		mockDataStore.On("GetFile", mock.Anything, objectName).Return(makeSingleLCMBatch(i), nil).Times(1)
	}
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: partitionSize,
	})

	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	return mockDataStore
}

func makeSingleLCMBatch(seq uint32) io.ReadCloser {
	lcm := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(seq),
		EndSequence:   xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			createLedgerCloseMeta(seq),
		},
	}
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, lcm)
	var buf bytes.Buffer
	encoder.WriteTo(&buf)
	capturedBuf := buf.Bytes()
	reader := bytes.NewReader(capturedBuf)
	return io.NopCloser(reader)
}

func createLedgerCloseMeta(ledgerSeq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: int32(0),
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet:              xdr.TransactionSet{},
			TxProcessing:       nil,
			UpgradesProcessing: nil,
			ScpInfo:            nil,
		},
		V1: nil,
	}
}
