package ledgerbackend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

var partitionSize = uint32(64000)
var ledgerPerFileCount = uint32(1)

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

func createBufferedStorageBackendConfigForTesting() BufferedStorageBackendConfig {
	param := make(map[string]string)
	param["destination_bucket_path"] = "testURL"

	return BufferedStorageBackendConfig{
		BufferSize: 100,
		NumWorkers: 5,
		RetryLimit: 3,
		RetryWait:  time.Microsecond,
	}
}

func createBufferedStorageBackendForTesting() BufferedStorageBackend {
	config := createBufferedStorageBackendConfigForTesting()

	dataStore := new(datastore.MockDataStore)
	return BufferedStorageBackend{
		config:    config,
		dataStore: dataStore,
	}
}

func createMockdataStore(t *testing.T, start, end, partitionSize, count uint32) *datastore.MockDataStore {
	mockDataStore := new(datastore.MockDataStore)
	partition := count*partitionSize - 1
	for i := start; i <= end; i = i + count {
		var objectName string
		var readCloser io.ReadCloser
		if count > 1 {
			endFileSeq := i + count - 1
			readCloser = createLCMBatchReader(i, endFileSeq, count)
			objectName = fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d-%d.xdr.zstd", partition, math.MaxUint32-i, i, endFileSeq)
		} else {
			readCloser = createLCMBatchReader(i, i, count)
			objectName = fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-i, i)
		}
		mockDataStore.On("GetFile", mock.Anything, objectName).Return(readCloser, nil).Times(1)
	}
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    count,
		FilesPerPartition: partitionSize,
	})

	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	return mockDataStore
}

func createLCMForTesting(start, end uint32) []xdr.LedgerCloseMeta {
	var lcmArray []xdr.LedgerCloseMeta
	for i := start; i <= end; i++ {
		lcmArray = append(lcmArray, createLedgerCloseMeta(i))
	}

	return lcmArray
}

func createTestLedgerCloseMetaBatch(startSeq, endSeq, count uint32) xdr.LedgerCloseMetaBatch {
	var ledgerCloseMetas []xdr.LedgerCloseMeta
	for i := uint32(0); i < count; i++ {
		ledgerCloseMetas = append(ledgerCloseMetas, createLedgerCloseMeta(startSeq+uint32(i)))
	}
	return xdr.LedgerCloseMetaBatch{
		StartSequence:    xdr.Uint32(startSeq),
		EndSequence:      xdr.Uint32(endSeq),
		LedgerCloseMetas: ledgerCloseMetas,
	}
}

func createLCMBatchReader(start, end, count uint32) io.ReadCloser {
	testData := createTestLedgerCloseMetaBatch(start, end, count)
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, testData)
	var buf bytes.Buffer
	encoder.WriteTo(&buf)
	capturedBuf := buf.Bytes()
	reader := bytes.NewReader(capturedBuf)
	return io.NopCloser(reader)
}

func TestNewBufferedStorageBackend(t *testing.T) {
	config := createBufferedStorageBackendConfigForTesting()
	mockDataStore := new(datastore.MockDataStore)
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    uint32(1),
		FilesPerPartition: partitionSize,
	})
	bsb, err := NewBufferedStorageBackend(config, mockDataStore)
	assert.NoError(t, err)

	assert.Equal(t, bsb.dataStore, mockDataStore)
	assert.Equal(t, uint32(1), bsb.dataStore.GetSchema().LedgersPerFile)
	assert.Equal(t, uint32(64000), bsb.dataStore.GetSchema().FilesPerPartition)
	assert.Equal(t, uint32(100), bsb.config.BufferSize)
	assert.Equal(t, uint32(5), bsb.config.NumWorkers)
	assert.Equal(t, uint32(3), bsb.config.RetryLimit)
	assert.Equal(t, time.Microsecond, bsb.config.RetryWait)
}

func TestNewLedgerBuffer(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(7)
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 2
	bsb.config.BufferSize = 5
	ledgerRange := BoundedRange(startLedger, endLedger)
	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	ledgerBuffer, err := bsb.newLedgerBuffer(ledgerRange)
	assert.Eventually(t, func() bool { return len(ledgerBuffer.ledgerQueue) == 5 }, time.Second*5, time.Millisecond*50)
	assert.NoError(t, err)

	latestSeq, err := ledgerBuffer.getLatestLedgerSequence()
	assert.NoError(t, err)
	assert.Equal(t, uint32(7), latestSeq)
	assert.Equal(t, ledgerRange, ledgerBuffer.ledgerRange)
}

func TestBSBGetLatestLedgerSequence(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(5)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)
	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 3 }, time.Second*5, time.Millisecond*50)

	latestSeq, err := bsb.GetLatestLedgerSequence(ctx)
	assert.NoError(t, err)

	assert.Equal(t, uint32(5), latestSeq)
}

func TestBSBGetLedger_SingleLedgerPerFile(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(5)
	ctx := context.Background()
	lcmArray := createLCMForTesting(startLedger, endLedger)
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 3 }, time.Second*5, time.Millisecond*50)

	lcm, err := bsb.GetLedger(ctx, uint32(3))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[0], lcm)
	lcm, err = bsb.GetLedger(ctx, uint32(4))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[1], lcm)
	lcm, err = bsb.GetLedger(ctx, uint32(5))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[2], lcm)
}

func TestCloudStorageGetLedger_MultipleLedgerPerFile(t *testing.T) {
	startLedger := uint32(2)
	endLedger := uint32(5)
	lcmArray := createLCMForTesting(startLedger, endLedger)
	bsb := createBufferedStorageBackendForTesting()
	ctx := context.Background()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, 2)
	bsb.dataStore = mockDataStore
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    uint32(2),
		FilesPerPartition: partitionSize,
	})
	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 2 }, time.Second*5, time.Millisecond*50)

	lcm, err := bsb.GetLedger(ctx, uint32(2))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[0], lcm)
	lcm, err = bsb.GetLedger(ctx, uint32(3))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[1], lcm)
	lcm, err = bsb.GetLedger(ctx, uint32(4))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[2], lcm)
}

func TestBSBGetLedger_ErrorPreceedingLedger(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(5)
	ctx := context.Background()
	lcmArray := createLCMForTesting(startLedger, endLedger)
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 3 }, time.Second*5, time.Millisecond*50)

	lcm, err := bsb.GetLedger(ctx, uint32(3))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[0], lcm)

	_, err = bsb.GetLedger(ctx, uint32(2))
	assert.EqualError(t, err, "requested sequence preceeds current LedgerRange")
}

func TestBSBGetLedger_NotPrepared(t *testing.T) {
	bsb := createBufferedStorageBackendForTesting()
	ctx := context.Background()

	_, err := bsb.GetLedger(ctx, uint32(3))
	assert.EqualError(t, err, "session is not prepared, call PrepareRange first")
}

func TestBSBGetLedger_SequenceNotInBatch(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(5)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 3 }, time.Second*5, time.Millisecond*50)

	_, err := bsb.GetLedger(ctx, uint32(2))
	assert.EqualError(t, err, "requested sequence preceeds current LedgerRange")

	_, err = bsb.GetLedger(ctx, uint32(6))
	assert.EqualError(t, err, "requested sequence beyond current LedgerRange")
}

func TestBSBPrepareRange(t *testing.T) {
	startLedger := uint32(2)
	endLedger := uint32(3)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 2 }, time.Second*5, time.Millisecond*50)

	assert.NotNil(t, bsb.prepared)

	// check alreadyPrepared
	err := bsb.PrepareRange(ctx, ledgerRange)
	assert.NoError(t, err)
	assert.NotNil(t, bsb.prepared)
}

func TestBSBProducerFn(t *testing.T) {
	startLedger := uint32(2)
	endLedger := uint32(3)
	ctx := context.Background()
	ledgerRange := BoundedRange(startLedger, endLedger)
	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	dsConfig := datastore.DataStoreConfig{}
	pubConfig := PublisherConfig{
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
	ledgerRange := BoundedRange(uint32(2), uint32(3))
	pubConfig := PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: BufferedStorageBackendConfig{},
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
	ledgerRange := BoundedRange(uint32(2), uint32(3))
	pubConfig := PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: BufferedStorageBackendConfig{},
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
	pubConfig := PublisherConfig{
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
	resultCh := PublishFromBufferedStorageBackend(BoundedRange(uint32(3), uint32(2)), pubConfig, ctx, appCallback)
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

	resultCh = PublishFromBufferedStorageBackend(Range{from: uint32(2), to: uint32(3), bounded: false}, pubConfig, ctx, appCallback)
	assert.Eventually(t, func() bool {
		select {
		case chErr, ok := <-resultCh:
			if ok {
				assert.ErrorContains(t, chErr, "invalid end value for unbounded ranged, must be zero")
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
	pubConfig := PublisherConfig{
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
	mockDataStore.On("GetFile", mock.Anything, "FFFFFFFC--3.xdr.zstd").Return(createLCMBatchReader(3, 3, 1), nil).Once()

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return nil
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(BoundedRange(uint32(2), uint32(3)), pubConfig, ctx, appCallback)
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
	pubConfig := PublisherConfig{
		DataStoreConfig:       datastore.DataStoreConfig{},
		BufferedStorageConfig: DefaultBufferedStorageBackendConfig(1),
	}
	mockDataStore := createMockdataStore(t, 2, 3, partitionSize, ledgerPerFileCount)

	appCallback := func(lcm xdr.LedgerCloseMeta) error {
		return errors.New("uhoh")
	}

	datastoreFactory = func(_ context.Context, _ datastore.DataStoreConfig) (datastore.DataStore, error) {
		return mockDataStore, nil
	}
	resultCh := PublishFromBufferedStorageBackend(BoundedRange(uint32(2), uint32(3)), pubConfig, ctx, appCallback)
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

func TestDefaultBSBConfigs(t *testing.T) {
	smallConfig := BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
		BufferSize: 500,
		NumWorkers: 5,
	}

	mediumConfig := BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
		BufferSize: 10,
		NumWorkers: 5,
	}

	largeConfig := BufferedStorageBackendConfig{
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

func TestBSBIsPrepared_Bounded(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(5)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 3 }, time.Second*5, time.Millisecond*50)

	ok, err := bsb.IsPrepared(ctx, ledgerRange)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = bsb.IsPrepared(ctx, BoundedRange(2, 4))
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = bsb.IsPrepared(ctx, UnboundedRange(3))
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = bsb.IsPrepared(ctx, UnboundedRange(2))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestBSBIsPrepared_Unbounded(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(8)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 2
	bsb.config.BufferSize = 5
	ledgerRange := UnboundedRange(3)
	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 5 }, time.Second*5, time.Millisecond*50)

	ok, err := bsb.IsPrepared(ctx, ledgerRange)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = bsb.IsPrepared(ctx, BoundedRange(3, 4))
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = bsb.IsPrepared(ctx, BoundedRange(2, 4))
	assert.NoError(t, err)
	assert.False(t, ok)

	ok, err = bsb.IsPrepared(ctx, UnboundedRange(4))
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = bsb.IsPrepared(ctx, UnboundedRange(2))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestBSBClose(t *testing.T) {
	startLedger := uint32(2)
	endLedger := uint32(3)
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 2 }, time.Second*5, time.Millisecond*50)

	err := bsb.Close()
	assert.NoError(t, err)
	assert.Equal(t, true, bsb.closed)

	_, err = bsb.GetLatestLedgerSequence(ctx)
	assert.EqualError(t, err, "BufferedStorageBackend is closed; cannot GetLatestLedgerSequence")

	_, err = bsb.GetLedger(ctx, 3)
	assert.EqualError(t, err, "BufferedStorageBackend is closed; cannot GetLedger")

	err = bsb.PrepareRange(ctx, ledgerRange)
	assert.EqualError(t, err, "BufferedStorageBackend is closed; cannot PrepareRange")

	_, err = bsb.IsPrepared(ctx, ledgerRange)
	assert.EqualError(t, err, "BufferedStorageBackend is closed; cannot IsPrepared")
}

func TestLedgerBufferInvariant(t *testing.T) {
	startLedger := uint32(3)
	endLedger := uint32(6)
	ctx := context.Background()
	lcmArray := createLCMForTesting(startLedger, endLedger)
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 2
	bsb.config.BufferSize = 2
	ledgerRange := BoundedRange(startLedger, endLedger)

	mockDataStore := createMockdataStore(t, startLedger, endLedger, partitionSize, ledgerPerFileCount)
	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 2 }, time.Second*5, time.Millisecond*50)

	// Buffer should have hit the BufferSize limit
	assert.Equal(t, 2, len(bsb.ledgerBuffer.ledgerQueue))

	lcm, err := bsb.GetLedger(ctx, uint32(3))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[0], lcm)
	lcm, err = bsb.GetLedger(ctx, uint32(4))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[1], lcm)

	// Buffer should fill up with remaining ledgers
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 2 }, time.Second*5, time.Millisecond*50)
	assert.Equal(t, 2, len(bsb.ledgerBuffer.ledgerQueue))

	lcm, err = bsb.GetLedger(ctx, uint32(5))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[2], lcm)

	// Buffer should only have the final ledger
	assert.Eventually(t, func() bool { return len(bsb.ledgerBuffer.ledgerQueue) == 1 }, time.Second*5, time.Millisecond*50)
	assert.Equal(t, 1, len(bsb.ledgerBuffer.ledgerQueue))

	lcm, err = bsb.GetLedger(ctx, uint32(6))
	assert.NoError(t, err)
	assert.Equal(t, lcmArray[3], lcm)

	// Buffer should be empty
	assert.Equal(t, 0, len(bsb.ledgerBuffer.ledgerQueue))
}

func TestLedgerBufferClose(t *testing.T) {
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 1
	bsb.config.BufferSize = 5
	ledgerRange := UnboundedRange(3)

	mockDataStore := new(datastore.MockDataStore)
	partition := ledgerPerFileCount*partitionSize - 1
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    ledgerPerFileCount,
		FilesPerPartition: partitionSize,
	})

	objectName := fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-3, 3)
	afterPrepareRange := make(chan struct{})
	mockDataStore.On("GetFile", mock.Anything, objectName).Return(io.NopCloser(&bytes.Buffer{}), context.Canceled).Run(func(args mock.Arguments) {
		<-afterPrepareRange
		go bsb.ledgerBuffer.close()
	}).Once()

	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))
	close(afterPrepareRange)

	bsb.ledgerBuffer.wg.Wait()

	_, err := bsb.GetLedger(ctx, 3)
	assert.EqualError(t, err, "failed getting next ledger batch from queue: context canceled")
}

func TestLedgerBufferBoundedObjectNotFound(t *testing.T) {
	ctx := context.Background()
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 1
	bsb.config.BufferSize = 5
	ledgerRange := BoundedRange(3, 5)

	mockDataStore := new(datastore.MockDataStore)
	partition := ledgerPerFileCount*partitionSize - 1
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    ledgerPerFileCount,
		FilesPerPartition: partitionSize,
	})
	objectName := fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-3, 3)
	mockDataStore.On("GetFile", mock.Anything, objectName).Return(io.NopCloser(&bytes.Buffer{}), os.ErrNotExist).Once()
	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))

	bsb.ledgerBuffer.wg.Wait()

	_, err := bsb.GetLedger(ctx, 3)
	assert.ErrorContains(t, err, "ledger object containing sequence 3 is missing")
	assert.ErrorContains(t, err, objectName)
	assert.ErrorContains(t, err, "file does not exist")
}

func TestLedgerBufferUnboundedObjectNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 1
	bsb.config.BufferSize = 5
	ledgerRange := UnboundedRange(3)

	mockDataStore := new(datastore.MockDataStore)
	partition := ledgerPerFileCount*partitionSize - 1
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    ledgerPerFileCount,
		FilesPerPartition: partitionSize,
	})
	objectName := fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-3, 3)
	iteration := &atomic.Int32{}
	cancelAfter := int32(bsb.config.RetryLimit) + 2
	mockDataStore.On("GetFile", mock.Anything, objectName).Return(io.NopCloser(&bytes.Buffer{}), os.ErrNotExist).Run(func(args mock.Arguments) {
		if iteration.Load() >= cancelAfter {
			cancel()
		}
		iteration.Add(1)
	})
	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	bsb.dataStore = mockDataStore

	assert.NoError(t, bsb.PrepareRange(ctx, ledgerRange))

	_, err := bsb.GetLedger(ctx, 3)
	assert.EqualError(t, err, "failed getting next ledger batch from queue: context canceled")
	assert.GreaterOrEqual(t, iteration.Load(), cancelAfter)
	assert.NoError(t, bsb.Close())
}

func TestLedgerBufferRetryLimit(t *testing.T) {
	bsb := createBufferedStorageBackendForTesting()
	bsb.config.NumWorkers = 1
	bsb.config.BufferSize = 5
	ledgerRange := UnboundedRange(3)

	mockDataStore := new(datastore.MockDataStore)
	partition := ledgerPerFileCount*partitionSize - 1

	objectName := fmt.Sprintf("FFFFFFFF--0-%d/%08X--%d.xdr.zstd", partition, math.MaxUint32-3, 3)
	mockDataStore.On("GetFile", mock.Anything, objectName).
		Return(io.NopCloser(&bytes.Buffer{}), fmt.Errorf("transient error")).
		Times(int(bsb.config.RetryLimit) + 1)
	t.Cleanup(func() {
		mockDataStore.AssertExpectations(t)
	})

	bsb.dataStore = mockDataStore
	mockDataStore.On("GetSchema").Return(datastore.DataStoreSchema{
		LedgersPerFile:    ledgerPerFileCount,
		FilesPerPartition: partitionSize,
	})
	assert.NoError(t, bsb.PrepareRange(context.Background(), ledgerRange))

	bsb.ledgerBuffer.wg.Wait()

	_, err := bsb.GetLedger(context.Background(), 3)
	assert.ErrorContains(t, err, "failed getting next ledger batch from queue")
	assert.ErrorContains(t, err, "maximum retries exceeded for downloading object containing sequence 3")
	assert.ErrorContains(t, err, objectName)
	assert.ErrorContains(t, err, "transient error")
}
