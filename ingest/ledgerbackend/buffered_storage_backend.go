// BufferedStorageBackend is a ledger backend that provides buffered access over a given DataStore.
// The DataStore must contain files generated from a LedgerExporter.

package ledgerbackend

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/ordered"
	"github.com/stellar/go/xdr"
)

// Ensure BufferedStorageBackend implements LedgerBackend
var _ LedgerBackend = (*BufferedStorageBackend)(nil)

// provide testing hooks to inject mocks of these
var datastoreFactory = datastore.NewDataStore

type BufferedStorageBackendConfig struct {
	BufferSize uint32        `toml:"buffer_size"`
	NumWorkers uint32        `toml:"num_workers"`
	RetryLimit uint32        `toml:"retry_limit"`
	RetryWait  time.Duration `toml:"retry_wait"`
}

// Generate a default buffered storage config with values
// set to optimize buffered performance to some degree based
// on number of ledgers per file expected in the underlying
// datastore used by an instance of BufferedStorageBackend.
//
// these numbers were derived empirically from benchmarking analysis:
// https://github.com/stellar/go/issues/5390
//
// ledgersPerFile - number of ledgers per file from remote datastore schema.
// return - preconfigured instance of BufferedStorageBackendConfig
func DefaultBufferedStorageBackendConfig(ledgersPerFile uint32) BufferedStorageBackendConfig {

	config := BufferedStorageBackendConfig{
		RetryLimit: 5,
		RetryWait:  30 * time.Second,
	}

	switch {
	case ledgersPerFile < 2:
		config.BufferSize = 500
		config.NumWorkers = 5
		return config
	case ledgersPerFile < 101:
		config.BufferSize = 10
		config.NumWorkers = 5
		return config
	default:
		config.BufferSize = 10
		config.NumWorkers = 2
		return config
	}
}

// BufferedStorageBackend is a ledger backend that reads from a storage service.
// The storage service contains files generated from the ledgerExporter.
type BufferedStorageBackend struct {
	config BufferedStorageBackendConfig

	bsBackendLock sync.RWMutex

	// ledgerBuffer is the buffer for LedgerCloseMeta data read in parallel.
	ledgerBuffer *ledgerBuffer

	dataStore  datastore.DataStore
	prepared   *Range // Non-nil if any range is prepared
	closed     bool   // False until the core is closed
	lcmBatch   xdr.LedgerCloseMetaBatch
	nextLedger uint32
	lastLedger uint32
}

// NewBufferedStorageBackend returns a new BufferedStorageBackend instance.
func NewBufferedStorageBackend(config BufferedStorageBackendConfig, dataStore datastore.DataStore) (*BufferedStorageBackend, error) {
	if config.BufferSize == 0 {
		return nil, errors.New("buffer size must be > 0")
	}

	if config.NumWorkers > config.BufferSize {
		return nil, errors.New("number of workers must be <= BufferSize")
	}

	if dataStore.GetSchema().LedgersPerFile <= 0 {
		return nil, errors.New("ledgersPerFile must be > 0")
	}

	bsBackend := &BufferedStorageBackend{
		config:    config,
		dataStore: dataStore,
	}

	return bsBackend, nil
}

type PublisherConfig struct {
	// Registry, optional, include to capture buffered storage backend metrics
	Registry *prometheus.Registry
	// RegistryNamespace, optional, include to emit buffered storage backend
	// under this namespace
	RegistryNamespace string
	// BufferedStorageConfig, required
	BufferedStorageConfig BufferedStorageBackendConfig
	//DataStoreConfig, required
	DataStoreConfig datastore.DataStoreConfig
	// Log, optional, if nil uses go default logger
	Log *log.Entry
}

// PublishFromBufferedStorageBackend is asynchronous.
// Proceeds to create an internal instance of BufferedStorageBackend
// using provided configs and emit ledgers asynchronously to the provided
// callback fn for all ledgers in the requested range.
//
// ledgerRange - the requested range. If bounded range, will close resultCh
// after last ledger is emitted.
//
// publisherConfig - PublisherConfig. Provide configuration settings for DataStore
// and BufferedStorageBackend. Use DefaultBufferedStorageBackendConfig() to create
// optimized BufferedStorageBackendConfig.
//
// ctx - the context. Caller uses this to cancel the asynchronousledger processing.
// If caller does cancel, can sync on resultCh to receive an error to confirm
// all asynchronous processing stopped.
//
// callback - function. Invoked for every LedgerCloseMeta. If callback invocation
// returns an error, the publishing will shut down and indicate with error on resultCh.
//
// return - channel, used to signal to caller when publishing has stopped.
// If stoppage was due to an error, the error will be sent on
// channel and then closed. If no errors and ledgerRange is bounded,
// the channel will be closed when range is completed. If ledgerRange
// is unbounded, then the channel is never closed until an error
// or caller cancels.
func PublishFromBufferedStorageBackend(ledgerRange Range,
	publisherConfig PublisherConfig,
	ctx context.Context,
	callback func(xdr.LedgerCloseMeta) error) chan error {

	logger := publisherConfig.Log
	if logger == nil {
		logger = log.DefaultLogger
	}
	resultCh := make(chan error, 1)

	go func() {
		dataStore, err := datastoreFactory(ctx, publisherConfig.DataStoreConfig)
		if err != nil {
			resultCh <- fmt.Errorf("failed to create datastore: %w", err)
			return
		}

		var ledgerBackend LedgerBackend
		ledgerBackend, err = NewBufferedStorageBackend(publisherConfig.BufferedStorageConfig, dataStore)
		if err != nil {
			resultCh <- fmt.Errorf("failed to create buffered storage backend: %w", err)
			return
		}

		if publisherConfig.Registry != nil {
			ledgerBackend = WithMetrics(ledgerBackend, publisherConfig.Registry, publisherConfig.RegistryNamespace)
		}

		if ledgerRange.bounded && ledgerRange.to <= ledgerRange.from {
			resultCh <- errors.New("invalid end value for bounded range, must be greater than start")
			return
		}

		if !ledgerRange.bounded && ledgerRange.to > 0 {
			resultCh <- errors.New("invalid end value for unbounded ranged, must be zero")
			return
		}

		from := ordered.Max(2, ledgerRange.from)
		to := ledgerRange.to
		if !ledgerRange.bounded {
			to = math.MaxUint32
		}

		ledgerBackend.PrepareRange(ctx, ledgerRange)

		for ledgerSeq := from; ledgerSeq <= to; ledgerSeq++ {
			var ledgerCloseMeta xdr.LedgerCloseMeta

			logger.WithField("sequence", ledgerSeq).Info("Requesting ledger from the backend...")
			startTime := time.Now()
			ledgerCloseMeta, err = ledgerBackend.GetLedger(ctx, ledgerSeq)

			if err != nil {
				resultCh <- errors.Wrap(err, "error getting ledger")
				return
			}

			log.WithFields(log.F{
				"sequence": ledgerSeq,
				"duration": time.Since(startTime).Seconds(),
			}).Info("Ledger returned from the backend")

			err = callback(ledgerCloseMeta)
			if err != nil {
				resultCh <- errors.Wrap(err, "received an error from callback invocation")
				return
			}
		}
		close(resultCh)
	}()

	return resultCh
}

// GetLatestLedgerSequence returns the most recent ledger sequence number available in the buffer.
func (bsb *BufferedStorageBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	bsb.bsBackendLock.RLock()
	defer bsb.bsBackendLock.RUnlock()

	if bsb.closed {
		return 0, errors.New("BufferedStorageBackend is closed; cannot GetLatestLedgerSequence")
	}

	if bsb.prepared == nil {
		return 0, errors.New("BufferedStorageBackend must be prepared, call PrepareRange first")
	}

	latestSeq, err := bsb.ledgerBuffer.getLatestLedgerSequence()
	if err != nil {
		return 0, err
	}

	return latestSeq, nil
}

// getBatchForSequence checks if the requested sequence is in the cached batch.
// Otherwise will continuously load in the next LedgerCloseMetaBatch until found.
func (bsb *BufferedStorageBackend) getBatchForSequence(ctx context.Context, sequence uint32) error {
	// Sequence inside the current cached LedgerCloseMetaBatch
	if sequence >= uint32(bsb.lcmBatch.StartSequence) && sequence <= uint32(bsb.lcmBatch.EndSequence) {
		return nil
	}

	// Sequence is before the current LedgerCloseMetaBatch
	// Does not support retrieving LedgerCloseMeta before the current cached batch
	if sequence < uint32(bsb.lcmBatch.StartSequence) {
		return errors.New("requested sequence precedes current LedgerCloseMetaBatch")
	}

	// Sequence is beyond the current LedgerCloseMetaBatch
	var err error
	bsb.lcmBatch, err = bsb.ledgerBuffer.getFromLedgerQueue(ctx)
	if err != nil {
		return errors.Wrap(err, "failed getting next ledger batch from queue")
	}
	return nil
}

// nextExpectedSequence returns nextLedger (if currently set) or start of
// prepared range. Otherwise it returns 0.
// This is done because `nextLedger` is 0 between the moment Stellar-Core is
// started and streaming the first ledger (in such case we return first ledger
// in requested range).
func (bsb *BufferedStorageBackend) nextExpectedSequence() uint32 {
	if bsb.nextLedger == 0 && bsb.prepared != nil {
		return bsb.prepared.from
	}
	return bsb.nextLedger
}

// GetLedger returns the LedgerCloseMeta for the specified ledger sequence number
func (bsb *BufferedStorageBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	bsb.bsBackendLock.RLock()
	defer bsb.bsBackendLock.RUnlock()

	if bsb.closed {
		return xdr.LedgerCloseMeta{}, errors.New("BufferedStorageBackend is closed; cannot GetLedger")
	}

	if bsb.prepared == nil {
		return xdr.LedgerCloseMeta{}, errors.New("session is not prepared, call PrepareRange first")
	}

	if sequence < bsb.ledgerBuffer.ledgerRange.from {
		return xdr.LedgerCloseMeta{}, errors.New("requested sequence preceeds current LedgerRange")
	}

	if bsb.ledgerBuffer.ledgerRange.bounded {
		if sequence > bsb.ledgerBuffer.ledgerRange.to {
			return xdr.LedgerCloseMeta{}, errors.New("requested sequence beyond current LedgerRange")
		}
	}

	if sequence < bsb.lastLedger {
		return xdr.LedgerCloseMeta{}, errors.New("requested sequence preceeds the lastLedger")
	}

	if sequence > bsb.nextExpectedSequence() {
		return xdr.LedgerCloseMeta{}, errors.New("requested sequence is not the lastLedger nor the next available ledger")
	}

	err := bsb.getBatchForSequence(ctx, sequence)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	ledgerCloseMeta, err := bsb.lcmBatch.GetLedger(sequence)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}
	bsb.lastLedger = bsb.nextLedger
	bsb.nextLedger++

	return ledgerCloseMeta, nil
}

// PrepareRange checks if the starting and ending (if bounded) ledgers exist.
func (bsb *BufferedStorageBackend) PrepareRange(ctx context.Context, ledgerRange Range) error {
	bsb.bsBackendLock.Lock()
	defer bsb.bsBackendLock.Unlock()

	if bsb.closed {
		return errors.New("BufferedStorageBackend is closed; cannot PrepareRange")
	}

	if alreadyPrepared, err := bsb.startPreparingRange(ledgerRange); err != nil {
		return errors.Wrap(err, "error starting prepare range")
	} else if alreadyPrepared {
		return nil
	}

	bsb.prepared = &ledgerRange

	return nil
}

// IsPrepared returns true if a given ledgerRange is prepared.
func (bsb *BufferedStorageBackend) IsPrepared(ctx context.Context, ledgerRange Range) (bool, error) {
	bsb.bsBackendLock.RLock()
	defer bsb.bsBackendLock.RUnlock()

	if bsb.closed {
		return false, errors.New("BufferedStorageBackend is closed; cannot IsPrepared")
	}

	return bsb.isPrepared(ledgerRange), nil
}

func (bsb *BufferedStorageBackend) isPrepared(ledgerRange Range) bool {
	if bsb.closed {
		return false
	}

	if bsb.prepared == nil {
		return false
	}

	if bsb.ledgerBuffer.ledgerRange.from > ledgerRange.from {
		return false
	}

	if bsb.ledgerBuffer.ledgerRange.bounded && !ledgerRange.bounded {
		return false
	}

	if !bsb.ledgerBuffer.ledgerRange.bounded && !ledgerRange.bounded {
		return true
	}

	if !bsb.ledgerBuffer.ledgerRange.bounded && ledgerRange.bounded {
		return true
	}

	if bsb.ledgerBuffer.ledgerRange.to >= ledgerRange.to {
		return true
	}

	return false
}

// Close closes existing BufferedStorageBackend processes.
// Note, once a BufferedStorageBackend instance is closed it can no longer be used and
// all subsequent calls to PrepareRange(), GetLedger(), etc will fail.
// Close is thread-safe and can be called from another go routine.
func (bsb *BufferedStorageBackend) Close() error {
	bsb.bsBackendLock.RLock()
	defer bsb.bsBackendLock.RUnlock()

	if bsb.ledgerBuffer != nil {
		bsb.ledgerBuffer.close()
	}

	bsb.closed = true

	return nil
}

// startPreparingRange prepares the ledger range by setting the range in the ledgerBuffer
func (bsb *BufferedStorageBackend) startPreparingRange(ledgerRange Range) (bool, error) {
	if bsb.isPrepared(ledgerRange) {
		return true, nil
	}

	var err error
	bsb.ledgerBuffer, err = bsb.newLedgerBuffer(ledgerRange)
	if err != nil {
		return false, err
	}

	bsb.nextLedger = ledgerRange.from

	return false, nil
}
