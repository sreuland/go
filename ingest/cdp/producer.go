package cdp

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/ordered"
	"github.com/stellar/go/xdr"
)

// provide testing hooks to inject mocks of these
var datastoreFactory = datastore.NewDataStore

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
func DefaultBufferedStorageBackendConfig(ledgersPerFile uint32) ledgerbackend.BufferedStorageBackendConfig {

	config := ledgerbackend.BufferedStorageBackendConfig{
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

type PublisherConfig struct {
	// Registry, optional, include to capture buffered storage backend metrics
	Registry *prometheus.Registry
	// RegistryNamespace, optional, include to emit buffered storage backend
	// under this namespace
	RegistryNamespace string
	// BufferedStorageConfig, required
	BufferedStorageConfig ledgerbackend.BufferedStorageBackendConfig
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
func PublishFromBufferedStorageBackend(ledgerRange ledgerbackend.Range,
	publisherConfig PublisherConfig,
	ctx context.Context,
	callback func(xdr.LedgerCloseMeta) error) chan error {

	logger := publisherConfig.Log
	if logger == nil {
		logger = log.DefaultLogger
	}
	resultCh := make(chan error, 1)

	go func() {
		defer close(resultCh)
		dataStore, err := datastoreFactory(ctx, publisherConfig.DataStoreConfig)
		if err != nil {
			resultCh <- fmt.Errorf("failed to create datastore: %w", err)
			return
		}

		var ledgerBackend ledgerbackend.LedgerBackend
		ledgerBackend, err = ledgerbackend.NewBufferedStorageBackend(publisherConfig.BufferedStorageConfig, dataStore)
		if err != nil {
			resultCh <- fmt.Errorf("failed to create buffered storage backend: %w", err)
			return
		}

		if publisherConfig.Registry != nil {
			ledgerBackend = ledgerbackend.WithMetrics(ledgerBackend, publisherConfig.Registry, publisherConfig.RegistryNamespace)
		}

		if ledgerRange.Bounded() && ledgerRange.To() <= ledgerRange.From() {
			resultCh <- fmt.Errorf("invalid end value for bounded range, must be greater than start")
			return
		}

		if !ledgerRange.Bounded() && ledgerRange.To() > 0 {
			resultCh <- fmt.Errorf("invalid end value for unbounded range, must be zero")
			return
		}

		from := ordered.Max(2, ledgerRange.From())
		to := ledgerRange.To()
		if !ledgerRange.Bounded() {
			to = math.MaxUint32
		}

		ledgerBackend.PrepareRange(ctx, ledgerRange)

		for ledgerSeq := from; ledgerSeq <= to; ledgerSeq++ {
			var ledgerCloseMeta xdr.LedgerCloseMeta

			logger.WithField("sequence", ledgerSeq).Info("Requesting ledger from the backend...")
			startTime := time.Now()
			ledgerCloseMeta, err = ledgerBackend.GetLedger(ctx, ledgerSeq)

			if err != nil {
				resultCh <- fmt.Errorf("error getting ledger, %w", err)
				return
			}

			log.WithFields(log.F{
				"sequence": ledgerSeq,
				"duration": time.Since(startTime).Seconds(),
			}).Info("Ledger returned from the backend")

			err = callback(ledgerCloseMeta)
			if err != nil {
				resultCh <- fmt.Errorf("received an error from callback invocation: %w", err)
				return
			}
		}
	}()

	return resultCh
}
