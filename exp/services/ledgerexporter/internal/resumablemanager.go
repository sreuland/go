package ledgerexporter

import (
	"context"
)

type ResumableManager interface {
	FindFirstLedgerGapInRange(ctx context.Context, start, end uint32) uint32
}

type resumableManagerService struct {
	exporterConfig ExporterConfig
	dataStore      DataStore
	networkManager NetworkManager
	network        string
}

func NewResumableManager(dataStore DataStore, exporterConfig ExporterConfig, networkManager NetworkManager, network string) ResumableManager {
	return &resumableManagerService{exporterConfig: exporterConfig, dataStore: dataStore, networkManager: networkManager, network: network}
}

// find the nearest "LedgersPerFile" starting boundary ledger number relative to requested start which
// does NOT exist on datastore yet.
//
// start - start search from this ledger
// stop - stop search at this ledger
// return - non-zero if it was able to identify the nearest "LedgersPerFile" starting boundary ledger number
//
//	which is absent on datastore. It may be equal to start or great than start.
//	Returns 0 if not able to identify next boundary ledger.
func (rm resumableManagerService) FindFirstLedgerGapInRange(ctx context.Context, start, end uint32) uint32 {
	if ctx.Err() != nil {
		return 0
	}

	// streaming mode for start, no historical point to resume from
	if start < 1 {
		return 0
	}

	// streaming mode for end, get current ledger to use for a sane/bounded range on resume check
	if end < 1 {
		var latestErr error
		end, latestErr = rm.networkManager.GetLatestLedgerSequenceFromHistoryArchives(ctx, rm.network)
		if latestErr != nil {
			logger.WithError(latestErr).Infof("For resuming of export ledger range start=%d, end=%d, was not able to get latest ledger from network %v", start, end, rm.network)
			return 0
		}
	}

	binarySearchStart := start
	binarySearchStop := end
	nearestAbsentLedger := uint32(0)
	lookupCache := map[string]bool{}

	for binarySearchStart <= binarySearchStop {
		if ctx.Err() != nil {
			return 0
		}

		binarySearchMiddle := (binarySearchStop-binarySearchStart)/2 + binarySearchStart
		objectKeyMiddle := rm.exporterConfig.GetObjectKeyFromSequenceNumber(binarySearchMiddle)

		// there may be small occurrence of repeated queries on same object key once
		// search narrows down to a range that fits within the ledgers per file
		// worst case being 'log of ledgers_per_file' queries.
		middleFoundOnStore, foundInCache := lookupCache[objectKeyMiddle]
		if !foundInCache {
			var datastoreErr error
			middleFoundOnStore, datastoreErr = rm.dataStore.Exists(ctx, objectKeyMiddle)
			if datastoreErr != nil {
				logger.WithError(datastoreErr).Infof("For resuming of export ledger range start=%d, end=%d, was not able to check if objec key %v exists on data store", start, end, objectKeyMiddle)
				return 0
			}
			lookupCache[objectKeyMiddle] = middleFoundOnStore
		}

		if middleFoundOnStore {
			binarySearchStart = binarySearchMiddle + 1
		} else {
			nearestAbsentLedger = binarySearchMiddle
			binarySearchStop = binarySearchMiddle - 1
		}
	}
	return rm.exporterConfig.GetSequenceNumberStartBoundary(nearestAbsentLedger)
}
