package ledgerexporter

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
)

type ResumableManager interface {
	// Find the closest ledger number to requested start but not greater which
	// does not exist on datastore yet.
	//
	// start - start search from this ledger, must be greater than 0.
	// end   - stop search at this ledger.
	//
	// If start=0, this means no starting point is resumability is skipped,
	//
	// If end=0, means unbounded, this will substitute an effective end value of
	// the network's latest checkpointed ledger + (2 * checkpoint_frequency),
	//
	// return:
	// resumableLedger   - if > 0, will be the next ledger that is not populated on data store.
	// dataStoreComplete - if true, there was no gaps on data store for bounded range requested
	// err               - when present, resumableLedger will be and dataStoreComplete will be false
	//
	// if no err and resumableLedger is 0 and dataStoreComplete is false, no resumability was applicable,
	// the datastore had no additional data to extend starting point.
	FindStart(ctx context.Context, start, end uint32) (resumableLedger uint32, dataStoreComplete bool, err error)
}

type resumableManagerService struct {
	network           string
	ledgerBatchConfig LedgerBatchConfig
	dataStore         DataStore
	archive           historyarchive.ArchiveInterface
}

func NewResumableManager(dataStore DataStore,
	network string,
	ledgerBatchConfig LedgerBatchConfig,
	archive historyarchive.ArchiveInterface) ResumableManager {
	return &resumableManagerService{
		ledgerBatchConfig: ledgerBatchConfig,
		network:           network,
		dataStore:         dataStore,
		archive:           archive,
	}
}

func (rm resumableManagerService) FindStart(ctx context.Context, start, end uint32) (resumableLedger uint32, dataStoreComplete bool, err error) {
	if start < 1 {
		return 0, false, errors.New("Invalid start value, must be greater than zero")
	}

	log := logger.WithField("start", start).WithField("end", end).WithField("network", rm.network)

	networkLatest := uint32(0)
	if end < 1 {
		var latestErr error
		networkLatest, latestErr = getLatestLedgerSequenceFromHistoryArchives(rm.archive)
		if latestErr != nil {
			err := errors.Wrap(latestErr, "Resumability of requested export ledger range, was not able to get latest ledger from network")
			log.WithError(err)
			return 0, false, err
		}
		logger.Infof("Resumability acquired latest archived network ledger =%d + for network=%v", networkLatest, rm.network)
		networkLatest = networkLatest + (getHistoryArchivesCheckPointFrequency() * 2)
		logger.Infof("Resumability computed effective latest network ledger including padding of checkpoint frequency to be %d + for network=%v", networkLatest, rm.network)

		if start > networkLatest {
			// requested to start at a point beyond the latest network, resume not applicable.
			return 0, false, errors.Errorf("Invalid start value of %v, it is greater than network's latest ledger of %v", start, networkLatest)
		}
	}

	binarySearchStop := max(end, networkLatest)
	binarySearchStart := start

	log.Infof("Resumability is searching datastore for next absent object key of requested export ledger range")

	rangeSize := max(int(binarySearchStop-binarySearchStart), 1)
	var binarySearchError error
	lowestAbsentIndex := sort.Search(rangeSize, binarySearchCallbackFn(&rm, ctx, binarySearchStart, binarySearchStop, &binarySearchError))
	if binarySearchError != nil {
		return 0, false, binarySearchError
	}

	if lowestAbsentIndex < 1 {
		// data store had no data within search range
		return 0, false, nil
	}

	if lowestAbsentIndex < int(rangeSize) {
		nearestAbsentLedgerSequence := binarySearchStart + uint32(lowestAbsentIndex)
		log.Infof("Resumability determined next absent object start key of %d for requested export ledger range", nearestAbsentLedgerSequence)
		return nearestAbsentLedgerSequence, false, nil
	}

	// unbounded, and datastore had up to latest network, return that as staring point.
	if networkLatest > 0 {
		return networkLatest, false, nil
	}

	// data store had all ledgers for requested range, no resumability needed.
	log.Infof("Resumability found no absent object keys in requested ledger range")
	return 0, true, nil
}

func binarySearchCallbackFn(rm *resumableManagerService, ctx context.Context, start, end uint32, binarySearchError *error) func(ledgerSequence int) bool {
	lookupCache := map[string]bool{}

	return func(binarySearchIndex int) bool {
		if *binarySearchError != nil {
			// an error has already occured in a callback for the same binary search, exiting
			return true
		}
		objectKeyMiddle := rm.ledgerBatchConfig.GetObjectKeyFromSequenceNumber(start + uint32(binarySearchIndex))

		// there may be small occurrence of repeated queries on same object key once
		// search narrows down to a range that fits within the ledgers per file
		// worst case being 'log of ledgers_per_file' queries.
		middleFoundOnStore, foundInCache := lookupCache[objectKeyMiddle]
		if !foundInCache {
			var datastoreErr error
			middleFoundOnStore, datastoreErr = rm.dataStore.Exists(ctx, objectKeyMiddle)
			if datastoreErr != nil {
				*binarySearchError = errors.Wrapf(datastoreErr, "While searching datastore for resumability within export ledger range start=%d, end=%d, was not able to check if object key %v exists on data store", start, end, objectKeyMiddle)
				return true
			}
			lookupCache[objectKeyMiddle] = middleFoundOnStore
		}
		return !middleFoundOnStore
	}
}
