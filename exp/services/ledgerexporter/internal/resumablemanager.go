package ledgerexporter

import (
	"context"
	"sort"

	"github.com/stellar/go/historyarchive"
)

type ResumableManager interface {
	// Find the closest ledger number to requested start but not greater which
	// does not exist on datastore yet.
	//
	// start - start search from this ledger
	// end   - stop search at this ledger.
	//
	// If end=0, meaning unbounded, this will substitute an effective end value of the
	// most recent archived ledger number.
	//
	// return:
	// resumableLedger - if > 0, will be the next ledger that is not populated on data store.
	// dataStoreComplete - if true, there was no gaps on data store for bounded range requested
	//
	// if resumableLedger is 0 and dataStoreComplete is false, no resumability was applicable.
	FindStart(ctx context.Context, start, end uint32) (resumableLedger uint32, dataStoreComplete bool)
}

type resumableManagerService struct {
	config    *Config
	dataStore DataStore
	archive   historyarchive.ArchiveInterface
}

func NewResumableManager(dataStore DataStore, config *Config, archive historyarchive.ArchiveInterface) ResumableManager {
	return &resumableManagerService{
		config:    config,
		dataStore: dataStore,
		archive:   archive,
	}
}

func (rm resumableManagerService) FindStart(ctx context.Context, start, end uint32) (resumableLedger uint32, dataStoreComplete bool) {
	// start < 1 means streaming mode, no historical point to resume from
	if start < 1 || !rm.config.Resume {
		return 0, false
	}

	log := logger.WithField("start", start).WithField("end", end).WithField("network", rm.config.Network)

	// streaming mode for end, get latest network ledger to use for a sane bounded range during resumability check
	// this will assume a padding of network latest = network latest + 2 checkpoint_frequency,
	// since the latest network will be some number of ledgers past the last archive checkpoint
	// this lets the search be a little more greedy on finding a potential empty object key towards the end of range on data store.
	networkLatest := uint32(0)
	if end < 1 {
		var latestErr error
		networkLatest, latestErr = GetLatestLedgerSequenceFromHistoryArchives(ctx, rm.archive)
		if latestErr != nil {
			log.WithError(latestErr).Errorf("Resumability of requested export ledger range, was not able to get latest ledger from network")
			return 0, false
		}
		logger.Infof("Resumability acquired latest archived network ledger =%d + for network=%v", networkLatest, rm.config.Network)
		networkLatest = networkLatest + (GetHistoryArchivesCheckPointFrequency() * 2)
		logger.Infof("Resumability computed effective latest network ledger including padding of checkpoint frequency to be %d + for network=%v", networkLatest, rm.config.Network)

		if start > networkLatest {
			// requested to start at a point beyond the latest network, resume not applicable.
			return 0, false
		}
	}

	binarySearchStop := max(end, networkLatest)
	binarySearchStart := start

	log.Infof("Resumability is searching datastore for next absent object key of requested export ledger range")

	rangeSize := max(int(binarySearchStop-binarySearchStart), 1)
	lowestAbsentIndex := sort.Search(rangeSize, binarySearchCallbackFn(&rm, ctx, binarySearchStart, binarySearchStop))
	if lowestAbsentIndex < 1 {
		// data store had no data within search range
		return 0, false
	}

	if lowestAbsentIndex < int(rangeSize) {
		nearestAbsentLedgerSequence := binarySearchStart + uint32(lowestAbsentIndex)
		log.Infof("Resumability determined next absent object start key of %d for requested export ledger range", nearestAbsentLedgerSequence)
		return nearestAbsentLedgerSequence, false
	}

	// unbounded, and datastore had up to latest network, return that as staring point.
	if networkLatest > 0 {
		return networkLatest, false
	}

	// data store had all ledgers for requested range, no resumability needed.
	log.Infof("Resumability found no absent object keys in requested ledger range")
	return 0, true
}

func binarySearchCallbackFn(rm *resumableManagerService, ctx context.Context, start, end uint32) func(ledgerSequence int) bool {
	lookupCache := map[string]bool{}

	return func(binarySearchIndex int) bool {
		objectKeyMiddle := rm.config.LedgerBatchConfig.GetObjectKeyFromSequenceNumber(start + uint32(binarySearchIndex))

		// there may be small occurrence of repeated queries on same object key once
		// search narrows down to a range that fits within the ledgers per file
		// worst case being 'log of ledgers_per_file' queries.
		middleFoundOnStore, foundInCache := lookupCache[objectKeyMiddle]
		if !foundInCache {
			var datastoreErr error
			middleFoundOnStore, datastoreErr = rm.dataStore.Exists(ctx, objectKeyMiddle)
			if datastoreErr != nil {
				logger.WithError(datastoreErr).Infof("While searching datastore for resumability within export ledger range start=%d, end=%d, was not able to check if object key %v exists on data store", start, end, objectKeyMiddle)
				return false
			}
			lookupCache[objectKeyMiddle] = middleFoundOnStore
		}
		return !middleFoundOnStore
	}
}
