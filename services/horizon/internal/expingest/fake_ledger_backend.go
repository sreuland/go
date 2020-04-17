package expingest

import (
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	logpkg "github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

type fakeLedgerBackend struct {
	numTransactions       int
	changesPerTransaction int
}

func (fakeLedgerBackend) GetLatestLedgerSequence() (uint32, error) {
	return 1, nil
}

func fakeAccount() xdr.LedgerEntryChange {
	account := keypair.MustRandom().Address()
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: xdr.MustAddress(account),
					Balance:   xdr.Int64(100),
				},
			},
		},
	}
}

func fakeAccountData() xdr.LedgerEntryChange {
	account := keypair.MustRandom().Address()
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeData,
				Data: &xdr.DataEntry{
					AccountId: xdr.MustAddress(account),
					DataName:  "test-name",
					DataValue: xdr.DataValue("test"),
				},
			},
		},
	}
}

func fakeTrustline() xdr.LedgerEntryChange {
	account := keypair.MustRandom().Address()
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: xdr.MustAddress(account),
					Balance:   123,
					Asset:     xdr.MustNewCreditAsset("usd", account),
				},
			},
		},
	}
}

func fakeOffer(offerID int64) xdr.LedgerEntryChange {
	account := keypair.MustRandom().Address()
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeOffer,
				Offer: &xdr.OfferEntry{
					SellerId: xdr.MustAddress(account),
					OfferId:  xdr.Int64(offerID),
					Amount:   213,
					Buying:   xdr.MustNewCreditAsset("usd", account),
					Price:    xdr.Price{N: 1, D: 1},
					Selling:  xdr.MustNewCreditAsset("eur", account),
				},
			},
		},
	}
}

func (f fakeLedgerBackend) GetLedger(sequence uint32) (bool, ledgerbackend.LedgerCloseMeta, error) {
	ledgerCloseMeta := ledgerbackend.LedgerCloseMeta{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Hash: xdr.Hash{1, 2, 3, 4, 5, 6},
			Header: xdr.LedgerHeader{
				LedgerVersion: 7,
				LedgerSeq:     xdr.Uint32(1),
			},
		},
		TransactionResult:     make([]xdr.TransactionResultPair, f.numTransactions),
		TransactionEnvelope:   make([]xdr.TransactionEnvelope, f.numTransactions),
		TransactionMeta:       make([]xdr.TransactionMeta, f.numTransactions),
		TransactionFeeChanges: make([]xdr.LedgerEntryChanges, f.numTransactions),
		UpgradesMeta:          []xdr.LedgerEntryChanges{},
	}

	logger := log.WithField("sequence", sequence)
	logger.Info("Creating fake ledger")
	var offers, trustlines, accounts, accountData, total int

	for i := 0; i < f.numTransactions; i++ {
		var results []xdr.OperationResult
		ledgerCloseMeta.TransactionResult[i] = xdr.TransactionResultPair{
			TransactionHash: xdr.Hash{1, byte(i % 256), byte((i / 256) % 256)},
			Result: xdr.TransactionResult{
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: &results,
				},
			},
		}
		ledgerCloseMeta.TransactionEnvelope[i] = xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAccountAddress("GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A"),
				},
			},
		}
		ledgerCloseMeta.TransactionMeta[i] = xdr.TransactionMeta{
			V: 1,
			V1: &xdr.TransactionMetaV1{
				Operations: []xdr.OperationMeta{},
			},
		}

		feeChanges := xdr.LedgerEntryChanges{}
		for j := 0; j < f.changesPerTransaction; j++ {
			var change xdr.LedgerEntryChange
			switch total % 4 {
			case 0:
				change = fakeAccount()
				accounts++
			case 1:
				change = fakeAccountData()
				accountData++
			case 2:
				offers++
				change = fakeOffer(int64(offers))
			case 3:
				change = fakeTrustline()
				trustlines++
			}
			total++
			feeChanges = append(feeChanges, change)

			if total%logFrequency == 0 {
				curHeap, sysHeap := getMemStats()
				logger.WithFields(logpkg.F{
					"currentHeapSizeMB": curHeap,
					"systemHeapSizeMB":  sysHeap,
					"accounts":          accounts,
					"offfers":           offers,
					"trustlines":        trustlines,
					"accountData":       accountData,
					"totalChanges":      total,
				}).Info("Adding changes to fake ledger")
			}
		}

		ledgerCloseMeta.TransactionFeeChanges[i] = feeChanges
	}

	curHeap, sysHeap := getMemStats()
	logger.WithFields(logpkg.F{
		"currentHeapSizeMB": curHeap,
		"systemHeapSizeMB":  sysHeap,
		"accounts":          accounts,
		"offfers":           offers,
		"trustlines":        trustlines,
		"accountData":       accountData,
		"totalChanges":      total,
	}).Info("Finished creating fake ledger")
	return true, ledgerCloseMeta, nil
}

func (fakeLedgerBackend) Close() error {
	return nil
}
