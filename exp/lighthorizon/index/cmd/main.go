package main

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stellar/go/exp/lighthorizon/index"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

var (
	mutex   sync.RWMutex
	indexes = map[string]*index.CheckpointIndex{}

	parallel  = uint32(20)
	s3Session *session.Session

	downloader *s3manager.Downloader
)

func main() {
	log.SetLevel(log.InfoLevel)

	var err error
	s3Session, err = session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		panic(err)
	}

	downloader = s3manager.NewDownloader(s3Session)

	historyArchive, err := historyarchive.Connect(
		// "file:///Users/Bartek/archive",
		"s3://history.stellar.org/prd/core-live/core_live_001",
		historyarchive.ConnectOptions{
			NetworkPassphrase: network.PublicNetworkPassphrase,
			S3Region:          "eu-west-1",
			UnsignedRequests:  false,
		},
	)
	if err != nil {
		panic(err)
	}

	startTime := time.Now()

	startCheckpoint := uint32(0) //uint32((39680056) / 64)
	endCheckpoint := uint32((39685056) / 64)
	all := endCheckpoint - startCheckpoint

	var wg sync.WaitGroup

	ch := make(chan uint32, parallel)

	go func() {
		for i := startCheckpoint; i <= endCheckpoint; i++ {
			ch <- i
		}
		close(ch)
	}()

	processed := uint64(0)
	for i := uint32(0); i < parallel; i++ {
		wg.Add(1)
		go func(i uint32) {
			for {
				checkpoint, ok := <-ch
				if !ok {
					wg.Done()
					return
				}

				startLedger := checkpoint * 64
				if startLedger == 0 {
					startLedger = 1
				}
				endLedger := checkpoint*64 - 1 + 64

				// fmt.Println("Processing checkpoint", checkpoint, "ledgers", startLedger, endLedger)

				ledgers, err := historyArchive.GetLedgers(startLedger, endLedger)
				if err != nil {
					log.WithField("error", err).Error("error getting ledgers")
					ch <- checkpoint
					continue
				}

				for i := startLedger; i <= endLedger; i++ {
					ledger, ok := ledgers[i]
					if !ok {
						panic(fmt.Sprintf("no ledger %d", i))
					}

					resultMeta := make([]xdr.TransactionResultMeta, len(ledger.TransactionResult.TxResultSet.Results))
					for i, result := range ledger.TransactionResult.TxResultSet.Results {
						resultMeta[i].Result = result
					}

					closeMeta := xdr.LedgerCloseMeta{
						V0: &xdr.LedgerCloseMetaV0{
							LedgerHeader: ledger.Header,
							TxSet:        ledger.Transaction.TxSet,
							TxProcessing: resultMeta,
						},
					}

					reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, closeMeta)
					if err != nil {
						panic(err)
					}

					for {
						tx, err := reader.Read()
						if err != nil {
							if err == io.EOF {
								break
							}
							panic(err)
						}

						allParticipants, err := participantsForOperations(tx, false)
						if err != nil {
							panic(err)
						}

						addParticipantsToIndexes(checkpoint, "%s_all_all", allParticipants)

						paymentsParticipants, err := participantsForOperations(tx, true)
						if err != nil {
							panic(err)
						}

						addParticipantsToIndexes(checkpoint, "%s_all_payments", paymentsParticipants)

						if tx.Result.Successful() {
							allParticipants, err := participantsForOperations(tx, false)
							if err != nil {
								panic(err)
							}

							addParticipantsToIndexes(checkpoint, "%s_successful_all", allParticipants)

							paymentsParticipants, err := participantsForOperations(tx, true)
							if err != nil {
								panic(err)
							}

							addParticipantsToIndexes(checkpoint, "%s_successful_payments", paymentsParticipants)
						}
					}
				}

				nprocessed := atomic.AddUint64(&processed, 1)

				if nprocessed%100 == 0 {
					log.Infof(
						"Reading checkpoints... - %.2f%% - time elapsed: %s",
						(float64(nprocessed)/float64(all))*100,
						time.Since(startTime),
					)

					// Clear indexes to save memory
					mutex.Lock()
					uploadIndexes()
					indexes = map[string]*index.CheckpointIndex{}
					mutex.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	uploadIndexes()
}

func uploadIndexes() {
	var wg sync.WaitGroup

	type upload struct {
		id    string
		index *index.CheckpointIndex
	}

	uch := make(chan upload, parallel)

	go func() {
		for id, index := range indexes {
			uch <- upload{
				id:    id,
				index: index,
			}
		}
		close(uch)
	}()

	uploader := s3manager.NewUploader(s3Session)

	written := uint64(0)
	for i := uint32(0); i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				u, ok := <-uch
				if !ok {
					return
				}

				_, err := uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String("horizon-index"),
					Key:    aws.String(u.id),
					Body:   u.index.Buffer(),
				})
				if err != nil {
					log.Errorf("Unable to upload %s, %v", u.id, err)
					uch <- u
					continue
				}

				nwritten := atomic.AddUint64(&written, 1)
				if nwritten%1000 == 0 {
					log.Infof("Writing indexes... %d/%d %.2f%%", nwritten, len(indexes), (float64(nwritten)/float64(len(indexes)))*100)
				}
			}
		}()
	}

	wg.Wait()
}

func addParticipantsToIndexes(checkpoint uint32, indexFormat string, participants []string) {
	for _, participant := range participants {
		ind := getCreateIndex(fmt.Sprintf(indexFormat, participant))
		err := ind.SetActive(checkpoint)
		if err != nil {
			panic(err)
		}
	}
}

func getCreateIndex(id string) *index.CheckpointIndex {
	mutex.Lock()
	defer mutex.Unlock()

	ind, ok := indexes[id]
	if !ok {
		// Check if index exists in S3
		b := &aws.WriteAtBuffer{}
		_, err := downloader.Download(b, &s3.GetObjectInput{
			Bucket: aws.String("horizon-index"),
			Key:    aws.String(id),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == s3.ErrCodeNoSuchKey {
					ind = &index.CheckpointIndex{}
				} else {
					panic(err)
				}
			} else {
				panic(err)
			}
		} else {
			ind, err = index.NewCheckpointIndexFromBytes(b.Bytes())
			if err != nil {
				panic(err)
			}
		}

		indexes[id] = ind
	}
	return ind
}

func participantsForOperations(transaction ingest.LedgerTransaction, onlyPayments bool) ([]string, error) {
	var participants []string

	for opindex, operation := range transaction.Envelope.Operations() {
		opSource := operation.SourceAccount
		if opSource == nil {
			txSource := transaction.Envelope.SourceAccount()
			opSource = &txSource
		}

		switch operation.Body.Type {
		case xdr.OperationTypeCreateAccount,
			xdr.OperationTypePayment,
			xdr.OperationTypePathPaymentStrictReceive,
			xdr.OperationTypePathPaymentStrictSend,
			xdr.OperationTypeAccountMerge:
			participants = append(participants, opSource.Address())
		default:
			if onlyPayments {
				continue
			}
			participants = append(participants, opSource.Address())
		}

		switch operation.Body.Type {
		case xdr.OperationTypeCreateAccount:
			participants = append(participants, operation.Body.MustCreateAccountOp().Destination.Address())
		case xdr.OperationTypePayment:
			participants = append(participants, operation.Body.MustPaymentOp().Destination.ToAccountId().Address())
		case xdr.OperationTypePathPaymentStrictReceive:
			participants = append(participants, operation.Body.MustPathPaymentStrictReceiveOp().Destination.ToAccountId().Address())
		case xdr.OperationTypePathPaymentStrictSend:
			participants = append(participants, operation.Body.MustPathPaymentStrictSendOp().Destination.ToAccountId().Address())
		case xdr.OperationTypeManageBuyOffer:
			// the only direct participant is the source_account
		case xdr.OperationTypeManageSellOffer:
			// the only direct participant is the source_account
		case xdr.OperationTypeCreatePassiveSellOffer:
			// the only direct participant is the source_account
		case xdr.OperationTypeSetOptions:
			// the only direct participant is the source_account
		case xdr.OperationTypeChangeTrust:
			// the only direct participant is the source_account
		case xdr.OperationTypeAllowTrust:
			participants = append(participants, operation.Body.MustAllowTrustOp().Trustor.Address())
		case xdr.OperationTypeAccountMerge:
			participants = append(participants, operation.Body.MustDestination().ToAccountId().Address())
		case xdr.OperationTypeInflation:
			// the only direct participant is the source_account
		case xdr.OperationTypeManageData:
			// the only direct participant is the source_account
		case xdr.OperationTypeBumpSequence:
			// the only direct participant is the source_account
		case xdr.OperationTypeCreateClaimableBalance:
			for _, c := range operation.Body.MustCreateClaimableBalanceOp().Claimants {
				participants = append(participants, c.MustV0().Destination.Address())
			}
		case xdr.OperationTypeClaimClaimableBalance:
			// the only direct participant is the source_account
		case xdr.OperationTypeBeginSponsoringFutureReserves:
			participants = append(participants, operation.Body.MustBeginSponsoringFutureReservesOp().SponsoredId.Address())
		case xdr.OperationTypeEndSponsoringFutureReserves:
			// Failed transactions may not have a compliant sandwich structure
			// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
			// and thus we bail out since we could return incorrect information.
			if transaction.Result.Successful() {
				sponsoree := transaction.Envelope.SourceAccount().ToAccountId().Address()
				if operation.SourceAccount != nil {
					sponsoree = operation.SourceAccount.Address()
				}
				operations := transaction.Envelope.Operations()
				for i := int(opindex) - 1; i >= 0; i-- {
					if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
						beginOp.SponsoredId.Address() == sponsoree {
						participants = append(participants, beginOp.SponsoredId.Address())
					}
				}
			}
		case xdr.OperationTypeRevokeSponsorship:
			op := operation.Body.MustRevokeSponsorshipOp()
			switch op.Type {
			case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
				participants = append(participants, getLedgerKeyParticipants(*op.LedgerKey)...)
			case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
				participants = append(participants, op.Signer.AccountId.Address())
				// We don't add signer as a participant because a signer can be arbitrary account.
				// This can spam successful operations history of any account.
			}
		case xdr.OperationTypeClawback:
			op := operation.Body.MustClawbackOp()
			participants = append(participants, op.From.ToAccountId().Address())
		case xdr.OperationTypeClawbackClaimableBalance:
			// the only direct participant is the source_account
		case xdr.OperationTypeSetTrustLineFlags:
			op := operation.Body.MustSetTrustLineFlagsOp()
			participants = append(participants, op.Trustor.Address())
		case xdr.OperationTypeLiquidityPoolDeposit:
			// the only direct participant is the source_account
		case xdr.OperationTypeLiquidityPoolWithdraw:
			// the only direct participant is the source_account
		default:
			return nil, fmt.Errorf("unknown operation type: %s", operation.Body.Type)
		}

		// Requires meta
		// sponsor, err := operation.getSponsor()
		// if err != nil {
		// 	return nil, err
		// }
		// if sponsor != nil {
		// 	otherParticipants = append(otherParticipants, *sponsor)
		// }
	}

	return participants, nil
}

func getLedgerKeyParticipants(ledgerKey xdr.LedgerKey) []string {
	var result []string
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result = append(result, ledgerKey.Account.AccountId.Address())
	case xdr.LedgerEntryTypeClaimableBalance:
		// nothing to do
	case xdr.LedgerEntryTypeData:
		result = append(result, ledgerKey.Data.AccountId.Address())
	case xdr.LedgerEntryTypeOffer:
		result = append(result, ledgerKey.Offer.SellerId.Address())
	case xdr.LedgerEntryTypeTrustline:
		result = append(result, ledgerKey.TrustLine.AccountId.Address())
	}
	return result
}
