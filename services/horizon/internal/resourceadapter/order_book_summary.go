package resourceadapter

import (
	"context"

	. "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/horizon/internal/db2/core"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

func PopulateOrderBookSummary(
	ctx context.Context,
	dest *OrderBookSummary,
	selling xdr.Asset,
	buying xdr.Asset,
	row core.OrderBookSummary,
) error {

	err := PopulateAsset(ctx, &dest.Selling, selling)
	if err != nil {
		return err
	}
	err = PopulateAsset(ctx, &dest.Buying, buying)
	if err != nil {
		return err
	}

	err = populatePriceLevels(&dest.Bids, row.Bids())
	if err != nil {
		return err
	}
	err = populatePriceLevels(&dest.Asks, row.Asks())
	if err != nil {
		return err
	}

	return nil
}

func populatePriceLevels(destp *[]PriceLevel, rows []core.OrderBookSummaryPriceLevel) error {
	*destp = make([]PriceLevel, len(rows))
	dest := *destp

	for i, row := range rows {
		amount, err := row.AmountAsString()
		if err != nil {
			return errors.Wrap(err, "Error converting PriceLevel.Amount: "+row.Amount)
		}
		dest[i] = PriceLevel{
			Price:  row.PriceAsString(),
			Amount: amount,
			PriceR: Price{
				N: row.Pricen,
				D: row.Priced,
			},
		}
	}

	return nil
}
