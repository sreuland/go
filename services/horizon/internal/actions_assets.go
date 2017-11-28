package horizon

import (
	"fmt"
	"math"
	"net/url"
	"strconv"

	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/assets"
	"github.com/stellar/go/services/horizon/internal/render/hal"
	"github.com/stellar/go/services/horizon/internal/resource"
)

// This file contains the actions:
//
// AssetsAction: pages of assets

// AssetsAction renders a page of Assets
type AssetsAction struct {
	Action
	AssetCode    string
	AssetIssuer  string
	PagingParams db2.PageQuery
	Records      []assets.AssetStatsR
	Page         hal.Page
}

const maxAssetCodeLength = 12

// JSON is a method for actions.JSON
func (action *AssetsAction) JSON() {
	action.Do(
		action.loadParams,
		action.loadRecords,
		action.loadPage,
		func() {
			hal.Render(action.W, action.Page)
		},
	)
}

func (action *AssetsAction) loadParams() {
	action.AssetCode = action.GetString("asset_code")
	if len(action.AssetCode) > maxAssetCodeLength {
		action.SetInvalidField("asset_code", fmt.Errorf("max length is: %d", maxAssetCodeLength))
		return
	}

	if len(action.GetString("asset_issuer")) > 0 {
		issuerAccount := action.GetAccountID("asset_issuer")
		if action.Err != nil {
			return
		}
		action.AssetIssuer = issuerAccount.Address()
	}
	action.PagingParams = action.GetPageQuery()
}

// history_assets (id) is 32-bits but paging expects to query on an int64 so defaulting for now
func (action *AssetsAction) defaultDescendingCursor() {
	if action.PagingParams.Cursor == "" && action.PagingParams.Order == db2.OrderDescending {
		action.PagingParams.Cursor = strconv.FormatInt(math.MaxInt32, 10)
	}
}

func (action *AssetsAction) loadRecords() {
	action.defaultDescendingCursor()
	sql, err := assets.AssetStatsQ{
		AssetCode:   &action.AssetCode,
		AssetIssuer: &action.AssetIssuer,
		PageQuery:   &action.PagingParams,
	}.GetSQL()
	if err != nil {
		action.Err = err
		return
	}
	action.Err = action.HistoryQ().Select(&action.Records, sql)
}

func (action *AssetsAction) loadPage() {
	for _, record := range action.Records {
		var res resource.AssetStat
		res.Populate(action.Ctx, record)
		action.Page.Add(res)
	}

	action.Page.BaseURL = action.BaseURL()
	action.Page.BasePath = action.Path()
	action.Page.Limit = action.PagingParams.Limit
	action.Page.Cursor = action.PagingParams.Cursor
	action.Page.Order = action.PagingParams.Order

	linkParams := url.Values{}
	if action.AssetCode != "" {
		linkParams.Set("asset_code", action.AssetCode)
	}
	if action.AssetIssuer != "" {
		linkParams.Set("asset_issuer", action.AssetIssuer)
	}
	action.Page.PopulateLinksWithParams(linkParams)
}
