package ticker

import (
	"github.com/stellar/go/exp/ticker/internal/gql"
	"github.com/stellar/go/exp/ticker/internal/tickerdb"
	hlog "github.com/stellar/go/support/log"
)

func StartGraphQLServer(s *tickerdb.TickerSession, l *hlog.Entry, port string) {
	graphql := gql.New(s, l)

	graphql.Serve(port)
}
