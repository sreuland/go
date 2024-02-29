package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

type CtxKey string

var RouteContextKey = CtxKey("route")
var QueryTypeContextKey = CtxKey("query_type")

type Subservice string

var CoreSubservice = Subservice("core")
var HistoryPrimarySubservice = Subservice("history_primary")
var HistorySubservice = Subservice("history")
var IngestSubservice = Subservice("ingest")

type QueryType string

var AdvisoryLockQueryType = QueryType("advisory_lock")
var DeleteQueryType = QueryType("delete")
var InsertQueryType = QueryType("insert")
var SelectQueryType = QueryType("select")
var UndefinedQueryType = QueryType("undefined")
var UpdateQueryType = QueryType("update")
var UpsertQueryType = QueryType("upsert")

// contextRoute returns a string representing the request endpoint, or "undefined" if it wasn't found
func contextRoute(ctx context.Context) string {
	if endpoint, ok := ctx.Value(&RouteContextKey).(string); ok {
		return endpoint
	}
	return "undefined"
}

type SessionWithMetrics struct {
	SessionInterface

	registry                 *prometheus.Registry
	queryCounter             *prometheus.CounterVec
	queryDurationSummary     *prometheus.SummaryVec
	maxOpenConnectionsGauge  prometheus.GaugeFunc
	openConnectionsGauge     prometheus.GaugeFunc
	inUseConnectionsGauge    prometheus.GaugeFunc
	idleConnectionsGauge     prometheus.GaugeFunc
	waitCountCounter         prometheus.CounterFunc
	waitDurationCounter      prometheus.CounterFunc
	maxIdleClosedCounter     prometheus.CounterFunc
	maxIdleTimeClosedCounter prometheus.CounterFunc
	maxLifetimeClosedCounter prometheus.CounterFunc
	roundTripProbe           *roundTripProbe
	roundTripTimeSummary     prometheus.Summary
	abendCounter             *prometheus.CounterVec
}

func RegisterMetrics(base *Session, namespace string, sub Subservice, registry *prometheus.Registry) SessionInterface {
	s := &SessionWithMetrics{
		SessionInterface: base,
		registry:         registry,
	}

	base.AddErrorHandler(s.handleErrorEvent)

	s.queryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "query_total",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		[]string{"query_type", "error", "route"},
	)
	registry.MustRegister(s.queryCounter)

	s.queryDurationSummary = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace, Subsystem: "db",
			Name:        "query_duration_seconds",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"query_type", "error", "route"},
	)
	registry.MustRegister(s.queryDurationSummary)

	// txnCounter: prometheus.NewCounter(
	// 	prometheus.CounterOpts{Namespace: namespace, Subsystem: "db", Name: "transaction_total"},
	// ),
	// registry.MustRegister(s.txnCounter)
	// txnDuration: prometheus.NewHistogram(
	// 	prometheus.HistogramOpts{
	// 		Namespace: namespace, Subsystem: "db",
	// 		Name:    "transaction_duration_seconds",
	//		Buckets: prometheus.ExponentialBuckets(0.1, 3, 5),
	// 	},
	// ),
	// registry.MustRegister(s.txnDuration)

	s.maxOpenConnectionsGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "max_open_connections",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			// Right now MaxOpenConnections in Horizon is static however it's possible that
			// it will change one day. In such case, using GaugeFunc is very cheap and will
			// prevent issues with this metric in the future.
			return float64(base.DB.Stats().MaxOpenConnections)
		},
	)
	registry.MustRegister(s.maxOpenConnectionsGauge)

	s.openConnectionsGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "open_connections",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().OpenConnections)
		},
	)
	registry.MustRegister(s.openConnectionsGauge)

	s.inUseConnectionsGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "in_use_connections",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().InUse)
		},
	)
	registry.MustRegister(s.inUseConnectionsGauge)

	s.idleConnectionsGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "idle_connections",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().Idle)
		},
	)
	registry.MustRegister(s.idleConnectionsGauge)

	s.waitCountCounter = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "wait_count_total",
			Help:        "total number of number of connections waited for",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().WaitCount)
		},
	)
	registry.MustRegister(s.waitCountCounter)

	s.waitDurationCounter = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "wait_duration_seconds_total",
			Help:        "total time blocked waiting for a new connection",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return base.DB.Stats().WaitDuration.Seconds()
		},
	)
	registry.MustRegister(s.waitDurationCounter)

	s.maxIdleClosedCounter = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "max_idle_closed_total",
			Help:        "total number of number of connections closed due to SetMaxIdleConns",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().MaxIdleClosed)
		},
	)
	registry.MustRegister(s.maxIdleClosedCounter)

	s.maxIdleTimeClosedCounter = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "max_idle_time_closed_total",
			Help:        "total number of number of connections closed due to SetConnMaxIdleTime",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().MaxIdleTimeClosed)
		},
	)
	registry.MustRegister(s.maxIdleTimeClosedCounter)

	s.maxLifetimeClosedCounter = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "max_lifetime_closed_total",
			Help:        "total number of number of connections closed due to SetConnMaxLifetime",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		func() float64 {
			return float64(base.DB.Stats().MaxLifetimeClosed)
		},
	)
	registry.MustRegister(s.maxLifetimeClosedCounter)

	s.abendCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "abend_total",
			Help:        "total number of abends, details are captured in labels",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
		},
		[]string{"origin", "condition", "type"},
	)
	registry.MustRegister(s.abendCounter)

	s.roundTripTimeSummary = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   "db",
			Name:        "round_trip_time_seconds",
			Help:        "time required to run `select 1` query in a DB - effectively measures round trip time, if time exceeds 1s it will be recorded as 1",
			ConstLabels: prometheus.Labels{"subservice": string(sub)},
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
	)
	registry.MustRegister(s.roundTripTimeSummary)

	s.roundTripProbe = &roundTripProbe{
		session:              base,
		roundTripTimeSummary: s.roundTripTimeSummary,
	}
	s.roundTripProbe.start()
	return s
}

func (s *SessionWithMetrics) Close() error {
	s.roundTripProbe.close()

	s.registry.Unregister(s.queryCounter)
	s.registry.Unregister(s.queryDurationSummary)
	// s.registry.Unregister(s.txnCounter)
	// s.registry.Unregister(s.txnDurationSummary)
	s.registry.Unregister(s.maxOpenConnectionsGauge)
	s.registry.Unregister(s.openConnectionsGauge)
	s.registry.Unregister(s.inUseConnectionsGauge)
	s.registry.Unregister(s.idleConnectionsGauge)
	s.registry.Unregister(s.waitCountCounter)
	s.registry.Unregister(s.waitDurationCounter)
	s.registry.Unregister(s.maxIdleClosedCounter)
	s.registry.Unregister(s.maxIdleTimeClosedCounter)
	s.registry.Unregister(s.maxLifetimeClosedCounter)
	s.registry.Unregister(s.abendCounter)
	return s.SessionInterface.Close()
}

func (s *SessionWithMetrics) TruncateTables(ctx context.Context, tables []string) (err error) {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": "truncate_tables",
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": "truncate_tables",
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()

	err = s.SessionInterface.TruncateTables(ctx, tables)
	return err
}

func (s *SessionWithMetrics) Clone() SessionInterface {
	return &SessionWithMetrics{
		SessionInterface: s.SessionInterface.Clone(),

		// Note that cloned Session will point at the same roundTripProbe
		// to avoid starting multiple go routines.
		roundTripProbe: s.roundTripProbe,

		registry:             s.registry,
		queryCounter:         s.queryCounter,
		queryDurationSummary: s.queryDurationSummary,
		// txnCounter:               s.txnCounter,
		// txnDurationSummary:       s.txnDurationSummary,
		maxOpenConnectionsGauge:  s.maxOpenConnectionsGauge,
		openConnectionsGauge:     s.openConnectionsGauge,
		inUseConnectionsGauge:    s.inUseConnectionsGauge,
		idleConnectionsGauge:     s.idleConnectionsGauge,
		waitCountCounter:         s.waitCountCounter,
		waitDurationCounter:      s.waitDurationCounter,
		maxIdleClosedCounter:     s.maxIdleClosedCounter,
		maxIdleTimeClosedCounter: s.maxIdleTimeClosedCounter,
		maxLifetimeClosedCounter: s.maxLifetimeClosedCounter,
		abendCounter:             s.abendCounter,
	}
}

func getQueryType(ctx context.Context, query squirrel.Sqlizer) QueryType {
	// Do we have an explicit query type set in the context? For raw execs, in
	// lieu of better detection. e.g. "upsert"
	if q, ok := ctx.Value(&QueryTypeContextKey).(QueryType); ok {
		return q
	}

	// is it a squirrel builder?
	if _, ok := query.(squirrel.DeleteBuilder); ok {
		return DeleteQueryType
	}
	if _, ok := query.(squirrel.InsertBuilder); ok {
		return InsertQueryType
	}
	if _, ok := query.(squirrel.SelectBuilder); ok {
		return SelectQueryType
	}
	if _, ok := query.(squirrel.UpdateBuilder); ok {
		return UpdateQueryType
	}

	// Try to guess based on the first word of the string.
	// e.g. "SELECT * FROM table"
	str, _, err := query.ToSql()
	words := strings.Fields(strings.TrimSpace(strings.ToLower(str)))
	if err == nil && len(words) > 0 {
		// Make sure we don't only get known keywords here, incase it's a more
		// complex query.
		for _, word := range []string{"delete", "insert", "select", "update"} {
			if word == words[0] {
				return QueryType(word)
			}
		}
	}

	// Fresh out of ideas.
	return UndefinedQueryType
}

// derive the db 'abend_total' metric from the err returned by libpq sdk
//
// dbErr - the error returned by any libpq method call
// ctx - the caller's context used on libpb method call
func (s *SessionWithMetrics) handleErrorEvent(dbErr error, ctx context.Context) {
	if dbErr == nil || s.NoRows(dbErr) {
		return
	}

	// default the metric to based just on top level libpq error
	abendOrigin := "libpq"
	abendType := "error"
	abendCondition := "n/a"
	var pgDbErrorCode string
	var pqErr *pq.Error

	// apply db server error info if it exists
	// libpq only provides a pg.Error if a server trip was made, otherwise it may not be present
	if errors.As(dbErr, &pqErr) {
		pgDbErrorCode = string(pqErr.Code)
		abendOrigin = "db"
		abendCondition = pgDbErrorCode
	}

	// apply remaining overrides to metric, when these specific points exist
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		abendOrigin = "client_context"
		abendType = "cancel"
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		abendOrigin = "horizon_context"
		abendType = "timeout"
	case pgDbErrorCode == "57014":
		// if getting here, no context deadline happened, but
		// the db reported query_canceled, which leaves only the possibility of
		// db-side statement timeout was triggered
		abendType = "timeout"
	}

	s.abendCounter.With(prometheus.Labels{
		"origin":    abendOrigin,
		"condition": abendCondition,
		"type":      abendType,
	}).Inc()
}

func (s *SessionWithMetrics) Get(ctx context.Context, dest interface{}, query squirrel.Sqlizer) (err error) {
	queryType := string(getQueryType(ctx, query))
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()
	err = s.SessionInterface.Get(ctx, dest, query)
	return err
}

func (s *SessionWithMetrics) GetRaw(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	return s.Get(ctx, dest, squirrel.Expr(query, args...))
}

func (s *SessionWithMetrics) Select(ctx context.Context, dest interface{}, query squirrel.Sqlizer) (err error) {
	queryType := string(getQueryType(ctx, query))
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()

	err = s.SessionInterface.Select(ctx, dest, query)
	return err
}

func (s *SessionWithMetrics) SelectRaw(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	return s.Select(ctx, dest, squirrel.Expr(query, args...))
}

func (s *SessionWithMetrics) Exec(ctx context.Context, query squirrel.Sqlizer) (result sql.Result, err error) {
	queryType := string(getQueryType(ctx, query))
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()

	result, err = s.SessionInterface.Exec(ctx, query)
	return result, err
}

func (s *SessionWithMetrics) ExecRaw(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	return s.Exec(ctx, squirrel.Expr(query, args...))
}

func (s *SessionWithMetrics) Ping(ctx context.Context, timeout time.Duration) (err error) {
	queryType := "ping"
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()

	err = s.SessionInterface.Ping(ctx, timeout)
	return err
}

func (s *SessionWithMetrics) DeleteRange(
	ctx context.Context,
	start, end int64,
	table string,
	idCol string,
) (err error) {
	queryType := "delete"
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		s.queryDurationSummary.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Observe(v)
	}))
	defer func() {
		timer.ObserveDuration()
		s.queryCounter.With(prometheus.Labels{
			"query_type": queryType,
			"error":      fmt.Sprint(err != nil),
			"route":      contextRoute(ctx),
		}).Inc()
	}()

	err = s.SessionInterface.DeleteRange(ctx, start, end, table, idCol)
	return err
}
