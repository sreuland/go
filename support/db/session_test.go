package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextTimeoutDuringSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(5) FROM people")
		assert.ErrorIs(err, ErrTimeout, "long running db server operation past context timeout, should return timeout")
		wg.Done()
	}()

	require.Eventually(t, func() bool { wg.Wait(); return true }, 5*time.Second, time.Second)
	// note, condition is populated with the db error, since a trip to server was made with sql running at time of cancel
	assertAbendMetrics(reg, "horizon_context", "query_canceled", "timeout", assert)
}

func TestContextTimeoutBeforeSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	time.Sleep(500 * time.Millisecond)
	err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(5) FROM people")
	assert.ErrorIs(err, ErrTimeout, "any db server operation should return error immediately if context already timed out")
	// note, the condition is empty, the sql never made it to db, libpq short-circuited it based on ctx
	assertAbendMetrics(reg, "horizon_context", "", "timeout", assert)
}

func TestContextCancelledBeforeSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	cancel()
	err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(2), COUNT(*) FROM people")
	assert.ErrorIs(err, ErrCancelled, "any db server operation should return error immediately if user already cancel")
	// note, the condition is empty, the sql never made it to db, libpq short-circuited it based on ctx
	assertAbendMetrics(reg, "client_context", "", "cancel", assert)
}

func TestContextCancelDuringSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(5) FROM people")
		assert.ErrorIs(err, ErrCancelled, "any ongoing db server operation should return error immediately after user cancel")
		wg.Done()
	}()
	time.Sleep(time.Second)
	cancel()

	require.Eventually(t, func() bool { wg.Wait(); return true }, 5*time.Second, time.Second)
	// note, condition is populated with the db error, since a trip to server was made with sql running at time of cancel
	assertAbendMetrics(reg, "client_context", "query_canceled", "cancel", assert)
}

func TestStatementTimeout(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sessRaw, err := Open(db.Dialect, db.DSN, StatementTimeout(50*time.Millisecond))
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	assert.NoError(err)
	defer sess.Close()

	var count int
	err = sess.GetRaw(context.Background(), &count, "SELECT pg_sleep(2) FROM people")
	assert.ErrorIs(err, ErrStatementTimeout)
	assertAbendMetrics(reg, "db", "query_canceled", "timeout", assert)
}

func TestSession(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	ctx := context.Background()
	assert := assert.New(t)
	require := require.New(t)
	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()

	assert.Equal("postgres", sessRaw.Dialect())

	var count int
	err := sess.GetRaw(ctx, &count, "SELECT COUNT(*) FROM people")
	assert.NoError(err)
	assert.Equal(3, count)

	var names []string
	err = sess.SelectRaw(ctx, &names, "SELECT name FROM people")
	assert.NoError(err)
	assert.Len(names, 3)

	ret, err := sess.ExecRaw(ctx, "DELETE FROM people")
	assert.NoError(err)
	deleted, err := ret.RowsAffected()
	assert.NoError(err)
	assert.Equal(int64(3), deleted)

	// Test args (NOTE: there is a simple escaped arg to ensure no error is raised
	// during execution)
	db.Load(testSchema)
	var name string
	err = sess.GetRaw(ctx,
		&name,
		"SELECT name FROM people WHERE hunger_level = ? AND name != '??'",
		1000000,
	)
	assert.NoError(err)
	assert.Equal("scott", name)

	// Test NoRows
	err = sess.GetRaw(ctx,
		&name,
		"SELECT name FROM people WHERE hunger_level = ?",
		1234,
	)
	assert.True(sess.NoRows(err))

	// Test transactions
	db.Load(testSchema)
	require.NoError(sess.Begin(ctx), "begin failed")
	err = sess.GetRaw(ctx, &count, "SELECT COUNT(*) FROM people")
	assert.NoError(err)
	assert.Equal(3, count)
	_, err = sess.ExecRaw(ctx, "DELETE FROM people")
	assert.NoError(err)
	err = sess.GetRaw(ctx, &count, "SELECT COUNT(*) FROM people")
	assert.NoError(err)
	assert.Equal(0, count, "people did not appear deleted inside transaction")
	assert.NoError(sess.Rollback(), "rollback failed")

	// Ensure commit works
	require.NoError(sess.Begin(ctx), "begin failed")
	sess.ExecRaw(ctx, "DELETE FROM people")
	assert.NoError(sess.Commit(), "commit failed")
	err = sess.GetRaw(ctx, &count, "SELECT COUNT(*) FROM people")
	assert.NoError(err)
	assert.Equal(0, count)

	// ensure that selecting into a populated slice clears the slice first
	db.Load(testSchema)
	require.Len(names, 3, "ids slice was not preloaded with data")
	err = sess.SelectRaw(ctx, &names, "SELECT name FROM people limit 2")
	assert.NoError(err)
	assert.Len(names, 2)

	// Test ReplacePlaceholders
	out, err := sessRaw.ReplacePlaceholders("? = ? = ? = ??")
	if assert.NoError(err) {
		assert.Equal("$1 = $2 = $3 = ?", out)
	}

	assertZeroAbendMetrics(reg, assert)
}

func TestIdleTransactionTimeout(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sessRaw, err := Open(db.Dialect, db.DSN, IdleTransactionTimeout(50*time.Millisecond))
	assert.NoError(err)
	reg := prometheus.NewRegistry()
	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)

	defer sess.Close()

	assert.NoError(sess.Begin(context.Background()))
	<-time.After(150 * time.Millisecond)

	var count int
	err = sess.GetRaw(context.Background(), &count, "SELECT COUNT(*) FROM people")
	assert.ErrorIs(err, ErrBadConnection)
	assertAbendMetrics(reg, "libpq", "driver: bad connection", "error", assert)
}

func TestSessionRollbackAfterContextCanceled(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sessRaw := setupRolledbackTx(t, db)
	reg := prometheus.NewRegistry()
	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()

	assert.ErrorIs(sess.Rollback(), ErrAlreadyRolledback)
	assertAbendMetrics(reg, "libpq", "sql: transaction has already been committed or rolled back", "error", assert)
}

func TestSessionCommitAfterContextCanceled(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sessRaw := setupRolledbackTx(t, db)
	reg := prometheus.NewRegistry()
	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()

	assert.ErrorIs(sess.Commit(), ErrAlreadyRolledback)
	assertAbendMetrics(reg, "libpq", "sql: transaction has already been committed or rolled back", "error", assert)
}

func assertZeroAbendMetrics(reg *prometheus.Registry, assert *assert.Assertions) {
	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_abend_total" {
			assert.Fail("abend_total metrics should not be present, never incremented")
		}
	}

}

func assertAbendMetrics(reg *prometheus.Registry, assertOrigin, assertCondition, assertType string, assert *assert.Assertions) {
	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_abend_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			var origin = ""
			var condition = ""
			var abend_type = ""
			for _, label := range metricFamily.GetMetric()[0].GetLabel() {
				if label.GetName() == "origin" {
					origin = label.GetValue()
				}
				if label.GetName() == "condition" {
					condition = label.GetValue()
				}
				if label.GetName() == "type" {
					abend_type = label.GetValue()
				}
			}

			assert.Equal(origin, assertOrigin)
			assert.Equal(condition, assertCondition)
			assert.Equal(abend_type, assertType)
			return
		}
	}
	assert.Fail("abend_total metrics were not correct")
}

func setupRolledbackTx(t *testing.T, db *dbtest.DB) *Session {
	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)

	sess := &Session{DB: db.Open()}

	assert.NoError(t, sess.Begin(ctx))

	var count int
	assert.NoError(t, sess.GetRaw(ctx, &count, "SELECT COUNT(*) FROM people"))
	assert.Equal(t, 3, count)

	cancel()
	time.Sleep(500 * time.Millisecond)
	return sess
}
