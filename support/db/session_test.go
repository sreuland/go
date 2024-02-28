package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/collections/set"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stellar/go/support/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextTimeoutDuringSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithTimeout(ctx, time.Duration(1))
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(2), COUNT(*) FROM people")
	assert.ErrorIs(err, ErrTimeout, "long running db server operation past context timeout, should return timeout")

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_server_timeout_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("server_timeout_closed_session_total metrics were not correct")
}

func TestContextTimeoutBeforeSql(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithTimeout(ctx, time.Second)
	assert := assert.New(t)

	sessRaw := &Session{DB: db.Open()}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)
	defer sess.Close()
	defer cancel()

	var count int
	time.Sleep(time.Second)
	err := sess.GetRaw(ctx, &count, "SELECT pg_sleep(5) FROM people")
	assert.ErrorIs(err, ErrTimeout, "any db server operation should return error immediately if context already timed out")

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_server_timeout_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("server_timeout_closed_session_total metrics were not correct")
}

func TestExternallyCancelledSql(t *testing.T) {
	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	assert := assert.New(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	sessRaw := &Session{DB: sqlxDB}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)

	defer sess.Close()
	defer cancel()

	// simulate an external(not originating from horizon) cancelled sql statement, such as cancelled from psql usage
	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("pq: canceling statement due to user request"))

	var count int
	err = sess.GetRaw(ctx, &count, "SELECT 1")
	assert.ErrorIs(err, ErrCancelled, "externally cancelled sql statement, should return cancelled error")

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_client_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("client_closed_session_total metrics were not correct")
}

func TestSqlStatementTimeout(t *testing.T) {
	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	assert := assert.New(t)

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	sessRaw := &Session{DB: sqlxDB}
	reg := prometheus.NewRegistry()

	sess := RegisterMetrics(sessRaw, "test", "subtest", reg)

	defer sess.Close()
	defer cancel()

	// simulate pg statement timeout
	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("pq: canceling statement due to statement timeout"))

	var count int
	err = sess.GetRaw(ctx, &count, "SELECT 1")
	assert.ErrorIs(err, ErrStatementTimeout, "sql statement timeout, should return timeout error")

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_statement_timeout_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("statement_timeout_closed_session_total metrics were not correct")
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

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_client_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("client_closed_session_total metrics were not correct")
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

	metrics, err := reg.Gather()
	assert.NoError(err)

	for _, metricFamily := range metrics {
		if metricFamily.GetName() == "test_db_client_closed_session_total" {
			assert.Len(metricFamily.GetMetric(), 1)
			assert.Equal(metricFamily.GetMetric()[0].GetCounter().GetValue(), float64(1))
			return
		}
	}
	assert.Fail("client_closed_session_total metrics were not correct")
}

func TestSession(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	ctx := context.Background()
	assert := assert.New(t)
	require := require.New(t)
	sess := &Session{DB: db.Open()}
	defer sess.DB.Close()

	assert.Equal("postgres", sess.Dialect())

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
	out, err := sess.ReplacePlaceholders("? = ? = ? = ??")
	if assert.NoError(err) {
		assert.Equal("$1 = $2 = $3 = ?", out)
	}
}

func TestStatementTimeout(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sess, err := Open(db.Dialect, db.DSN, StatementTimeout(50*time.Millisecond))
	assert.NoError(err)
	defer sess.Close()

	var count int
	err = sess.GetRaw(context.Background(), &count, "SELECT pg_sleep(2), COUNT(*) FROM people")
	assert.ErrorIs(err, ErrStatementTimeout)
}

func TestIdleTransactionTimeout(t *testing.T) {
	assert := assert.New(t)
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sess, err := Open(db.Dialect, db.DSN, IdleTransactionTimeout(50*time.Millisecond))
	assert.NoError(err)
	defer sess.Close()

	assert.NoError(sess.Begin(context.Background()))
	<-time.After(150 * time.Millisecond)

	var count int
	err = sess.GetRaw(context.Background(), &count, "SELECT COUNT(*) FROM people")
	assert.ErrorIs(err, ErrBadConnection)
}

func TestSessionRollbackAfterContextCanceled(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sess := setupRolledbackTx(t, db)
	defer sess.DB.Close()

	assert.ErrorIs(t, sess.Rollback(), ErrAlreadyRolledback)
}

func TestSessionCommitAfterContextCanceled(t *testing.T) {
	db := dbtest.Postgres(t).Load(testSchema)
	defer db.Close()

	sess := setupRolledbackTx(t, db)
	defer sess.DB.Close()

	assert.ErrorIs(t, sess.Commit(), ErrAlreadyRolledback)
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
