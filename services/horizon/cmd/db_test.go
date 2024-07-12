package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	horizon "github.com/stellar/go/services/horizon/internal"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/ingest"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestDBCommandsTestSuite(t *testing.T) {
	dbCmdSuite := &DBCommandsTestSuite{}
	suite.Run(t, dbCmdSuite)
}

type DBCommandsTestSuite struct {
	suite.Suite
	db      *dbtest.DB
	rootCmd *cobra.Command
}

func (s *DBCommandsTestSuite) SetupSuite() {
	runDBReingestRangeFn = func([]history.LedgerRange, bool, uint,
		horizon.Config, ingest.StorageBackendConfig) error {
		return nil
	}

	s.db = dbtest.Postgres(s.T())

	RootCmd.SetArgs([]string{
		"db", "migrate", "up", "--db-url", s.db.DSN})
	require.NoError(s.T(), RootCmd.Execute())
}

func (s *DBCommandsTestSuite) TearDownSuite() {
	s.db.Close()
}

func (s *DBCommandsTestSuite) BeforeTest(suiteName string, testName string) {
	s.rootCmd = NewRootCmd()
}

func (s *DBCommandsTestSuite) TestDefaultParallelJobSizeForBufferedBackend() {
	s.rootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.db.DSN,
		"--network", "testnet",
		"--parallel-workers", "2",
		"--ledgerbackend", "datastore",
		"--datastore-config", "../config.storagebackend.toml",
		"2",
		"10"})

	require.NoError(s.T(), s.rootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(100))
}

func (s *DBCommandsTestSuite) TestDefaultParallelJobSizeForCaptiveBackend() {
	s.rootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.db.DSN,
		"--network", "testnet",
		"--stellar-core-binary-path", "/test/core/bin/path",
		"--parallel-workers", "2",
		"--ledgerbackend", "captive-core",
		"2",
		"10"})

	require.NoError(s.T(), s.rootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(100_000))
}

func (s *DBCommandsTestSuite) TestUsesParallelJobSizeWhenSetForCaptive() {
	s.rootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.db.DSN,
		"--network", "testnet",
		"--stellar-core-binary-path", "/test/core/bin/path",
		"--parallel-workers", "2",
		"--parallel-job-size", "5",
		"--ledgerbackend", "captive-core",
		"2",
		"10"})

	require.NoError(s.T(), s.rootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(5))
}

func (s *DBCommandsTestSuite) TestUsesParallelJobSizeWhenSetForBuffered() {
	s.rootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.db.DSN,
		"--network", "testnet",
		"--parallel-workers", "2",
		"--parallel-job-size", "5",
		"--ledgerbackend", "datastore",
		"--datastore-config", "../config.storagebackend.toml",
		"2",
		"10"})

	require.NoError(s.T(), s.rootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(5))
}
