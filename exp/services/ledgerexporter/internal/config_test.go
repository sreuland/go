package ledgerexporter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	ctx := context.Background()

	mockArchive := &historyarchive.MockArchive{}
	mockArchive.On("GetRootHAS").Return(historyarchive.HistoryArchiveState{CurrentLedger: 5}, nil).Once()

	config, err := NewConfig(
		RuntimeSettings{StartLedger: 2, EndLedger: 3, ConfigFilePath: "test/test.toml", Mode: Append})
	require.NoError(t, err)
	err = config.ValidateAndSetLedgerRange(ctx, mockArchive)
	require.NoError(t, err)
	require.Equal(t, config.NetworkName, "test")
	require.Equal(t, config.DataStoreConfig.Type, "ABC")
	require.Equal(t, config.DataStoreConfig.Schema.FilesPerPartition, uint32(1))
	require.Equal(t, config.DataStoreConfig.Schema.LedgersPerFile, uint32(3))
	require.Equal(t, config.UserAgent, "ledgerexporter")
	require.True(t, config.Resumable())
	url, ok := config.DataStoreConfig.Params["destination_bucket_path"]
	require.True(t, ok)
	require.Equal(t, url, "your-bucket-name/subpath")
	mockArchive.AssertExpectations(t)
}

func TestGenerateHistoryArchiveFromPreconfiguredNetwork(t *testing.T) {
	ctx := context.Background()
	config, err := NewConfig(
		RuntimeSettings{StartLedger: 2, EndLedger: 3, ConfigFilePath: "test/valid_preconfigured_network.toml", Mode: Append})
	require.NoError(t, err)

	_, err = config.GenerateHistoryArchive(ctx)
	require.NoError(t, err)
}

func TestGenerateHistoryArchive(t *testing.T) {
	ctx := context.Background()
	config, err := NewConfig(
		RuntimeSettings{StartLedger: 2, EndLedger: 3, ConfigFilePath: "test/valid_captive_core.toml", Mode: Append})
	require.NoError(t, err)

	_, err = config.GenerateHistoryArchive(ctx)
	require.NoError(t, err)
}

func TestNewConfigUserAgent(t *testing.T) {
	config, err := NewConfig(
		RuntimeSettings{StartLedger: 2, EndLedger: 3, ConfigFilePath: "test/test_useragent.toml"})
	require.NoError(t, err)
	require.Equal(t, config.UserAgent, "useragent_x")
}

func TestResumeDisabled(t *testing.T) {
	// resumable is only enabled when mode is Append
	config, err := NewConfig(
		RuntimeSettings{StartLedger: 2, EndLedger: 3, ConfigFilePath: "test/test.toml", Mode: ScanFill})
	require.NoError(t, err)
	require.False(t, config.Resumable())
}

func TestInvalidConfigPath(t *testing.T) {
	_, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/notfound.toml"})
	require.Error(t, err)
}

func TestInvalidNetworkConfig(t *testing.T) {
	_, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/no_network_name.toml"})
	require.ErrorContains(t, err, "Invalid config file, network_name, must be set")

	cfg, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/invalid_preconfigured_network.toml"})
	require.NoError(t, err)

	_, err = cfg.GenerateCaptiveCoreConfig()
	require.ErrorContains(t, err, "invalid captive core config")
}

func TestValidNetworkConfig(t *testing.T) {
	cfg, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/valid_preconfigured_network.toml"})
	require.NoError(t, err)

	_, err = cfg.GenerateCaptiveCoreConfig()
	require.NoError(t, err)
	require.Equal(t, cfg.StellarCoreConfig.NetworkPassphrase, network.PublicNetworkPassphrase)
}

func TestValidCaptiveCoreConfig(t *testing.T) {
	cfg, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/valid_captive_core.toml"})
	require.NoError(t, err)

	ccConfig, err := cfg.GenerateCaptiveCoreConfig()
	require.NoError(t, err)

	require.Equal(t, ccConfig.NetworkPassphrase, "test")
	require.Len(t, ccConfig.Toml.Validators, 1)
	require.Equal(t, ccConfig.Toml.Validators[0].Name, "local_core")
}

func TestInvalidCaptiveCoreConfig(t *testing.T) {
	cfg, err := NewConfig(
		RuntimeSettings{ConfigFilePath: "test/invalid_captive_core.toml"})
	require.NoError(t, err)

	_, err = cfg.GenerateCaptiveCoreConfig()
	require.ErrorContains(t, err, "Failed to load captive-core-toml-path file")
}

func TestValidateStartAndEndLedger(t *testing.T) {
	latestNetworkLedger := uint32(20000)
	latestNetworkLedgerPadding := datastore.GetHistoryArchivesCheckPointFrequency() * 2

	tests := []struct {
		name        string
		startLedger uint32
		endLedger   uint32
		errMsg      string
		mode        Mode
		mockHas     bool
	}{
		{
			name:        "End ledger same as latest ledger",
			startLedger: 512,
			endLedger:   512,
			mode:        ScanFill,
			errMsg:      "invalid end value, must be greater than start",
			mockHas:     false,
		},
		{
			name:        "End ledger greater than start ledger",
			startLedger: 512,
			endLedger:   600,
			mode:        ScanFill,
			errMsg:      "",
			mockHas:     true,
		},
		{
			name:        "No end ledger provided, append mode, no error",
			startLedger: 512,
			endLedger:   0,
			mode:        Append,
			errMsg:      "",
			mockHas:     true,
		},
		{
			name:        "No end ledger provided, scan-and-fill error",
			startLedger: 512,
			endLedger:   0,
			mode:        ScanFill,
			errMsg:      "invalid end value, unbounded mode not supported, end must be greater than start.",
		},
		{
			name:        "End ledger before start ledger",
			startLedger: 512,
			endLedger:   2,
			mode:        ScanFill,
			errMsg:      "invalid end value, must be greater than start",
		},
		{
			name:        "End ledger exceeds latest ledger",
			startLedger: 512,
			endLedger:   latestNetworkLedger + latestNetworkLedgerPadding + 1,
			mode:        ScanFill,
			mockHas:     true,
			errMsg: fmt.Sprintf("end %d exceeds latest network ledger %d",
				latestNetworkLedger+latestNetworkLedgerPadding+1, latestNetworkLedger+latestNetworkLedgerPadding),
		},
		{
			name:        "Start ledger 0",
			startLedger: 0,
			endLedger:   2,
			mode:        ScanFill,
			errMsg:      "invalid start value, must be greater than one.",
		},
		{
			name:        "Start ledger 1",
			startLedger: 1,
			endLedger:   2,
			mode:        ScanFill,
			errMsg:      "invalid start value, must be greater than one.",
		},
		{
			name:        "Start ledger exceeds latest ledger",
			startLedger: latestNetworkLedger + latestNetworkLedgerPadding + 1,
			endLedger:   latestNetworkLedger + latestNetworkLedgerPadding + 2,
			mode:        ScanFill,
			mockHas:     true,
			errMsg: fmt.Sprintf("start %d exceeds latest network ledger %d",
				latestNetworkLedger+latestNetworkLedgerPadding+1, latestNetworkLedger+latestNetworkLedgerPadding),
		},
	}

	ctx := context.Background()
	mockArchive := &historyarchive.MockArchive{}
	mockArchive.On("GetRootHAS").Return(historyarchive.HistoryArchiveState{CurrentLedger: latestNetworkLedger}, nil)

	mockedHasCtr := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockHas {
				mockedHasCtr++
			}
			config, err := NewConfig(
				RuntimeSettings{StartLedger: tt.startLedger, EndLedger: tt.endLedger, ConfigFilePath: "test/validate_start_end.toml", Mode: tt.mode})
			require.NoError(t, err)
			err = config.ValidateAndSetLedgerRange(ctx, mockArchive)
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Equal(t, tt.errMsg, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
	mockArchive.AssertNumberOfCalls(t, "GetRootHAS", mockedHasCtr)
}

func TestAdjustedLedgerRangeBoundedMode(t *testing.T) {
	tests := []struct {
		name          string
		configFile    string
		start         uint32
		end           uint32
		expectedStart uint32
		expectedEnd   uint32
	}{
		{
			name:          "No change, 1 ledger per file",
			configFile:    "test/1perfile.toml",
			start:         2,
			end:           3,
			expectedStart: 2,
			expectedEnd:   3,
		},
		{
			name:          "Min start ledger2, round up end ledger, 10 ledgers per file",
			configFile:    "test/10perfile.toml",
			start:         2,
			end:           3,
			expectedStart: 2,
			expectedEnd:   10,
		},
		{
			name:          "Round down start ledger and round up end ledger, 15 ledgers per file ",
			configFile:    "test/15perfile.toml",
			start:         4,
			end:           10,
			expectedStart: 2,
			expectedEnd:   15,
		},
		{
			name:          "Round down start ledger and round up end ledger, 64 ledgers per file ",
			configFile:    "test/64perfile.toml",
			start:         400,
			end:           500,
			expectedStart: 384,
			expectedEnd:   512,
		},
		{
			name:          "No change, 64 ledger per file",
			configFile:    "test/64perfile.toml",
			start:         64,
			end:           128,
			expectedStart: 64,
			expectedEnd:   128,
		},
	}

	ctx := context.Background()
	mockArchive := &historyarchive.MockArchive{}
	mockArchive.On("GetRootHAS").Return(historyarchive.HistoryArchiveState{CurrentLedger: 500}, nil).Times(len(tests))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewConfig(
				RuntimeSettings{StartLedger: tt.start, EndLedger: tt.end, ConfigFilePath: tt.configFile, Mode: ScanFill})
			require.NoError(t, err)
			err = config.ValidateAndSetLedgerRange(ctx, mockArchive)
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedStart, config.StartLedger)
			require.EqualValues(t, tt.expectedEnd, config.EndLedger)
		})
	}
	mockArchive.AssertExpectations(t)
}

func TestAdjustedLedgerRangeUnBoundedMode(t *testing.T) {
	tests := []struct {
		name          string
		configFile    string
		start         uint32
		end           uint32
		expectedStart uint32
		expectedEnd   uint32
	}{
		{
			name:          "No change, 1 ledger per file",
			configFile:    "test/1perfile.toml",
			start:         2,
			end:           0,
			expectedStart: 2,
			expectedEnd:   0,
		},
		{
			name:          "Round down start ledger, 15 ledgers per file ",
			configFile:    "test/15perfile.toml",
			start:         4,
			end:           0,
			expectedStart: 2,
			expectedEnd:   0,
		},
		{
			name:          "Round down start ledger, 64 ledgers per file ",
			configFile:    "test/64perfile.toml",
			start:         400,
			end:           0,
			expectedStart: 384,
			expectedEnd:   0,
		},
		{
			name:          "No change, 64 ledger per file",
			configFile:    "test/64perfile.toml",
			start:         64,
			end:           0,
			expectedStart: 64,
			expectedEnd:   0,
		},
	}

	ctx := context.Background()

	mockArchive := &historyarchive.MockArchive{}
	mockArchive.On("GetRootHAS").Return(historyarchive.HistoryArchiveState{CurrentLedger: 500}, nil).Times(len(tests))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewConfig(
				RuntimeSettings{StartLedger: tt.start, EndLedger: tt.end, ConfigFilePath: tt.configFile, Mode: Append})
			require.NoError(t, err)
			err = config.ValidateAndSetLedgerRange(ctx, mockArchive)
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedStart, config.StartLedger)
			require.EqualValues(t, tt.expectedEnd, config.EndLedger)
		})
	}
	mockArchive.AssertExpectations(t)
}
