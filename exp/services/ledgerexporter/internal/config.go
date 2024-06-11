package ledgerexporter

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"

	"github.com/pelletier/go-toml"

	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/ordered"
	"github.com/stellar/go/support/storage"
)

const (
	Pubnet    = "pubnet"
	Testnet   = "testnet"
	UserAgent = "ledgerexporter"
)

type Mode int

const (
	_        Mode = iota
	ScanFill Mode = iota
	Append
)

type RuntimeSettings struct {
	StartLedger    uint32
	EndLedger      uint32
	ConfigFilePath string
	Mode           Mode
}

type StellarCoreConfig struct {
	PreconfiguredNetwork  string   `toml:"preconfigured_network"`
	NetworkPassphrase     string   `toml:"network_passphrase"`
	HistoryArchiveUrls    []string `toml:"history_archive_urls"`
	StellarCoreBinaryPath string   `toml:"stellar_core_binary_path"`
	CaptiveCoreTomlPath   string   `toml:"captive_core_toml_path"`
}

type Config struct {
	AdminPort int `toml:"admin_port"`

	NetworkName       string                    `toml:"network_name"`
	DataStoreConfig   datastore.DataStoreConfig `toml:"datastore_config"`
	StellarCoreConfig StellarCoreConfig         `toml:"stellar_core_config"`
	UserAgent         string                    `toml:"user_agent"`

	StartLedger uint32
	EndLedger   uint32
	Mode        Mode

	CoreVersion string
}

// This will generate the config based on settings
//
// settings              - requested settings
//
// return                - *Config or an error if any range validation failed.
func NewConfig(settings RuntimeSettings) (*Config, error) {
	config := &Config{}

	config.StartLedger = uint32(settings.StartLedger)
	config.EndLedger = uint32(settings.EndLedger)
	config.Mode = settings.Mode

	logger.Infof("Requested export mode of %v with start=%d, end=%d", settings.Mode, config.StartLedger, config.EndLedger)

	var err error
	if err = config.processToml(settings.ConfigFilePath); err != nil {
		return nil, err
	}
	logger.Infof("Config: %v", *config)

	return config, nil
}

func (config *Config) Resumable() bool {
	return config.Mode == Append
}

// Validates requested ledger range, and will automatically adjust it
// to be ledgers-per-file boundary aligned
func (config *Config) ValidateAndSetLedgerRange(ctx context.Context, archive historyarchive.ArchiveInterface) error {

	if config.StartLedger < 2 {
		return errors.New("invalid start value, must be greater than one.")
	}

	if config.Mode == ScanFill && config.EndLedger == 0 {
		return errors.New("invalid end value, unbounded mode not supported, end must be greater than start.")
	}

	if config.EndLedger != 0 && config.EndLedger <= config.StartLedger {
		return errors.New("invalid end value, must be greater than start")
	}

	latestNetworkLedger, err := datastore.GetLatestLedgerSequenceFromHistoryArchives(archive)
	latestNetworkLedger = latestNetworkLedger + (datastore.GetHistoryArchivesCheckPointFrequency() * 2)

	if err != nil {
		return errors.Wrap(err, "Failed to retrieve the latest ledger sequence from history archives.")
	}
	logger.Infof("Latest ledger sequence was detected as %d", latestNetworkLedger)

	if config.StartLedger > latestNetworkLedger {
		return errors.Errorf("start %d exceeds latest network ledger %d",
			config.StartLedger, latestNetworkLedger)
	}

	if config.EndLedger > latestNetworkLedger {
		return errors.Errorf("end %d exceeds latest network ledger %d",
			config.EndLedger, latestNetworkLedger)
	}

	config.adjustLedgerRange()
	return nil
}

func (config *Config) GenerateHistoryArchive(ctx context.Context) (historyarchive.ArchiveInterface, error) {
	if config.StellarCoreConfig.PreconfiguredNetwork != "" {
		return datastore.CreateHistoryArchiveFromNetworkName(ctx, config.StellarCoreConfig.PreconfiguredNetwork, config.UserAgent)
	}
	return historyarchive.NewArchivePool(config.StellarCoreConfig.HistoryArchiveUrls, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: config.UserAgent,
			Context:   ctx,
		},
	})
}

// coreBinDefaultPath - a default value to use for core binary path on system.
//
//	this will be used if StellarCoreConfig.StellarCoreBinaryPath is not specified
func (config *Config) GenerateCaptiveCoreConfig(coreBinFromPath string) (ledgerbackend.CaptiveCoreConfig, error) {
	var err error
	if config.StellarCoreConfig.PreconfiguredNetwork == "" && (len(config.StellarCoreConfig.HistoryArchiveUrls) == 0 || config.StellarCoreConfig.NetworkPassphrase == "" || config.StellarCoreConfig.CaptiveCoreTomlPath == "") {
		return ledgerbackend.CaptiveCoreConfig{}, errors.New("Invalid captive core config, the 'preconfigured_network' parameter must be set to pubnet or testnet or " +
			"'stellar_core_config.history_archive_urls' and 'stellar_core_config.network_passphrase' and 'stellar_core_config.captive_core_toml_path' must be set.")
	}

	if config.StellarCoreConfig.StellarCoreBinaryPath == "" && coreBinFromPath == "" {
		return ledgerbackend.CaptiveCoreConfig{}, errors.New("Invalid config, no stellar-core binary path was provided.")
	}

	if config.StellarCoreConfig.StellarCoreBinaryPath == "" {
		config.StellarCoreConfig.StellarCoreBinaryPath = coreBinFromPath
	}

	if err = config.setCoreVersionInfo(); err != nil {
		return ledgerbackend.CaptiveCoreConfig{}, fmt.Errorf("failed to set stellar-core version info: %w", err)
	}

	var captiveCoreConfig []byte
	switch config.StellarCoreConfig.PreconfiguredNetwork {
	case "":
		if captiveCoreConfig, err = os.ReadFile(config.StellarCoreConfig.CaptiveCoreTomlPath); err != nil {
			return ledgerbackend.CaptiveCoreConfig{}, errors.Wrap(err, "Failed to load captive-core-toml-path file")
		}

	case Pubnet:
		config.StellarCoreConfig.NetworkPassphrase = network.PublicNetworkPassphrase
		config.StellarCoreConfig.HistoryArchiveUrls = network.PublicNetworkhistoryArchiveURLs
		captiveCoreConfig = ledgerbackend.PubnetDefaultConfig

	case Testnet:
		config.StellarCoreConfig.NetworkPassphrase = network.TestNetworkPassphrase
		config.StellarCoreConfig.HistoryArchiveUrls = network.TestNetworkhistoryArchiveURLs
		captiveCoreConfig = ledgerbackend.TestnetDefaultConfig

	default:
		return ledgerbackend.CaptiveCoreConfig{}, errors.New("invalid captive core config, " +
			"preconfigured_network must be set to 'pubnet' or 'testnet' or network_passphrase, history_archive_urls," +
			" and captive_core_toml_path must be set")
	}

	params := ledgerbackend.CaptiveCoreTomlParams{
		NetworkPassphrase:  config.StellarCoreConfig.NetworkPassphrase,
		HistoryArchiveURLs: config.StellarCoreConfig.HistoryArchiveUrls,
		UseDB:              true,
	}

	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromData(captiveCoreConfig, params)
	if err != nil {
		return ledgerbackend.CaptiveCoreConfig{}, errors.Wrap(err, "Failed to create captive-core toml")
	}

	return ledgerbackend.CaptiveCoreConfig{
		BinaryPath:          config.StellarCoreConfig.StellarCoreBinaryPath,
		NetworkPassphrase:   params.NetworkPassphrase,
		HistoryArchiveURLs:  params.HistoryArchiveURLs,
		CheckpointFrequency: datastore.GetHistoryArchivesCheckPointFrequency(),
		Log:                 logger.WithField("subservice", "stellar-core"),
		Toml:                captiveCoreToml,
		UserAgent:           "ledger-exporter",
		UseDB:               true,
	}, nil
}

// By default, it points to exec.Command, overridden for testing purpose
var execCommand = exec.Command

// Executes the "stellar-core version" command and parses its output to extract
// the core version
// The output of the "version" command is expected to be a multi-line string where the
// first line is the core version in format "vX.Y.Z-*".
func (c *Config) setCoreVersionInfo() (err error) {
	versionCmd := execCommand(c.StellarCoreConfig.StellarCoreBinaryPath, "version")
	versionOutput, err := versionCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to execute stellar-core version command: %w", err)
	}

	// Split the output into lines
	rows := strings.Split(string(versionOutput), "\n")
	if len(rows) == 0 || len(rows[0]) == 0 {
		return fmt.Errorf("stellar-core version not found")
	}
	c.CoreVersion = rows[0]
	logger.Infof("stellar-core version: %s", c.CoreVersion)
	return nil
}

func (config *Config) processToml(tomlPath string) error {
	// Load config TOML file
	cfg, err := toml.LoadFile(tomlPath)
	if err != nil {
		return err
	}

	// Unmarshal TOML data into the Config struct
	if err := cfg.Unmarshal(config); err != nil {
		return errors.Wrap(err, "Error unmarshalling TOML config.")
	}

	if config.UserAgent == "" {
		config.UserAgent = UserAgent
	}

	if config.NetworkName == "" {
		return errors.Errorf("Invalid config file, network_name must be set")
	}

	return nil
}

func (config *Config) adjustLedgerRange() {
	// Check if either the start or end ledger does not fall on the "LedgersPerFile" boundary
	// and adjust the start and end ledger accordingly.
	// Align the start ledger to the nearest "LedgersPerFile" boundary.
	config.StartLedger = config.DataStoreConfig.Schema.GetSequenceNumberStartBoundary(config.StartLedger)

	// Ensure that the adjusted start ledger is at least 2.
	config.StartLedger = ordered.Max(2, config.StartLedger)

	// Align the end ledger (for bounded cases) to the nearest "LedgersPerFile" boundary.
	if config.EndLedger != 0 {
		config.EndLedger = config.DataStoreConfig.Schema.GetSequenceNumberEndBoundary(config.EndLedger)
	}

	logger.Infof("Computed effective export boundary ledger range: start=%d, end=%d", config.StartLedger, config.EndLedger)
}
