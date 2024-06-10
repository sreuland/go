package ledgerexporter

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stellar/go/support/strutils"
)

var (
	ledgerExporterCmdRunner = func(runtimeSettings RuntimeSettings) error {
		app := NewApp()
		return app.Run(runtimeSettings)
	}
	rootCmd, scanAndFillCmd, appendCmd *cobra.Command
)

func Execute() error {
	defineCommands()
	return rootCmd.Execute()
}

func defineCommands() {
	rootCmd = &cobra.Command{
		Use:   "ledgerexporter",
		Short: "Export Stellar network ledger data to a remote data store",
		Long:  "Converts ledger meta data from Stellar network into static data and exports it remote data storage.",

		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("please specify one of the availble sub-commands to initiate export")
		},
	}
	scanAndFillCmd = &cobra.Command{
		Use:   "scan-and-fill",
		Short: "scans the entire bounded requested range between 'start' and 'end' flags and exports only the ledgers which are missing from the data lake.",
		Long:  "scans the entire bounded requested range between 'start' and 'end' flags and exports only the ledgers which are missing from the data lake.",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = ScanFill
			return ledgerExporterCmdRunner(settings)
		},
	}
	appendCmd = &cobra.Command{
		Use:   "append",
		Short: "export ledgers beginning with a dynamically determined starting point based on binary search for first absent ledger on datastore after the 'start' ledger",
		Long:  "export ledgers beginning with a dynamically determined starting point based on binary search for first absent ledger on datastore after the 'start' ledger",
		RunE: func(cmd *cobra.Command, args []string) error {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
			)
			settings.Mode = Append
			return ledgerExporterCmdRunner(settings)
		},
	}

	rootCmd.AddCommand(scanAndFillCmd)
	rootCmd.AddCommand(appendCmd)

	scanAndFillCmd.PersistentFlags().Uint32("start", 0, "Starting ledger (inclusive), must be set to a value greater than 1")
	scanAndFillCmd.PersistentFlags().Uint32("end", 0, "Ending ledger (inclusive), must be set to value greater than 'start' and less than 'latest checkpoint ledger from network history archives + (2 * checkpoint ledger frequency)'")
	scanAndFillCmd.PersistentFlags().String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")
	viper.BindPFlags(scanAndFillCmd.PersistentFlags())

	appendCmd.PersistentFlags().Uint32("start", 0, "Starting ledger (inclusive), must be set to a value greater than 1")
	appendCmd.PersistentFlags().Uint32("end", 0, "Ending ledger, optional, setting to non-zero means bounded mode, "+
		"only export ledgers from 'start' up to 'end' value which must be greater than 'start' and less than 'latest checkpoint ledger from network history archives + (2 * checkpoint ledger frequency)'. "+
		"If 'end' is absent or '0' means unbounded mode, exporter will continue to run indefintely and export the latest closed ledgers from network as they are generated in real time.")
	appendCmd.PersistentFlags().String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")
	viper.BindPFlags(appendCmd.PersistentFlags())
}

func bindCliParameters(startFlag *pflag.Flag, endFlag *pflag.Flag, configFileFlag *pflag.Flag) RuntimeSettings {
	settings := RuntimeSettings{}

	viper.BindPFlag(startFlag.Name, startFlag)
	viper.BindEnv(startFlag.Name, strutils.KebabToConstantCase(startFlag.Name))
	settings.StartLedger = viper.GetUint32(startFlag.Name)

	viper.BindPFlag(endFlag.Name, endFlag)
	viper.BindEnv(endFlag.Name, strutils.KebabToConstantCase(endFlag.Name))
	settings.EndLedger = viper.GetUint32(endFlag.Name)

	viper.BindPFlag(configFileFlag.Name, configFileFlag)
	viper.BindEnv(configFileFlag.Name, strutils.KebabToConstantCase(configFileFlag.Name))
	settings.ConfigFilePath = viper.GetString(configFileFlag.Name)

	return settings
}
