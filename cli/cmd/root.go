package cmd

import (
	"fmt"
	"os"
	"time"

	"gvdb-cli/client"
	"gvdb-cli/config"
	"gvdb-cli/output"

	"github.com/spf13/cobra"
)

var (
	version = "dev"

	flagAddress string
	flagAPIKey  string
	flagOutput  string
	flagConfig  string
	flagProfile string
	flagTimeout time.Duration
	flagNoColor bool

	cfg *config.Config
	cli *client.Client
)

func outputFormat() output.Format {
	switch cfg.Output {
	case "json":
		return output.JSON
	case "csv":
		return output.CSV
	default:
		return output.Table
	}
}

var rootCmd = &cobra.Command{
	Use:   "gvdb",
	Short: "CLI and TUI for GVDB distributed vector database",
	Long:  "Interactive terminal UI and scriptable CLI for managing GVDB collections, vectors, and searches.",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfg = config.Load(flagConfig)
		cfg.Resolve(flagAddress, flagAPIKey, flagOutput, flagProfile, flagTimeout, flagNoColor)

		// Skip client creation for commands that don't need it
		if cmd.Name() == "version" || cmd.Name() == "help" {
			return nil
		}

		var err error
		cli, err = client.New(client.Options{
			Address: cfg.Address,
			APIKey:  cfg.APIKey,
			Timeout: cfg.Timeout,
		})
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", cfg.Address, err)
		}
		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if cli != nil {
			cli.Close()
		}
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runTUI()
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return err
	}
	return nil
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&flagAddress, "address", "a", "", "GVDB server address (env: GVDB_ADDR)")
	rootCmd.PersistentFlags().StringVar(&flagAPIKey, "api-key", "", "API key for authentication (env: GVDB_API_KEY)")
	rootCmd.PersistentFlags().StringVarP(&flagOutput, "output", "o", "", "Output format: table, json, csv")
	rootCmd.PersistentFlags().StringVar(&flagConfig, "config", config.DefaultPath(), "Config file path")
	rootCmd.PersistentFlags().StringVar(&flagProfile, "profile", "", "Named profile from config")
	rootCmd.PersistentFlags().DurationVar(&flagTimeout, "timeout", 0, "Request timeout (e.g. 30s, 1m)")
	rootCmd.PersistentFlags().BoolVar(&flagNoColor, "no-color", false, "Disable colored output")
}
