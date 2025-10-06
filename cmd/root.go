package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "dataqa",
	Short: "High-performance CSV statistics and analysis tool",
	Long: `A high-performance CLI tool that provides comprehensive statistics and analysis
for CSV files with parallel processing capabilities. Optimized for speed and memory efficiency
with automatic resource optimization.`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default is $HOME/.dataqa.yaml)")
}
