package cmd

import (
	"fmt"
	"log"

	"github.com/dustin/go-humanize"
	"github.com/peekknuf/dataqa/internal/connectors"
	"github.com/spf13/cobra"
)

var (
	dirPath    string
	fileFormat string
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan directory for data files",
	Long: `Scan a directory and analyze data files 
for quality metrics and statistics`,
	Run: func(cmd *cobra.Command, args []string) {
		if dirPath == "" {
			log.Fatal("You must specify a directory with --dir")
		}

		files, err := connectors.DiscoverFiles(dirPath, fileFormat)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		fmt.Printf("\nFound %d %s files:\n", len(files), fileFormat)
		for _, f := range files {
			fmt.Printf("- %s (%s)\n",
				f.Path,
				humanize.Bytes(uint64(f.Size)))
		}
		fmt.Println() // Add extra newline for better formatting
	},
}

func init() {
	rootCmd.AddCommand(scanCmd)

	scanCmd.Flags().StringVarP(&dirPath, "dir", "d", "",
		"Directory to scan (required)")
	scanCmd.Flags().StringVarP(&fileFormat, "format", "f", "csv",
		"File format to analyze (csv, json)")

	// Mark dir flag as required
	scanCmd.MarkFlagRequired("dir")
}
