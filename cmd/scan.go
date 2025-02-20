package cmd

import (
	"fmt"
	"log"

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
		fmt.Printf("Scanning %s for %s files...\n", dirPath, fileFormat)
		// Actual scanning logic will go here
	},
}

func init() {
	rootCmd.AddCommand(scanCmd)

	scanCmd.Flags().StringVarP(&dirPath, "dir", "d", "",
		"Directory to scan")
	scanCmd.Flags().StringVarP(&fileFormat, "format", "f", "csv",
		"File format to analyze (csv, json)")
}
