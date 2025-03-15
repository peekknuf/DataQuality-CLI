package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/peekknuf/DataQuality-CLI/internal/connectors"
	"github.com/peekknuf/DataQuality-CLI/internal/profiler"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
	filename   string
	dirPath    string
	fileFormat string
	recursive  bool
	verbose    bool
	minSize    int64
	maxSize    int64
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan directory for data files",
	Long: `Scan a directory and analyze data files
for quality metrics and statistics`,
	Run: func(cmd *cobra.Command, args []string) {
		if dirPath == "" {
			log.Printf("You must specify a directory with --dir")
			return
		}

		if filename != "" {
			specificFile := filepath.Join(dirPath, filename)
			if _, err := os.Stat(specificFile); os.IsNotExist(err) {
				log.Printf("File not found: %s", specificFile)
				return
			}

			profiler := profiler.NewCSVProfiler(specificFile)
			if err := profiler.Profile(); err != nil {
				log.Printf("Failed to profile %s: %v", specificFile, err)
				return
			}

			metrics := profiler.CalculateQuality()
			fmt.Printf("\nFile: %s\n", specificFile)
			fmt.Printf("- Rows: %d\n", profiler.RowCount)
			fmt.Printf("- Null Percentage: %.2f%%\n", metrics.NullPercentage*100)
			fmt.Printf("- Distinct Ratio: %.2f\n", metrics.DistinctRatio)

			// check for verbose flag
			if verbose {
				for _, stats := range profiler.ColumnStats {
					fmt.Printf("\nColumn: %s\n", stats.Name)
					fmt.Printf("  Type: %s\n", stats.Type)
					fmt.Printf("  Nulls: %d\n", stats.NullCount)
					fmt.Printf("  Distinct: %d\n", stats.DistinctCount)
					fmt.Printf("  Min: %s\n", stats.Min)
					fmt.Printf("  Max: %s\n", stats.Max)
				}
				return
			}
		}

		// First, count the files
		options := connectors.DiscoveryOptions{
			Recursive: recursive,
			MinSize:   minSize,
			MaxSize:   maxSize,
		}

		files, fileCount, err := connectors.DiscoverFiles(dirPath, fileFormat, options)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		// Now create the progress bar with the correct count
		bar := progressbar.NewOptions(fileCount,
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionSetDescription("[cyan][reset] Processing files..."),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				fmt.Println()
			}),
		)

		// Process the files with progress bar updates
		for _, file := range files {
			if file.IsDir {
				continue
			}

			bar.Add(1)

			profiler := profiler.NewCSVProfiler(file.Path)
			if err := profiler.Profile(); err != nil {
				log.Printf("Failed to profile %s: %v", file.Path, err)
				continue
			}

			metrics := profiler.CalculateQuality()
			fmt.Printf("\nFile: %s\n", file.Path)
			fmt.Printf("- Rows: %d\n", profiler.RowCount)
			fmt.Printf("- Null Value Percentage: %.2f%%\n", metrics.NullPercentage*100)
			fmt.Printf("- Distinct Row Ratio: %.2f\n", metrics.DistinctRatio)

			if verbose {
				for _, stats := range profiler.ColumnStats {
					fmt.Printf("\nColumn: %s\n", stats.Name)
					fmt.Printf("  Type: %s\n", stats.Type)
					fmt.Printf("  Nulls: %d\n", stats.NullCount)
					fmt.Printf("  Distinct: %d\n", stats.DistinctCount)
					fmt.Printf("  Min: %s\n", stats.Min)
					fmt.Printf("  Max: %s\n", stats.Max)
				}
			}
		}

		bar.Finish()
	},
}

func init() {
	rootCmd.AddCommand(scanCmd)
	scanCmd.Flags().StringVarP(&filename, "file", "n", "",
		"You might want to check specific file only")
	scanCmd.Flags().StringVarP(&dirPath, "dir", "d", "",
		"Directory to scan (required)")
	scanCmd.Flags().StringVarP(&fileFormat, "format", "f", "csv",
		"File format to analyze (csv, json)")
	scanCmd.Flags().BoolVarP(&recursive, "recursive", "r", false,
		"Search directories recursively")
	scanCmd.Flags().BoolVarP(&verbose, "verbose", "v", false,
		"Display detailed quality metrics")
	scanCmd.Flags().Int64Var(&minSize, "min-size", 0,
		"Minimum file size in bytes")
	scanCmd.Flags().Int64Var(&maxSize, "max-size", 0,
		"Maximum file size in bytes")

	scanCmd.MarkFlagRequired("dir")
}
