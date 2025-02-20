package cmd

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/peekknuf/dataqa/internal/connectors"
	"github.com/peekknuf/dataqa/internal/profiler" // Import the profiler package
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
	filename   string
	dirPath    string
	fileFormat string
	recursive  bool
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
			log.Fatal("You must specify a directory with --dir")
		}

		// Check if a specific file is provided based on the -n flag
		if filename != "" {
			specificFile := filepath.Join(dirPath, filename)
			if _, err := os.Stat(specificFile); os.IsNotExist(err) {
				log.Fatalf("File not found: %s", specificFile)
			}

			// Process the specific file
			profiler := profiler.NewCSVProfiler(specificFile)
			if err := profiler.Profile(); err != nil {
				log.Fatalf("Failed to profile %s: %v", specificFile, err)
			}

			// Calculate and display quality metrics
			metrics := profiler.CalculateQuality()
			fmt.Printf("\nFile: %s\n", specificFile)
			fmt.Printf("- Rows: %d\n", profiler.RowCount)
			fmt.Printf("- Null Percentage: %.2f%%\n", metrics.NullPercentage*100)
			fmt.Printf("- Distinct Ratio: %.2f\n", metrics.DistinctRatio)

			// Display column stats
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

		// Count total files/directories
		totalItems := 0
		filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Handle errors during counting
			}
			totalItems++
			return nil
		})

		// Initialize progress bar
		bar := progressbar.NewOptions(totalItems,
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionSetDescription("[cyan][1/3][reset] Scanning files..."),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "[green]=[reset]",
				SaucerHead:    "[green]>[reset]",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				fmt.Println() // Newline after completion
			}),
		)

		// Set up progress callback
		options := connectors.DiscoveryOptions{
			Recursive: recursive,
			MinSize:   minSize,
			MaxSize:   maxSize,
			Progress: func(path string, d fs.DirEntry, err error) error {
				bar.Add(1) // Increment progress
				return nil
			},
		}

		// Perform the scan
		files, err := connectors.DiscoverFiles(dirPath, fileFormat, options)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		// Process each file with the profiler
		for _, file := range files {
			if file.IsDir {
				continue // Skip directories
			}

			// Initialize and run the profiler
			profiler := profiler.NewCSVProfiler(file.Path)
			if err := profiler.Profile(); err != nil {
				log.Printf("Failed to profile %s: %v", file.Path, err)
				continue
			}

			// Calculate and display quality metrics
			metrics := profiler.CalculateQuality()
			fmt.Printf("\nFile: %s\n", file.Path)
			fmt.Printf("- Rows: %d\n", profiler.RowCount)
			fmt.Printf("- Null Percentage: %.2f%%\n", metrics.NullPercentage*100)
			fmt.Printf("- Distinct Ratio: %.2f\n", metrics.DistinctRatio)

			// Display column stats
			for _, stats := range profiler.ColumnStats {
				fmt.Printf("\nColumn: %s\n", stats.Name)
				fmt.Printf("  Type: %s\n", stats.Type)
				fmt.Printf("  Nulls: %d\n", stats.NullCount)
				fmt.Printf("  Distinct: %d\n", stats.DistinctCount)
				fmt.Printf("  Min: %s\n", stats.Min)
				fmt.Printf("  Max: %s\n", stats.Max)
			}
		}

		// Ensure the progress bar is complete
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
	scanCmd.Flags().Int64Var(&minSize, "min-size", 0,
		"Minimum file size in bytes")
	scanCmd.Flags().Int64Var(&maxSize, "max-size", 0,
		"Maximum file size in bytes")

	scanCmd.MarkFlagRequired("dir")
}
