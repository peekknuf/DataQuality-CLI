package cmd

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/dustin/go-humanize"
	"github.com/peekknuf/dataqa/internal/connectors"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
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
		// Count total files/directories
		totalItems := 0
		filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Handle errors during counting
			}
			totalItems++
			return nil
		})

		bar := progressbar.NewOptions(totalItems, // -1 = unknown total
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

		options := connectors.DiscoveryOptions{
			Recursive: recursive,
			MinSize:   minSize,
			MaxSize:   maxSize,
			Progress: func(path string, d fs.DirEntry, err error) error {
				bar.Add(1) // Increment progress
				return nil
			},
		}

		files, err := connectors.DiscoverFiles(dirPath, fileFormat, options)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		bar.Finish()

		fmt.Printf("\nFound %d %s files:\n", len(files), fileFormat)
		for _, f := range files {
			fmt.Printf("- %s (%s, modified: %s)\n",
				f.Path,
				humanize.Bytes(uint64(f.Size)),
				f.Modified.Format("2006-01-02 15:04:05"))
		}
		fmt.Println()
	},
}

func init() {
	rootCmd.AddCommand(scanCmd)

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
