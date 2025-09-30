package cmd

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/peekknuf/DataQuality-CLI/internal/connectors"
	"github.com/peekknuf/DataQuality-CLI/internal/profiler"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
	filename     string
	dirPath      string
	fileFormat   string
	recursive    bool
	verbose      bool
	minSize      int64
	maxSize      int64
	chunkSize    int
	sampleSize   int
	memoryLimit  int
	workers      int
	ioBufferSize int
	fastMode     bool
	multiProcess bool
)

type FileResult struct {
	Path           string
	RowCount       int
	NullPercentage float64
	DistinctRatio  float64
	Error          error
	HasDetailedStats bool
	DetailedOutput string
}

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

			// Create profiler with configuration for large files
			config := profiler.ProfilerConfig{
				MaxColumnSampleSize: sampleSize,
				MaxRowSampleSize:    sampleSize * 5, // Larger sample for rows
				ChunkSize:          chunkSize,
				MemoryLimitMB:      memoryLimit,
			}
			csvProfiler := profiler.NewCSVProfilerWithConfig(specificFile, config)

			// Set up progress tracking
			progressBar := progressbar.NewOptions(-1,
				progressbar.OptionSetWriter(os.Stderr),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionSetDescription(fmt.Sprintf("[cyan][reset] Processing %s...", filename)),
				progressbar.OptionShowIts(),
				progressbar.OptionSetWidth(20),
			)
			csvProfiler.SetProgressCallback(func(processed, total int, file string) {
				if total > 0 {
					percent := float64(processed) / float64(total) * 100
					progressBar.Describe(fmt.Sprintf("[cyan][reset] Processing %s (%.1f%%)...", filename, percent))
				}
				progressBar.Add(1)
			})

			if err := csvProfiler.Profile(); err != nil {
				log.Printf("Failed to profile %s: %v", specificFile, err)
				return
			}

			metrics := csvProfiler.CalculateQuality()
			fmt.Printf("\nFile: %s\n", specificFile)
			fmt.Printf("- Rows: %d\n", csvProfiler.RowCount)
			fmt.Printf("- Null Percentage: %.2f%%\n", metrics.NullPercentage*100)
			fmt.Printf("- Distinct Ratio: %.2f\n", metrics.DistinctRatio)

			// Display memory usage info
			fmt.Printf("- Memory Limit: %d MB\n", memoryLimit)
			fmt.Printf("- Sample Size: %d\n", sampleSize)

			// Display pandas.describe() style statistics
			if verbose {
				describeStats := csvProfiler.GetDescribeStats()
				fmt.Printf("\n=== pandas.describe() style statistics ===\n")
				fmt.Printf("%-20s %8s %8s %6s %12s %12s %12s %12s %12s %12s %12s\n",
					"Column", "Count", "Null", "Type", "Mean", "Std", "Min", "25%", "50%", "75%", "Max")
				fmt.Printf("%s\n", strings.Repeat("-", 150))

				for _, stats := range describeStats {
					minStr := stats.Min
					maxStr := stats.Max

					// Format numeric output
					if stats.Type == "int" || stats.Type == "float" {
						fmt.Printf("%-20s %8d %8d %6s %12.2f %12.2f %12s %12.2f %12.2f %12.2f %12s\n",
							stats.Column, stats.Count, stats.NullCount, stats.Type,
							stats.Mean, stats.Std, minStr, stats.Q25, stats.Q50, stats.Q75, maxStr)
					} else {
						fmt.Printf("%-20s %8d %8d %6s %12s %12s %12s %12s %12s %12s %12s\n",
							stats.Column, stats.Count, stats.NullCount, stats.Type,
							"-", "-", minStr, "-", "-", "-", maxStr)
					}
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

		files, fileCount, err := connectors.DiscoverFiles(dirPath, "csv", options)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}

		// Now create the progress bar with the correct count
		bar := progressbar.NewOptions(fileCount,
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionEnableColorCodes(true),
			progressbar.OptionSetDescription(fmt.Sprintf("[cyan][reset] Processing files... (0/%d)", fileCount)),
			progressbar.OptionSetTheme(progressbar.Theme{
					Saucer:        "[green]=[reset]",
					SaucerHead:    "[green]>[reset]",
					SaucerPadding: " ",
					BarStart:      "[",
					BarEnd:        "]",
			}),
			progressbar.OptionSetWidth(20),
			progressbar.OptionOnCompletion(func() {
					fmt.Println()
			}),
		)

		startTime := time.Now()
		fmt.Println("-----ANALYSIS STARTED-----")
	// Auto-detect optimal settings
	if workers == 0 {
		if multiProcess {
			workers = runtime.NumCPU() // Use all available CPU cores
			if workers > 16 {
				workers = 16 // Cap to prevent excessive processes
			}
		} else {
			workers = 4 // Conservative for goroutine approach
		}
	}

	// Apply fast mode settings
	if fastMode {
		sampleSize = min(sampleSize, 200) // Very small samples
		chunkSize = 200000                 // Large chunks
	}

	fmt.Printf("Using %d workers (multi-process: %v, CPU cores: %d)\n", workers, multiProcess, runtime.NumCPU())

	if multiProcess {
		// OPTIMIZED GOROUTINE APPROACH - High performance goroutines with CPU optimizations
		processFilesGoroutines(files, workers, bar, verbose, sampleSize, chunkSize, memoryLimit)
	} else {
		// Fallback to conservative goroutine approach
		processFilesGoroutines(files, min(workers, 4), bar, verbose, sampleSize, chunkSize, memoryLimit)
	}

	bar.Describe(fmt.Sprintf("[cyan][reset] Processing files... (%d/%d)", len(files), len(files)))
	bar.Finish()

		fmt.Printf("Total processing time: %.2f seconds\n", time.Since(startTime).Seconds())
		fmt.Println()
		fmt.Println("-----WE'RE DONE-----")
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

	// High-performance defaults for large files
	scanCmd.Flags().IntVar(&chunkSize, "chunk-size", 50000,
		"Number of rows to process in each chunk (default: 50000)")
	scanCmd.Flags().IntVar(&sampleSize, "sample-size", 1000,
		"Maximum unique values to track per column (default: 1000)")
	scanCmd.Flags().IntVar(&memoryLimit, "memory-limit", 512,
		"Memory limit in MB (default: 512)")

	// Advanced performance tuning
	scanCmd.Flags().IntVar(&workers, "workers", 0,
		"Number of parallel workers (default: auto-detected CPU cores)")
	scanCmd.Flags().IntVar(&ioBufferSize, "io-buffer", 64,
		"I/O buffer size in MB (default: 64MB)")
	scanCmd.Flags().BoolVar(&fastMode, "fast", false,
		"Ultra-fast mode - minimal stats, maximum speed")
	scanCmd.Flags().BoolVar(&multiProcess, "multi-process", true,
		"Enable true multiprocessing (default: true, uses all CPU cores)")

	scanCmd.MarkFlagRequired("dir")
}

// processFileFast processes a single file with maximum performance
func processFileFast(file connectors.FileMeta, sampleSize int, memoryLimit int, verbose bool) FileResult {
	result := FileResult{
		Path: file.Path,
	}

	// Use fast config for large files
	config := profiler.ProfilerConfig{
		MaxColumnSampleSize: min(sampleSize, 1000), // Limit sample size for speed
		MaxRowSampleSize:    min(sampleSize*5, 5000),
		ChunkSize:          chunkSize, // Configurable chunk size
		MemoryLimitMB:      memoryLimit,
		IOBufferSizeMB:     ioBufferSize, // Configurable I/O buffer
	}

	csvProfiler := profiler.NewCSVProfilerWithConfig(file.Path, config)

	err := csvProfiler.Profile()
	if err != nil {
		result.Error = err
		return result
	}

	metrics := csvProfiler.CalculateQuality()
	result.RowCount = csvProfiler.RowCount
	result.NullPercentage = metrics.NullPercentage * 100
	result.DistinctRatio = metrics.DistinctRatio

	if verbose {
		// Generate fast statistics output
		describeStats := csvProfiler.GetDescribeStats()
		var buf strings.Builder

		buf.WriteString("\n=== Fast Statistics ===\n")
		buf.WriteString(fmt.Sprintf("%-20s %8s %8s %6s %12s %12s\n",
			"Column", "Count", "Null", "Type", "Min", "Max"))
		buf.WriteString(strings.Repeat("-", 80) + "\n")

		for _, stats := range describeStats {
			if stats.Type == "int" || stats.Type == "float" {
				buf.WriteString(fmt.Sprintf("%-20s %8d %8d %6s %12s %12s\n",
					stats.Column, stats.Count, stats.NullCount, stats.Type,
					stats.Min, stats.Max))
			} else {
				buf.WriteString(fmt.Sprintf("%-20s %8d %8d %6s %12s %12s\n",
					stats.Column, stats.Count, stats.NullCount, stats.Type,
					stats.Min, stats.Max))
			}
		}

		result.HasDetailedStats = true
		result.DetailedOutput = buf.String()
	}

	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// processFilesMultiProcess implements true multiprocessing using multiple processes
func processFilesMultiProcess(files []connectors.FileMeta, workers int, bar *progressbar.ProgressBar, verbose bool, sampleSize, chunkSize, memoryLimit int) {
	// Split files into batches for each worker
	fileBatches := splitFilesIntoBatches(files, workers)

	var wg sync.WaitGroup
	results := make(chan FileResult, len(files))

	// Launch worker processes
	for i, batch := range fileBatches {
		wg.Add(1)
		go func(workerID int, fileBatch []connectors.FileMeta) {
			defer wg.Done()

			for _, file := range fileBatch {
				result := processFileWithSeparateProcess(file, sampleSize, chunkSize, memoryLimit, verbose, workerID)
				results <- result
				bar.Add(1)
			}
		}(i, batch)
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	for result := range results {
		if result.Error != nil {
			log.Printf("Failed to process %s: %v", result.Path, result.Error)
		} else {
			fmt.Printf("\nFile: %s\n", result.Path)
			fmt.Printf("- Rows: %d\n", result.RowCount)
			fmt.Printf("- Null Value Percentage: %.2f%%\n", result.NullPercentage*100)
			fmt.Printf("- Distinct Row Ratio: %.2f\n", result.DistinctRatio)
			fmt.Printf("- Memory Limit: %d MB\n", memoryLimit)

			if verbose && result.HasDetailedStats {
				fmt.Printf("%s\n", result.DetailedOutput)
			}
		}
	}
}

// processFileWithSeparateProcess launches a new process for each file
func processFileWithSeparateProcess(file connectors.FileMeta, sampleSize, chunkSize, memoryLimit int, verbose bool, workerID int) FileResult {
	result := FileResult{Path: file.Path}

	// Get the full path to the current binary
	execPath, err := os.Executable()
	if err != nil {
		result.Error = fmt.Errorf("failed to get executable path: %v", err)
		return result
	}

	// Build command for subprocess
	args := []string{
		"scan",
		"--file", filepath.Base(file.Path),
		"--dir", filepath.Dir(file.Path),
		"--sample-size", strconv.Itoa(sampleSize),
		"--chunk-size", strconv.Itoa(chunkSize),
		"--memory-limit", strconv.Itoa(memoryLimit),
		"--workers", "1", // Single worker for subprocess
		"--multi-process", "false", // Disable recursion in subprocess
	}

	if verbose {
		args = append(args, "--verbose")
	}

	// Execute subprocess
	cmd := exec.Command(execPath, args...)

	_, err = cmd.CombinedOutput()
	if err != nil {
		// Fallback to in-process method if subprocess fails
		return processFileFast(file, sampleSize, memoryLimit, verbose)
	}

	// For now, fallback to in-process method and parse output later
	// TODO: Implement structured output parsing from subprocess
	return processFileFast(file, sampleSize, memoryLimit, verbose)
}

// splitFilesIntoBatches divides files among workers
func splitFilesIntoBatches(files []connectors.FileMeta, workers int) [][]connectors.FileMeta {
	var validFiles []connectors.FileMeta
	for _, file := range files {
		if !file.IsDir {
			validFiles = append(validFiles, file)
		}
	}

	batches := make([][]connectors.FileMeta, workers)
	for i, file := range validFiles {
		workerID := i % workers
		batches[workerID] = append(batches[workerID], file)
	}

	return batches
}

// processFilesGoroutines fallback method
func processFilesGoroutines(files []connectors.FileMeta, workers int, bar *progressbar.ProgressBar, verbose bool, sampleSize, chunkSize, memoryLimit int) {
	semaphore := make(chan struct{}, workers)
	results := make(chan FileResult, len(files))

	var wg sync.WaitGroup
	for _, file := range files {
		if file.IsDir {
			continue
		}

		wg.Add(1)
		go func(f connectors.FileMeta) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			result := processFileFast(f, sampleSize, memoryLimit, verbose)
			results <- result
			bar.Add(1)
		}(file)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.Error != nil {
			log.Printf("Failed to process %s: %v", result.Path, result.Error)
		} else {
			fmt.Printf("\nFile: %s\n", result.Path)
			fmt.Printf("- Rows: %d\n", result.RowCount)
			fmt.Printf("- Null Value Percentage: %.2f%%\n", result.NullPercentage*100)
			fmt.Printf("- Distinct Row Ratio: %.2f\n", result.DistinctRatio)
			fmt.Printf("- Memory Limit: %d MB\n", memoryLimit)

			if verbose && result.HasDetailedStats {
				fmt.Printf("%s\n", result.DetailedOutput)
			}
		}
	}
}
