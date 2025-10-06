package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/peekknuf/DataQuality-CLI/internal/connectors"
	"github.com/peekknuf/DataQuality-CLI/internal/engine"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var (
	describeWorkers     int
	describeMemoryLimit int
	outputFile          string
	describeRecursive   bool
)

// Auto-detect optimal settings
func autoDetectSettings() (int, int) {
	// Detect CPU cores
	cpuCount := runtime.NumCPU()
	if describeWorkers == 0 {
		describeWorkers = cpuCount // Start with 1x CPU cores
	}

	// Detect memory limit (75% of available RAM, capped at 8GB, minimum 512MB)
	if describeMemoryLimit == 0 {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		availableMemMB := int(memStats.Sys / (1024 * 1024))
		detectedLimit := availableMemMB * 3 / 4 // 75% of available memory
		if detectedLimit > 8192 { // Cap at 8GB
			detectedLimit = 8192
		}
		if detectedLimit < 512 { // Minimum 512MB
			detectedLimit = 512
		}
		describeMemoryLimit = detectedLimit
	}



	return describeWorkers, describeMemoryLimit
}

type DescribeResult struct {
	Path            string
	RowCount        int
	ColumnStats     []ColumnStats
	NullPercentage  float64
	ProcessingTime  time.Duration
	Error           error
}

type ColumnStats struct {
	Name       string
	Type       string
	Count      int
	NullCount  int
	Mean       float64
	Std        float64
	Min        string
	Q25        float64
	Q50        float64
	Q75        float64
	Max        string
	Unique     int    // For string columns
	Top        string // For string columns
	Freq       int    // For string columns
}

var describeCmd = &cobra.Command{
	Use:   "describe [file or directory]",
	Short: "Generate comprehensive statistics for CSV files",
	Long: `Generate comprehensive statistics for CSV files with high performance.
Analyzes numeric, string, and datetime columns automatically.

Examples:
  dataqa describe file.csv                           # Single file, max performance
  dataqa describe /data/directory/ --recursive       # Directory processing
  dataqa describe file.csv --workers 4               # Limit CPU usage
  dataqa describe file.csv --memory-limit 2048       # Limit memory usage

  dataqa describe file.csv --output results.txt      # Save output`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Fatalf("Please specify a file or directory to describe")
		}

		targetPath := args[0]

		// Auto-detect optimal settings
		detectedWorkers, detectedMemory := autoDetectSettings()
		fmt.Printf("Auto-detected settings: %d workers, %dMB memory\n",
			detectedWorkers, detectedMemory)

		// Check if target is a file or directory
		fileInfo, err := os.Stat(targetPath)
		if err != nil {
			log.Fatalf("Error accessing %s: %v", targetPath, err)
		}

		if fileInfo.IsDir() {
			describeDirectory(targetPath, detectedWorkers, detectedMemory)
		} else {
			describeSingleFile(targetPath, detectedWorkers, detectedMemory)
		}
	},
}

func init() {
	rootCmd.AddCommand(describeCmd)

	// Simplified CLI flags
	describeCmd.Flags().IntVar(&describeWorkers, "workers", 0,
		"Number of parallel workers (default: auto-detect CPU cores)")
	describeCmd.Flags().IntVar(&describeMemoryLimit, "memory-limit", 0,
		"Memory limit in MB (default: auto-detect 75% RAM, capped at 8GB)")

	describeCmd.Flags().StringVar(&outputFile, "output", "",
		"Output file to save results (default: stdout)")
	describeCmd.Flags().BoolVar(&describeRecursive, "recursive", false,
		"Process directories recursively")
}

func describeSingleFile(filePath string, workers, memoryLimit int) {
	startTime := time.Now()

	// Check if file exists and is CSV
	if !strings.HasSuffix(strings.ToLower(filePath), ".csv") {
		log.Fatalf("File must be a CSV file: %s", filePath)
	}

	// Create progress bar
	progressBar := progressbar.NewOptions(-1,
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan][reset] Describing %s...", filepath.Base(filePath))),
		progressbar.OptionSetWidth(20),
	)

	result := processFileDescribe(filePath, memoryLimit, progressBar)
	progressBar.Finish()

	// Output results
	outputResults([]DescribeResult{result}, time.Since(startTime))
}

func describeDirectory(dirPath string, workers, memoryLimit int) {
	// Discover CSV files
	options := connectors.DiscoveryOptions{
		Recursive: describeRecursive,
		MinSize:   0,
		MaxSize:   0,
	}

	files, fileCount, err := connectors.DiscoverFiles(dirPath, "csv", options)
	if err != nil {
		log.Fatalf("Failed to discover files: %v", err)
	}

	if fileCount == 0 {
		fmt.Printf("No CSV files found in %s\n", dirPath)
		return
	}

	fmt.Printf("Found %d CSV files\n", fileCount)

	// Create progress bar
	progressBar := progressbar.NewOptions(fileCount,
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan][reset] Processing files...")),
		progressbar.OptionSetWidth(20),
	)

	// Process files in parallel
	startTime := time.Now()
	results := processFilesParallel(files, workers, memoryLimit, progressBar)
	progressBar.Finish()

	// Output results
	outputResults(results, time.Since(startTime))
}

func processFilesParallel(files []connectors.FileMeta, workers, memoryLimit int, progressBar *progressbar.ProgressBar) []DescribeResult {
	// Smart worker allocation based on I/O vs CPU bottlenecks
	cpuCount := runtime.NumCPU()
	totalSize := int64(0)
	largeFiles := 0
	
	for _, file := range files {
		if !file.IsDir {
			totalSize += file.Size
			if file.Size > 50*1024*1024 { // > 50MB (really large)
				largeFiles++
			}
		}
	}
	
	// Smart scaling: I/O work can benefit from more workers than CPU cores
	actualWorkers := workers
	
	// For massive datasets, we can use more workers for I/O parallelism
	if totalSize > 500*1024*1024 { // > 500MB total dataset
		actualWorkers = cpuCount * 2 // 2x CPU cores for massive I/O work
	}
	if largeFiles > 3 {
		actualWorkers = cpuCount * 2 // More I/O parallelism for many huge files
	}
	
	// Cap to reasonable limits
	maxWorkers := cpuCount * 3 // Maximum 3x CPU cores
	if maxWorkers > 32 {
		maxWorkers = 32 // Absolute maximum
	}
	if actualWorkers > maxWorkers {
		actualWorkers = maxWorkers
	}
	
	semaphore := make(chan struct{}, actualWorkers)
	results := make(chan DescribeResult, len(files))

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

			result := processFileDescribe(f.Path, memoryLimit, progressBar)
			results <- result
		}(file)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []DescribeResult
	for result := range results {
		allResults = append(allResults, result)
	}

	return allResults
}

func processFileDescribe(filePath string, memoryLimit int, progressBar *progressbar.ProgressBar) DescribeResult {
	// Use adaptive streaming engine - size-aware processing for large files
	adaptiveEngine := engine.NewAdaptiveStreamingEngine(filePath)

	// Process the file with optimal strategy based on size
	result := adaptiveEngine.Describe()

	// Convert engine result to describe result
	describeResult := DescribeResult{
		Path:           result.Path,
		RowCount:       result.RowCount,
		ColumnStats:    convertColumnStats(result.ColumnStats),
		NullPercentage: result.NullPercentage,
		ProcessingTime: result.ProcessingTime,
		Error:          result.Error,
	}

	progressBar.Add(1)
	return describeResult
}

// convertColumnStats converts engine.ColumnStats to cmd.ColumnStats
func convertColumnStats(engineStats []engine.ColumnStats) []ColumnStats {
	cmdStats := make([]ColumnStats, len(engineStats))
	for i, stat := range engineStats {
		cmdStats[i] = ColumnStats{
			Name:       stat.Name,
			Type:       stat.Type,
			Count:      stat.Count,
			NullCount:  stat.NullCount,
			Mean:       stat.Mean,
			Std:        stat.Std,
			Min:        stat.Min,
			Q25:        stat.Q25,
			Q50:        stat.Q50,
			Q75:        stat.Q75,
			Max:        stat.Max,
			Unique:     stat.Unique,
			Top:        stat.Top,
			Freq:       stat.Freq,
		}
	}
	return cmdStats
}

func outputResults(results []DescribeResult, totalTime time.Duration) {
	var output strings.Builder

	// Summary statistics first
	output.WriteString("=== DATA QUALITY SUMMARY ===\n")
	output.WriteString(fmt.Sprintf("Total files processed: %d\n", len(results)))
	output.WriteString(fmt.Sprintf("Total processing time: %v\n", totalTime))
	
	var totalRows, totalCols int
	var totalNulls int
	var numericCols, stringCols int
	
	for _, result := range results {
		if result.Error != nil {
			log.Printf("Failed to process %s: %v", result.Path, result.Error)
			continue
		}
		totalRows += result.RowCount
		totalCols += len(result.ColumnStats)
		totalNulls += int(float64(result.NullPercentage) * float64(result.RowCount*len(result.ColumnStats)) / 100.0)
		
		for _, col := range result.ColumnStats {
			if col.Type == "int" || col.Type == "float" {
				numericCols++
			} else {
				stringCols++
			}
		}
	}
	
	output.WriteString(fmt.Sprintf("Total rows processed: %d\n", totalRows))
	output.WriteString(fmt.Sprintf("Total columns analyzed: %d\n", totalCols))
	output.WriteString(fmt.Sprintf("Data completeness: %.1f%%\n", 100.0-(float64(totalNulls)/float64(totalRows*totalCols/len(results))*100.0)))
	output.WriteString(fmt.Sprintf("Numeric columns: %d, String columns: %d\n", numericCols, stringCols))
	output.WriteString("\n")

	// Per-file summary (more concise)
	output.WriteString("=== PER-FILE ANALYSIS ===\n")
	output.WriteString(fmt.Sprintf("%-40s %10s %10s %12s %12s %10s\n", "File", "Rows", "Columns", "Null Rate", "Process Time", "Data Quality"))
	output.WriteString(strings.Repeat("-", 100) + "\n")

	for _, result := range results {
		if result.Error != nil {
			continue
		}
		
		// Extract just filename from path
		filename := result.Path
		if lastSlash := strings.LastIndex(filename, "/"); lastSlash >= 0 {
			filename = filename[lastSlash+1:]
		}
		
		// Truncate long filenames
		if len(filename) > 37 {
			filename = filename[:34] + "..."
		}
		
		quality := "Good"
		if result.NullPercentage > 10 {
			quality = "Fair"
		}
		if result.NullPercentage > 25 {
			quality = "Poor"
		}
		
		output.WriteString(fmt.Sprintf("%-40s %10d %10d %11.1f%% %11s %10s\n",
			filename, result.RowCount, len(result.ColumnStats),
			result.NullPercentage, result.ProcessingTime.Round(time.Millisecond), quality))
	}
	
	output.WriteString("\n")

	// Detailed analysis for files with issues (high null rates, interesting patterns)
	output.WriteString("=== DETAILED ANALYSIS ===\n")
	detailedShown := 0
	for _, result := range results {
		if result.Error != nil || detailedShown >= 3 {
			continue
		}
		
		// Show details for files with interesting characteristics
		showDetails := result.NullPercentage > 5 || result.RowCount > 100000 || len(result.ColumnStats) > 20
		
		if showDetails && detailedShown < 3 {
			filename := result.Path
			if lastSlash := strings.LastIndex(filename, "/"); lastSlash >= 0 {
				filename = filename[lastSlash+1:]
			}
			
			output.WriteString(fmt.Sprintf("File: %s\n", filename))
			output.WriteString(fmt.Sprintf("  Rows: %d | Columns: %d | Null Rate: %.1f%%\n", 
				result.RowCount, len(result.ColumnStats), result.NullPercentage))
			
			// Show key insights
			var highNullCols, numericColsWithNulls int
			for _, col := range result.ColumnStats {
				nullRate := float64(col.NullCount) / float64(col.Count) * 100
				if nullRate > 10 {
					highNullCols++
				}
				if (col.Type == "int" || col.Type == "float") && col.NullCount > 0 {
					numericColsWithNulls++
				}
			}
			
			if highNullCols > 0 {
				output.WriteString(fmt.Sprintf("  ⚠️  %d columns have >10%% null values\n", highNullCols))
			}
			if numericColsWithNulls > 0 {
				output.WriteString(fmt.Sprintf("  ⚠️  %d numeric columns contain nulls\n", numericColsWithNulls))
			}
			
			// Show sample of interesting columns
			interestingCols := 0
			output.WriteString("  Key columns:\n")
			for _, col := range result.ColumnStats {
				if interestingCols >= 3 {
					break
				}
				if col.Type == "float" && col.Mean > 0 {
					output.WriteString(fmt.Sprintf("    • %s: %s (avg: %.2f)\n", col.Name, col.Type, col.Mean))
					interestingCols++
				} else if col.Type == "string" && col.Unique > 100 {
					output.WriteString(fmt.Sprintf("    • %s: %s (%d unique values)\n", col.Name, col.Type, col.Unique))
					interestingCols++
				}
			}
			output.WriteString("\n")
			detailedShown++
		}
	}

	// Write to output file or stdout
	if outputFile != "" {
		err := os.WriteFile(outputFile, []byte(output.String()), 0644)
		if err != nil {
			log.Fatalf("Failed to write to output file %s: %v", outputFile, err)
		}
		fmt.Printf("Results saved to %s\n", outputFile)
	} else {
		fmt.Print(output.String())
	}
}