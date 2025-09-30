# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataQuality-CLI is a Go-based command-line tool for analyzing data quality metrics in CSV files. It provides exploratory data analysis capabilities including missing value detection, distinct counts, and column-level statistics.

## Architecture

The project follows a clean Go architecture with clear separation of concerns:

- **cmd/**: Cobra CLI commands and command-line interface
  - `root.go`: Main command configuration
  - `scan.go`: Core scanning functionality with file and directory processing
- **internal/profiler/**: Core data profiling logic
  - `profiler.go`: CSV profiler implementation with column-level analysis
  - `profiler_test.go`: Unit tests for profiling functionality
- **internal/connectors/**: File discovery and access layer
  - `filescanner.go`: Directory traversal and file filtering capabilities

## Build and Development Commands

```bash
# Build the binary
go build -o dataqa

# Run tests
go test ./...

# Run specific tests
go test ./internal/profiler/

# Run performance benchmarks
go test ./internal/profiler/ -bench=. -benchmem

# Run the tool
./dataqa scan --dir /path/to/csv/files
./dataqa scan --file specific.csv --dir /path/to/dir --verbose

# Run with scalability options for large files
./dataqa scan --file large.csv --dir . --sample-size 10000 --chunk-size 10000 --memory-limit 512

# Run with multiprocessing for maximum performance
./dataqa scan --dir /path/to/large/dataset --recursive --workers 8 --fast
```

## Key Components

### CSVProfiler (internal/profiler/profiler.go:113)
Main profiling engine that processes CSV files and calculates:
- Row counts and null percentages
- Column-level statistics with type inference (int, float, date, string)
- Distinct value ratios and sample values
- Min/max value detection
- Comprehensive statistics (Count, Mean, Std, Min, Max)

### File Discovery (internal/connectors/filescanner.go:28)
Handles file system traversal with filtering options:
- Recursive directory scanning
- File size filtering (min/max)
- Extension-based filtering
- Progress tracking support

### Quality Metrics (internal/profiler/profiler.go:173)
Calculates comprehensive quality indicators:
- Null percentage across all columns
- Type consistency metrics
- Distinct row ratios for data uniqueness assessment

### Parallel Processing Engine (cmd/scan.go)
High-performance processing system that:
- Auto-detects and utilizes all available CPU cores
- Processes multiple files concurrently using goroutines
- Implements configurable worker pools
- Provides load balancing across available processors

## Scalability Architecture

The tool has been optimized for handling very large files (GB+ scale) with bounded memory usage:

### Memory-Efficient Sampling
- **Reservoir Sampling**: Uses probabilistic sampling to estimate distinct counts without storing all unique values
- **Capture-Recapture Estimation**: Statistical methods for estimating cardinality in large datasets
- **Bounded Memory**: Configurable memory limits prevent OOM errors on large files
- **Fixed-Size Circular Buffers**: O(1) operations for statistical sampling

### Chunk-Based Processing
- **Stream Processing**: Files are processed in configurable chunks (default: 50,000 rows)
- **Progress Tracking**: Real-time progress reporting for long-running operations
- **Memory Management**: Chunked processing prevents loading entire files into memory
- **Optimized I/O**: Larger chunks reduce disk I/O overhead

### Parallel Processing
- **Auto CPU Detection**: Automatically uses all available CPU cores
- **Concurrent File Processing**: Multiple files processed simultaneously
- **Goroutine-Based**: Lightweight concurrency for maximum throughput
- **Load Balancing**: Intelligent distribution of files across workers

### Performance Configuration
- **Default Mode**: Auto-detected CPU cores, 50K chunks, 1K sample size, 512MB memory
- **Fast Mode**: Maximum workers, 200K chunks, 200 sample size for speed
- **Custom Tuning**: Manual configuration for specific hardware requirements

## Development Notes

- Uses Cobra for CLI framework with progress bars via `progressbar/v3`
- CSV processing uses Go's standard `encoding/csv` package with streaming readers
- Type inference supports integers, floats, dates, and strings
- File discovery supports both recursive and non-recursive scanning
- All file I/O operations include proper error handling and resource cleanup
- Memory usage is bounded regardless of input file size through sampling algorithms
- Progress callbacks enable monitoring of large file processing operations
- Parallel processing utilizes Go's lightweight goroutines for maximum performance
- Fixed-point arithmetic for optimized sampling operations