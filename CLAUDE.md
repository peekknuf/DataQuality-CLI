# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataQuality-CLI is a high-performance Go-based command-line tool for analyzing data quality metrics in CSV and structured data files. It provides exploratory data analysis capabilities including missing value detection, distinct counts, column-level statistics, and comprehensive quality assessments with 10x+ performance improvements over traditional approaches.

## Architecture

The project follows a clean Go architecture with clear separation of concerns and modular design:

- **cmd/**: Cobra CLI commands and command-line interface
  - `root.go`: Main command configuration and CLI setup
  - `scan.go`: Core scanning functionality with file and directory processing

- **internal/engine/**: High-performance processing engine
  - `high_performance_engine.go`: Main processing engine with SIMD and parallel optimizations
  - Integration tests for engine performance and functionality

- **internal/quality/**: Comprehensive quality assessment framework
  - `framework.go`: Core quality check interfaces and result structures
  - `type_inference.go`: Data type detection and validation
  - `null_analysis.go`: Missing value analysis and pattern detection
  - `duplicate_detection.go`: Duplicate record identification
  - `format_validation.go`: Data format validation (emails, dates, etc.)
  - `outlier_detection.go`: Statistical outlier detection
  - `consistency_checks.go`: Data consistency validation
  - `metrics_aggregator.go`: Quality metrics aggregation and scoring
  - `integration.go`: Quality framework integration with processing engine

- **internal/profiler/**: Core data profiling logic
  - `profiler.go`: Original CSV profiler implementation with column-level analysis
  - `high_performance_profiler.go`: Optimized profiler with performance enhancements
  - Comprehensive test suites including scalability benchmarks

- **internal/parser/**: Optimized CSV parsing
  - `csv_parser.go`: High-performance CSV parser with memory-mapped I/O

- **internal/processing/**: Parallel processing engine
  - `parallel_engine.go`: Work-stealing scheduler and parallel task management

- **internal/memory/**: Memory management and optimization
  - `pool_manager.go`: Object pooling for memory efficiency

- **internal/io/**: I/O optimization layer
  - `mmap_reader.go`: Memory-mapped file reading for performance

- **internal/stats/**: Statistical computation with SIMD
  - `simd_stats.go`: SIMD-accelerated statistical calculations

- **internal/performance/**: Performance monitoring and reporting
  - `reporter.go`: Performance metrics collection and reporting

- **internal/connectors/**: File discovery and access layer
  - `filescanner.go`: Directory traversal and file filtering capabilities

## Build and Development Commands

```bash
# Build the binary
go build -o dataqa main.go

# Run tests
go test ./...

# Run specific package tests
go test ./internal/quality/
go test ./internal/engine/
go test ./internal/profiler/
go test ./internal/stats/

# Run all integration tests
go test ./test/

# Run performance benchmarks
go test ./benchmark/ -bench=. -benchmem -v

# Run specific benchmark categories
go test ./benchmark/ -bench=BenchmarkCSVParser -benchmem
go test ./benchmark/ -bench=BenchmarkSIMD -benchmem
go test ./benchmark/ -bench=BenchmarkParallel -benchmem
go test ./benchmark/ -bench=BenchmarkQuality -benchmem
go test ./benchmark/ -bench=BenchmarkMmap -benchmem

# Run scalability tests
go test ./internal/profiler/ -run=Scalability -v

# Run the tool
./dataqa scan --dir /path/to/csv/files
./dataqa scan --file specific.csv --dir /path/to/dir --verbose

# Run with high-performance options for large files
./dataqa scan --file large.csv --dir . --sample-size 10000 --chunk-size 100000 --memory-limit 2048 --workers 8

# Run with maximum performance configuration
./dataqa scan --dir /path/to/large/dataset --recursive --workers $(nproc) --multi-process --fast

# Run with custom performance tuning
./dataqa scan --dir /data --memory-limit 4096 --chunk-size 200000 --io-buffer 128 --fast
```

## Key Components

### High-Performance Engine (internal/engine/high_performance_engine.go)
Main processing engine with advanced optimizations:
- SIMD-accelerated statistical computations
- Memory-mapped I/O for efficient file access
- Work-stealing scheduler for parallel processing
- Configurable worker pools and load balancing
- Real-time performance monitoring and reporting

### Quality Assessment Framework (internal/quality/framework.go)
Comprehensive quality check system with modular architecture:
- **QualityCheck Interface**: Extensible quality check framework
- **Type Inference**: Automatic data type detection and validation
- **Null Analysis**: Pattern detection in missing values
- **Duplicate Detection**: Statistical duplicate identification
- **Format Validation**: Email, date, phone number format checking
- **Outlier Detection**: Statistical outlier identification using IQR and Z-score
- **Consistency Checks**: Cross-column and cross-table consistency validation
- **Metrics Aggregation**: Composite quality scoring and reporting

### High-Performance Profiler (internal/profiler/high_performance_profiler.go)
Optimized profiling engine with performance enhancements:
- Bounded memory usage regardless of file size
- Reservoir sampling for distinct value estimation
- Chunk-based processing for memory efficiency
- Parallel column processing
- Real-time progress tracking

### SIMD Statistics Engine (internal/stats/simd_stats.go)
Vectorized statistical computations:
- Parallel mean, standard deviation, and quantile calculations
- Memory-efficient operations using CPU SIMD instructions
- Fixed-point arithmetic for optimized sampling
- Bounded-error approximations for large datasets

### Memory Management (internal/memory/pool_manager.go)
Advanced memory optimization:
- Object pooling for reduced garbage collection pressure
- Memory-mapped file buffers for efficient I/O
- Configurable memory limits and pool sizing
- Automatic memory pressure detection and response

### Parallel Processing Engine (internal/processing/parallel_engine.go)
Work-stealing scheduler and task management:
- Dynamic load balancing across CPU cores
- Configurable worker pools and queue sizes
- Automatic CPU detection and optimization
- Goroutine-based lightweight concurrency

### File Discovery (internal/connectors/filescanner.go)
Enhanced file system traversal with filtering:
- Recursive directory scanning with progress tracking
- File size and extension-based filtering
- Parallel file discovery for large directories
- Integration with parallel processing engine

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

## Performance Architecture

Data Quality CLI achieves 10x+ performance improvements through multiple optimization layers:

### SIMD Optimization Layer
- **Vectorized Operations**: Statistical calculations using CPU SIMD instructions
- **Parallel Processing**: Column-wise and row-wise parallel computation
- **Memory Efficiency**: Reduced memory bandwidth usage through vectorization
- **Fixed-Point Arithmetic**: Optimized sampling operations with bounded error

### Memory Optimization Layer
- **Object Pooling**: 50-70% reduction in memory allocations
- **Memory-Mapped I/O**: 3-5x faster file access through OS memory mapping
- **Bounded Memory Usage**: Configurable memory limits prevent OOM errors
- **Garbage Collection Optimization**: Reduced GC pressure through pooling strategies

### Parallel Processing Layer
- **Work-Stealing Scheduler**: Dynamic load balancing with 90%+ CPU utilization
- **Goroutine Pools**: Lightweight concurrency for maximum throughput
- **Auto CPU Detection**: Automatic utilization of all available CPU cores
- **Chunk-Based Processing**: Efficient processing of large datasets in memory

### I/O Optimization Layer
- **Memory-Mapped Files**: Efficient file access with minimal copying
- **Chunked Reading**: Optimized I/O patterns for large files
- **Buffered Operations**: Configurable I/O buffer sizes
- **Parallel File Discovery**: Concurrent directory scanning

### Statistical Algorithm Layer
- **Reservoir Sampling**: O(1) memory distinct value estimation
- **Capture-Recapture**: Statistical cardinality estimation
- **Approximate Algorithms**: Fast estimation with bounded error
- **Streaming Statistics**: Real-time statistical computation

## Development Notes

- Uses Cobra for CLI framework with progress bars via `progressbar/v3`
- Go 1.23.4 required for latest language features and optimizations
- Comprehensive test coverage including unit, integration, and benchmark tests
- Memory usage is bounded regardless of input file size through advanced sampling
- All components support concurrent execution with proper synchronization
- Performance monitoring and reporting built into the core engine
- Extensible quality framework allows custom quality checks
- SIMD optimizations require proper CPU feature detection
- Error handling and resource cleanup throughout all components
- Configuration through CLI flags, environment variables, and config files