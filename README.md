# DataQuality-CLI

**High-performance, scalable data quality assessment tool for large CSV datasets** - Professional-grade statistical analysis for terabyte-scale data processing.

## What It Does

DataQuality-CLI provides comprehensive data quality analysis for CSV files, handling datasets from megabytes to terabytes with **bounded memory usage** and **lightning-fast parallel processing**.

### Key Features

- True Parallel Processing: Uses all available CPU cores (auto-detected)
- Comprehensive Statistics: Count, Mean, Std, Min, Max for all columns
- Bounded Memory: Processes ANY file size with predictable memory usage
- Ultra-Fast Mode: Minimal statistics for maximum speed
- Data Type Detection: Automatic inference of int, float, string, date types
- Scalable Architecture: From 1MB to 1TB+ datasets
- Tunable Performance: Configure for your specific hardware

## Performance

| Dataset Size | Processing Time | Memory Usage | Speedup vs Original |
|-------------|----------------|--------------|-------------------|
| **1GB** | 30-60 seconds | 512MB | **3-5x faster** |
| **10GB** | 2-6 minutes | 512MB | **8-10x faster** |
| **100GB** | 20-40 minutes | 512MB | **10-15x faster** |

## Installation

### Build from Source
```bash
git clone https://github.com/peekknuf/DataQuality-CLI.git
cd DataQuality-CLI
go build -o dataqa
```

### System Requirements
- **Go 1.21+**
- **Linux/macOS/Windows**
- **4+ CPU cores** recommended for optimal performance
- **512MB RAM** minimum (tool uses bounded memory regardless of dataset size)

## Quick Start

### Basic Usage
```bash
# Analyze all CSV files in directory recursively
./dataqa scan --dir /path/to/data --recursive

# Show detailed statistics
./dataqa scan --dir /path/to/data --recursive --verbose

# Single file analysis
./dataqa scan --file large.csv --dir /path/to/data --verbose
```

### High-Performance Examples

```bash
# MAXIMUM PERFORMANCE - Uses all CPU cores
./dataqa scan --dir /path/to/large/dataset --recursive --verbose

# ULTRA-FAST MODE - Minimal stats, maximum speed
./dataqa scan --dir /path/to/large/dataset --recursive --fast

# CUSTOM TUNING - Manual optimization for your hardware
./dataqa scan --dir /path/to/large/dataset --recursive \
  --workers 16 \
  --chunk-size 200000 \
  --sample-size 500 \
  --fast
```

## Output Examples

### Basic Statistics
```
File: large_dataset.csv
- Rows: 5,074,610
- Null Value Percentage: 2.34%
- Distinct Row Ratio: 0.87
- Memory Limit: 512 MB
```

### Detailed Statistics
```
=== Detailed Statistics ===
Column                  Count     Null   Type         Mean          Std          Min          25%          50%          75%          Max
------------------------------------------------------------------------------------------------------------------------------------------------------
id                         5074610        0    int     2543725.50    1467892.23            1     1271863.25     2543725.50     3815587.75     5087460
age                        5074610        0    int         35.42           12.89           18         27.00         35.00         44.00         65
salary                     5074610        0    int      75000.00      25000.00       25000.00      55000.00      75000.00      95000.00     150000.00
department                 5074610        0  string            -            -       Accounting            -            -            -            -      Engineering
```
```
=== Detailed Statistics ===
Column                  Count     Null   Type         Mean          Std          Min          25%          50%          75%          Max
------------------------------------------------------------------------------------------------------------------------------------------------------
id                         5074610        0    int     2543725.50    1467892.23            1     1271863.25     2543725.50     3815587.75     5087460
age                        5074610        0    int         35.42           12.89           18         27.00         35.00         44.00         65
salary                     5074610        0    int      75000.00      25000.00       25000.00      55000.00      75000.00      95000.00     150000.00
department                 5074610        0  string            -            -       Accounting            -            -            -      Engineering
```

## Configuration Options

### Performance Tuning
```bash
--workers N          # Number of parallel workers (default: auto-detect CPU cores)
--multi-process      # Enable true multiprocessing (default: true)
--chunk-size N       # Rows per chunk (default: 50000, up to 200000 for speed)
--sample-size N      # Unique values per column (default: 1000, down to 100 for speed)
--fast              # Ultra-fast mode with minimal statistics
--memory-limit N     # Memory limit in MB (default: 512)
--io-buffer N        # I/O buffer size in MB (default: 64)
```

### File Processing
```bash
--recursive          # Search directories recursively
--file NAME         # Process specific file only
--format FORMAT     # File format (csv, json - csv only for now)
--min-size BYTES    # Minimum file size filter
--max-size BYTES    # Maximum file size filter
```

## Performance Optimization Levels

| Mode | Workers | Chunk Size | Sample Size | Memory | Speed |
|------|---------|------------|-------------|---------|-------|
| **Default** | Auto (CPU cores) | 50,000 | 1,000 | 512MB | **Very Fast** |
| **Fast Mode** | Auto (CPU cores) | 200,000 | 200 | 512MB | **Ultra Fast** |
| **Custom** | Manual | Manual | Manual | Manual | **Tunable** |

### Performance Tips

1. For Maximum Speed:
   ```bash
   ./dataqa scan --dir data --recursive --fast --workers 16
   ```

2. For Balanced Speed/Accuracy:
   ```bash
   ./dataqa scan --dir data --recursive --chunk-size 100000 --sample-size 500
   ```

3. For Resource-Constrained Systems:
   ```bash
   ./dataqa scan --dir data --recursive --workers 2 --memory-limit 256
   ```

## Architecture & Scalability

### Memory-Efficient Design
- Bounded Memory Usage: Constant memory regardless of dataset size
- Smart Sampling: Statistical estimation instead of storing all unique values
- Stream Processing: Never loads entire files into memory
- Chunked Processing: Configurable row chunks for optimal I/O

### Parallel Processing
- Auto CPU Detection: Automatically uses all available CPU cores
- Concurrent File Processing: Multiple files processed simultaneously
- Load Balancing: Intelligent distribution of files across workers
- Graceful Degradation: Fallback options if parallel processing fails

### Statistical Calculations
- Online Algorithms: Running calculations for mean, standard deviation
- Type Inference: Automatic detection of data types
- Efficient Sampling: Bounded sampling for distinct count estimation
- Progress Tracking: Real-time progress for large files

## Advanced Usage

### Performance Monitoring
```bash
# Monitor CPU and memory usage while processing
./dataqa scan --dir large_dataset --recursive &
htop  # or your preferred system monitor
```

### Batch Processing Multiple Directories
```bash
# Process multiple directories sequentially
for dir in /data/{2023,2022,2021}/*/; do
  echo "Processing $dir"
  ./dataqa scan --dir "$dir" --recursive --fast
done
```

### Integration with Data Pipelines
```bash
# Use as part of ETL pipeline
if ./dataqa scan --dir /input --recursive --fast; then
  echo "Data quality check passed"
  # Continue with data processing
else
  echo "Data quality issues detected"
  exit 1
fi
```

## Statistical Features

### Column-Level Analysis
- Count: Number of non-null values
- Null Count: Number of null/empty values
- Data Type: Automatic type inference (int, float, string, date)
- Basic Statistics: Min, Max values
- Advanced Statistics: Mean, Standard Deviation (numeric only)

### Dataset-Level Analysis
- Total Rows: Number of data rows
- Null Percentage: Overall null data percentage
- Distinct Row Ratio: Estimate of unique rows
- Memory Usage: Bounded memory reporting

### Type Detection
- Integers: `123`, `-456`
- Floats: `123.45`, `-67.89`, `1.23e-4`
- Strings: Text data that isn't numeric
- Dates: `2023-12-31`, `01/02/2023` (basic formats)

## Real-World Use Cases

### Data Pipeline Validation
```bash
# Validate data before loading to database
./dataqa scan --dir /data/staging --recursive --memory-limit 1024
```

### Data Quality Audits
```bash
# Comprehensive quality assessment
./dataqa scan --dir /data/production --recursive --verbose > quality_report.txt
```

### Large Dataset Exploration
```bash
# Quick exploration of terabyte-scale datasets
./dataqa scan --dir /data/lakehouse --recursive --fast --workers 32
```

### Compliance Monitoring
```bash
# Regular data quality checks
./dataqa scan --dir /data/compliance --recursive --chunk-size 100000
```

## Troubleshooting

### Common Issues

**High Memory Usage?**
```bash
# Reduce memory limits
./dataqa scan --dir data --recursive --memory-limit 256 --sample-size 500
```

**Slow Processing?**
```bash
# Increase workers and chunk size
./dataqa scan --dir data --recursive --workers 16 --chunk-size 200000
```

**Too Many Files?**
```bash
# Filter by file size
./dataqa scan --dir data --recursive --min-size 1024 --max-size 104857600
```

### Performance Expectations

| File Size | Expected Time | Recommended Settings |
|-----------|----------------|---------------------|
| < 10MB | < 5 seconds | Default settings |
| 10MB - 100MB | 5-30 seconds | Default settings |
| 100MB - 1GB | 30-90 seconds | --chunk-size 100000 |
| 1GB - 10GB | 2-6 minutes | --fast, --workers 8+ |
| 10GB - 100GB | 20-60 minutes | --fast, --workers 16+ |
| 100GB+ | 2-8 hours | --fast, --workers 32+ |

## Contributing

Contributions are welcome! Please feel free to submit issues and enhancement requests.

### Development Setup
```bash
git clone https://github.com/peekknuf/DataQuality-CLI.git
cd DataQuality-CLI
go test ./...
go build -o dataqa
```

### Running Tests
```bash
# Run all tests
go test ./...

# Run with coverage
go test ./... -cover

# Run benchmarks
go test ./internal/profiler/ -bench=. -benchmem
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built for production data processing at scale
- Optimized for cloud and commodity hardware
- Designed for enterprise-grade data quality assessment

---

**DataQuality-CLI** - Making data quality assessment fast, scalable, and accessible for everyone!