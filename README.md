# DataQuality-CLI

[![Go Report Card](https://goreportcard.com/badge/github.com/peekknuf/DataQuality-CLI)](https://goreportcard.com/report/github.com/peekknuf/DataQuality-CLI)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A high-performance CLI tool that provides **comprehensive statistics and analysis** for CSV files with parallel processing capabilities. Optimized for speed and memory efficiency with automatic resource optimization.

## Features

- **High Performance**: 10x+ faster than pandas.describe() through parallel processing
- **Auto-Optimization**: Automatically detects CPU cores, memory, and optimal settings
- **Simple Interface**: Ultra-simple CLI with smart defaults
- **Scalable**: Efficient processing of files from MB to GB scale
- **Memory Bounded**: Never exceeds configured memory limits

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/peekknuf/DataQuality-CLI.git
cd DataQuality-CLI

# Build the binary
go build -o dataqa main.go

# Move to your PATH (optional)
sudo mv dataqa /usr/local/bin/
```

### Basic Usage

```bash
# Primary commands (auto-max performance)
dataqa describe file.csv                           # Max performance, all cores
dataqa describe /data/directory/ --recursive       # Parallel file processing

# Optional overrides (rarely needed)
dataqa describe file.csv --workers 4               # Limit CPU usage
dataqa describe file.csv --memory-limit 2048       # Limit memory usage
dataqa describe file.csv --sample 10000            # Quick preview mode
dataqa describe file.csv --output results.txt      # Save output
```

```
## Architecture

The tool follows a simplified 5-component architecture:

1. **File Orchestrator** - Manages multiple files, spawns workers
2. **Parallel CSV Parser** - Parses files in chunks with parallel workers
3. **Column Statistics Engine** - Calculates describe() stats in parallel
4. **Basic Quality Checker** - Simple quality metrics (null %, type consistency)
5. **Output Formatter** - Pandas-like table output

## Command Line Options

### describe Command

```bash
dataqa describe [file or directory] [flags]
```

**Flags:**
- `--workers int` - Number of parallel workers (default: auto-detect CPU cores)
- `--memory-limit int` - Memory limit in MB (default: auto-detect 75% RAM, capped at 8GB)
- `--sample int` - Sample size for distinct value estimation (default: auto-detect based on memory)
- `--output string` - Output file to save results (default: stdout)
- `--recursive` - Process directories recursively

## Examples

### Single File Analysis

```bash
# Basic analysis with auto-max performance
dataqa describe sales_data.csv

# Custom settings
dataqa describe sales_data.csv --workers 4 --memory-limit 512
```

### Directory Analysis

```bash
# Process all CSV files in directory
dataqa describe /data/sales/

# Recursive directory processing
dataqa describe /data/ --recursive
```

### Output Options

```bash
# Save results to file
dataqa describe sales_data.csv --output analysis_report.txt

# Preview mode with smaller sample
dataqa describe large_dataset.csv --sample 100
```

## Column Types Supported

- **Numeric (int/float)**: count, mean, std, min, 25%, 50%, 75%, max
- **String**: count, unique, top, freq, min, max
- **Datetime**: count, unique, min, max (auto-detected)
- **Mixed**: Type consistency analysis with null percentage

## Essential Quality Metrics

DataQuality-CLI focuses on essential quality metrics that matter most:

- **Null Analysis**: Percentage and patterns of missing values
- **Type Consistency**: Detect mixed data types in columns
- **Basic Duplicate Detection**: Exact duplicate row counts
- **Statistical Outliers**: Simple IQR-based outliers for numeric columns

## System Requirements

### Minimum Requirements
- Go 1.23+ (for building from source)
- 2GB RAM
- 2+ CPU cores

### Recommended Requirements
- Go 1.23+
- 8GB+ RAM
- 8+ CPU cores
- SSD storage

## Building from Source

```bash
# Clone the repository
git clone https://github.com/peekknuf/DataQuality-CLI.git
cd DataQuality-CLI

# Build the binary
go build -o dataqa main.go

# Verify installation
./dataqa --help
```

## Use Cases

### Data Engineering
- Quick data profiling before pipeline processing
- Validate CSV file quality and structure
- Monitor data quality in automated workflows

### Data Science
- Fast exploratory data analysis
- Initial data quality assessment
- Generate summary statistics for reports

### Business Intelligence
- Validate data before loading to data warehouses
- Quick data quality checks
- Generate data summaries for stakeholders


## License

DataQuality-CLI is released under the [MIT License](LICENSE).

## Contributing

We welcome contributions! Please feel free to submit issues and enhancement requests.

