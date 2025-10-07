# DataQuality-CLI



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

## Example Output

```bash
$ dataqa describe ../data/sales_directory/
Auto-detected settings: 8 workers, 512MB memory
Found 41 CSV files
 Processing files... 100% |████████████████████|

=== DATA QUALITY SUMMARY ===
Total files processed: 41
Total processing time: 4.077031959s
Total rows processed: 6,752,025
Total columns analyzed: 989
Data completeness: 99.8%
Numeric columns: 840, String columns: 149

=== PER-FILE ANALYSIS ===
File                                           Rows    Columns    Null Rate Process Time Data Quality
----------------------------------------------------------------------------------------------------
dim_catalog_pages.csv                           303          9         0.0%         1ms       Good
dim_call_centers.csv                             12         31        35.5%         5ms       Poor
fact_catalog_sales_5.csv                     108,124         34         0.0%      1.269s       Good
fact_web_sales_3.csv                         216,248         34         0.0%      2.199s       Good

=== DETAILED ANALYSIS ===
File: dim_call_centers.csv
  Rows: 12 | Columns: 31 | Null Rate: 35.5%
   11 columns have >10% null values

File: dim_web_pages.csv
  Rows: 92,265 | Columns: 14 | Null Rate: 14.3%
   2 columns have >10% null values
  Key columns:
    wp_web_page_id: string (92,265 unique values)
    wp_autogen_flag: string (92,265 unique values)
```

## Output Format

The tool generates a comprehensive data quality report with three main sections:

### 1. Data Quality Summary
Aggregate statistics across all processed files:
- Total files processed and processing time
- Total rows and columns analyzed
- Overall data completeness percentage
- Distribution of numeric vs string columns

### 2. Per-File Analysis
Detailed table showing metrics for each file:
- File name (truncated for long paths)
- Row count and column count
- Null rate percentage
- Processing time
- Data quality rating (Good/Fair/Poor)

### 3. Detailed Analysis
In-depth analysis for files with quality issues:
- Files with high null rates (>5%)
- Large datasets (>100K rows)
- Files with many columns (>20)
- Column-level insights and warnings
- Key columns with interesting characteristics

## Data Quality Ratings

- **Good**: Null rate ≤ 10%
- **Fair**: Null rate 10-25%
- **Poor**: Null rate > 25%

## Column Types Supported

- **Numeric (int/float)**: Automatic detection, mean calculation, standard deviation
- **String**: Unique value counting, frequency analysis
- **Mixed Types**: Type consistency analysis with null percentage

## Quality Metrics

DataQuality-CLI focuses on practical quality metrics:

- **Null Analysis**: Percentage of missing values per column and file
- **Column Type Detection**: Automatic identification of numeric vs string data
- **Unique Value Analysis**: Cardinality assessment for string columns
- **Data Completeness**: Overall dataset completeness scoring
- **Performance Metrics**: Processing time and file size analysis

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

