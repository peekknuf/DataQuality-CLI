# DataQuality-CLI

A command-line tool for analyzing data quality metrics in CSV files, designed for Exploratory Data Analysis (EDA). This tool helps users quickly assess the quality of their datasets, providing insights into missing values, distinct counts, and other essential statistics.

## Features

### File Scanning
- Scan single files or entire directories.
- Recursive directory traversal support.
- File size filtering with min/max thresholds.
- Progress bar visualization for scanning process.

### Quality Metrics
- Basic Statistics:
  - Row count.
  - Null value percentage.
  - Distinct row ratio.

- Column-level Analysis (verbose mode):
  - Automatic data type inference (int, float, date, string).
  - Null count tracking.
  - Distinct value counting.
  - Min/Max value detection.
  - Sample value collection.

## Usage

### Scan a Specific File
To scan a specific CSV file and analyze its quality metrics, use the following command:

```bash
dataqa scan --file example.csv --dir /path/to/dir
```

### Scan a Directory Recursively
To scan all CSV files in a directory and its subdirectories, use:

```bash
dataqa scan --dir /path/to/dir --recursive
```

### Show Detailed Metrics
To display detailed metrics for each column in a CSV file, use the verbose flag:

```bash
dataqa scan --dir /path/to/dir --verbose
```

### Filter by File Size
To filter files by size, use the min-size and max-size flags:

```bash
dataqa scan --dir /path/to/dir --min-size 1000 --max-size 1000000
```

## TO BE DONE

- [ ] **JSON Format Support:** Add support for scanning and analyzing JSON files in addition to CSV.
- [ ] **Enhanced Data Type Detection:** Improve the data type inference to handle more complex data types and formats.
- [ ] **Data Validation Rules Configuration:** Allow users to define custom validation rules for specific columns (e.g., regex patterns).
- [ ] **Export Capabilities for Quality Reports:** Implement functionality to export quality reports in various formats (e.g., CSV, JSON, PDF).
- [ ] **Batch Processing Improvements:** Optimize the tool for processing large datasets and multiple files simultaneously.
- [ ] **API for Integration with Other Tools:** Provide a REST API for integrating the tool with other data processing pipelines.
- [ ] **Configuration File Support:** Allow users to specify configuration files (e.g., YAML, TOML) for custom settings.
- [ ] **Custom Metric Definitions:** Enable users to define and calculate custom data quality metrics beyond the built-in ones.

## License

MIT License - see LICENSE file for details.