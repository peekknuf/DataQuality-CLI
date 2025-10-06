#!/bin/bash

# Performance Benchmark Script
# This script runs performance benchmarks and generates reports

set -e

# Configuration
BENCHMARK_DIR="benchmark_results"
BASELINE_FILE="$BENCHMARK_DIR/baseline.json"
REPORT_DIR="$BENCHMARK_DIR/reports"
ALERT_FILE="$BENCHMARK_DIR/alerts.json"
TREND_DIR="$BENCHMARK_DIR/trends"

# Create directories
mkdir -p "$BENCHMARK_DIR"
mkdir -p "$REPORT_DIR"
mkdir -p "$TREND_DIR"

# Parse command line arguments
UPDATE_BASELINE=false
COMPARE_BASELINE=false
GENERATE_TREND=false
RUN_LOAD_TESTS=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --update-baseline)
      UPDATE_BASELINE=true
      shift
      ;;
    --compare-baseline)
      COMPARE_BASELINE=true
      shift
      ;;
    --generate-trend)
      GENERATE_TREND=true
      shift
      ;;
    --run-load-tests)
      RUN_LOAD_TESTS=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --update-baseline    Update baseline with current results"
      echo "  --compare-baseline   Compare current results with baseline"
      echo "  --generate-trend      Generate trend report from multiple runs"
      echo "  --run-load-tests      Run load tests"
      echo "  -h, --help            Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Function to run benchmarks
run_benchmarks() {
  echo "Running benchmarks..."
  
  # Run Go benchmarks
  go test -bench=. -benchmem -run=^$ -count=1 ./benchmark/... > "$BENCHMARK_DIR/bench_output.txt" 2>&1
  
  # Run benchmark suite
  go test -run=TestBenchmarkSuite ./benchmark/... -v
  
  echo "Benchmarks completed."
}

# Function to run load tests
run_load_tests() {
  echo "Running load tests..."
  
  # Run load tests
  go test -run=TestLoadTest ./test/... -v
  
  echo "Load tests completed."
}

# Function to generate reports
generate_reports() {
  echo "Generating reports..."
  
  # Generate timestamp for this run
  TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
  CURRENT_FILE="$BENCHMARK_DIR/current_$TIMESTAMP.json"
  
  # Copy current results to timestamped file
  if [ -f "$BENCHMARK_DIR/benchmark_results.json" ]; then
    cp "$BENCHMARK_DIR/benchmark_results.json" "$CURRENT_FILE"
  fi
  
  # Generate HTML report
  if [ -f "$BENCHMARK_DIR/benchmark_results.json" ]; then
    go run scripts/generate_report.go "$BENCHMARK_DIR/benchmark_results.json" "$REPORT_DIR/report_$TIMESTAMP.html"
  fi
  
  # Generate alerts
  if [ -f "$BENCHMARK_DIR/benchmark_results.json" ]; then
    go run scripts/generate_alerts.go "$BENCHMARK_DIR/benchmark_results.json" "$ALERT_FILE"
  fi
  
  echo "Reports generated."
}

# Function to update baseline
update_baseline() {
  echo "Updating baseline..."
  
  if [ -f "$BENCHMARK_DIR/benchmark_results.json" ]; then
    cp "$BENCHMARK_DIR/benchmark_results.json" "$BASELINE_FILE"
    echo "Baseline updated."
  else
    echo "No benchmark results found to update baseline."
  fi
}

# Function to compare with baseline
compare_baseline() {
  echo "Comparing with baseline..."
  
  if [ -f "$BASELINE_FILE" ] && [ -f "$BENCHMARK_DIR/benchmark_results.json" ]; then
    go run scripts/compare_results.go "$BASELINE_FILE" "$BENCHMARK_DIR/benchmark_results.json" "$REPORT_DIR/comparison.html"
    echo "Comparison report generated."
  else
    echo "Baseline or current results not found."
  fi
}

# Function to generate trend report
generate_trend() {
  echo "Generating trend report..."
  
  # Find all benchmark result files
  RESULT_FILES=$(find "$BENCHMARK_DIR" -name "current_*.json" -o -name "baseline.json" | sort)
  
  if [ -n "$RESULT_FILES" ]; then
    # Convert to array
    readarray -t FILES <<< "$RESULT_FILES"
    
    # Generate trend report
    go run scripts/generate_trend.go "${FILES[@]}" "$REPORT_DIR/trend.html"
    echo "Trend report generated."
  else
    echo "No benchmark result files found for trend analysis."
  fi
}

# Main execution
echo "Starting performance benchmarking..."

# Run benchmarks
run_benchmarks

# Run load tests if requested
if [ "$RUN_LOAD_TESTS" = true ]; then
  run_load_tests
fi

# Generate reports
generate_reports

# Update baseline if requested
if [ "$UPDATE_BASELINE" = true ]; then
  update_baseline
fi

# Compare with baseline if requested
if [ "$COMPARE_BASELINE" = true ]; then
  compare_baseline
fi

# Generate trend report if requested
if [ "$GENERATE_TREND" = true ]; then
  generate_trend
fi

echo "Performance benchmarking completed."
echo "Results saved to: $BENCHMARK_DIR"
echo "Reports saved to: $REPORT_DIR"