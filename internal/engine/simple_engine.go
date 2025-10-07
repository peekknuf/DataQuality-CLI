package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// SimpleEngine provides pandas.describe() functionality for CSV files
type SimpleEngine struct {
	FilePath      string
	Workers       int
	MemoryLimitMB int
	SampleSize    int
	ChunkSize     int
}

// ColumnStats represents pandas.describe() style statistics for a column
type ColumnStats struct {
	Name      string
	Type      string // "int", "float", "string", "datetime"
	Count     int
	NullCount int
	Mean      float64
	Std       float64
	Min       string
	Q25       float64
	Q50       float64
	Q75       float64
	Max       string
	Unique    int    // For string columns
	Top       string // For string columns
	Freq      int    // For string columns
}

// DescribeResult contains the complete analysis results
type DescribeResult struct {
	Path           string
	RowCount       int
	ColumnStats    []ColumnStats
	NullPercentage float64
	ProcessingTime time.Duration
	Error          error
}

// NewSimpleEngine creates a new simple statistics engine
func NewSimpleEngine(filePath string, workers, memoryLimitMB, sampleSize int) *SimpleEngine {
	// Auto-detect optimal chunk size based on memory
	chunkSize := 50000 // Default
	if memoryLimitMB > 1024 {
		chunkSize = 100000 // Larger chunks for systems with more memory
	}

	return &SimpleEngine{
		FilePath:      filePath,
		Workers:       workers,
		MemoryLimitMB: memoryLimitMB,
		SampleSize:    sampleSize,
		ChunkSize:     chunkSize,
	}
}

// Describe processes the CSV file and returns pandas.describe() style statistics
func (e *SimpleEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	// Open file
	file, err := os.Open(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %v", err)
		return result
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		result.Error = fmt.Errorf("failed to read header: %v", err)
		return result
	}

	// Initialize column processors
	processors := make([]*ColumnProcessor, len(headers))
	for i, header := range headers {
		processors[i] = NewColumnProcessor(header, e.SampleSize)
	}

	// Process file in chunks
	rowCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Error = fmt.Errorf("failed to read record: %v", err)
			return result
		}

		// Process each column (sequential to reduce goroutine overhead)
		for i, value := range record {
			if i < len(processors) {
				processors[i].ProcessValue(value)
			}
		}

		rowCount++
	}

	// Generate final statistics
	result.ColumnStats = make([]ColumnStats, len(processors))
	var totalNulls int

	for i, processor := range processors {
		stats := processor.GetStats()
		result.ColumnStats[i] = stats
		totalNulls += stats.NullCount
	}

	result.RowCount = rowCount
	if rowCount > 0 && len(headers) > 0 {
		result.NullPercentage = float64(totalNulls) / float64(rowCount*len(headers)) * 100
	}
	result.ProcessingTime = time.Since(startTime)

	return result
}

// ColumnProcessor handles statistics for a single column
type ColumnProcessor struct {
	name       string
	columnType string
	sampleSize int

	// Numeric statistics
	count      int
	nullCount  int
	sum        float64
	sumSquared float64
	min        float64
	max        float64
	sortedVals []float64

	// String statistics
	uniqueCount int
	topValue    string
	topFreq     int
	valueCounts map[string]int

	// Type detection
	hasInt    bool
	hasFloat  bool
	hasString bool
	mu        sync.Mutex
}

// NewColumnProcessor creates a new column processor
func NewColumnProcessor(name string, sampleSize int) *ColumnProcessor {
	return &ColumnProcessor{
		name:        name,
		sampleSize:  sampleSize,
		min:         math.MaxFloat64,
		max:         -math.MaxFloat64,
		valueCounts: make(map[string]int),
	}
}

// ProcessValue processes a single value in the column
func (p *ColumnProcessor) ProcessValue(value string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.count++

	// Check for null/empty
	if value == "" {
		p.nullCount++
		return
	}

	// Try to parse as integer first
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		p.hasInt = true
		floatVal := float64(intVal)
		p.updateNumericStats(floatVal)
		p.updateStringStats(value)
		return
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		p.hasFloat = true
		p.updateNumericStats(floatVal)
		p.updateStringStats(value)
		return
	}

	// It's a string
	p.hasString = true
	p.updateStringStats(value)
}

// updateNumericStats updates numeric statistics
func (p *ColumnProcessor) updateNumericStats(value float64) {
	p.sum += value
	p.sumSquared += value * value

	if value < p.min {
		p.min = value
	}
	if value > p.max {
		p.max = value
	}

	// Keep sorted values for quantiles (sample to limit memory)
	// If sampleSize is 0, process all values
	if p.sampleSize == 0 || len(p.sortedVals) < p.sampleSize {
		p.sortedVals = append(p.sortedVals, value)
	} else if len(p.sortedVals) == p.sampleSize && value < p.max {
		// Replace the maximum value to keep sample representative
		p.sortedVals = append(p.sortedVals[:p.sampleSize-1], value)
	}
}

// updateStringStats updates string statistics
func (p *ColumnProcessor) updateStringStats(value string) {
	p.valueCounts[value]++
	if p.valueCounts[value] > p.topFreq {
		p.topFreq = p.valueCounts[value]
		p.topValue = value
	}
}

// GetStats returns the final column statistics
func (p *ColumnProcessor) GetStats() ColumnStats {
	stats := ColumnStats{
		Name:      p.name,
		Count:     p.count,
		NullCount: p.nullCount,
	}

	// Determine column type
	if p.hasInt && !p.hasFloat && !p.hasString {
		stats.Type = "int"
	} else if (p.hasFloat || p.hasInt) && !p.hasString {
		stats.Type = "float"
	} else {
		stats.Type = "string"
	}

	// Calculate numeric statistics
	if stats.Type == "int" || stats.Type == "float" {
		if p.count-p.nullCount > 0 {
			stats.Mean = p.sum / float64(p.count-p.nullCount)
			variance := (p.sumSquared / float64(p.count-p.nullCount)) - (stats.Mean * stats.Mean)
			if variance > 0 {
				stats.Std = math.Sqrt(variance)
			}
		}

		if p.min != math.MaxFloat64 {
			stats.Min = fmt.Sprintf("%.6g", p.min)
		}
		if p.max != -math.MaxFloat64 {
			stats.Max = fmt.Sprintf("%.6g", p.max)
		}

		// Calculate quantiles
		if len(p.sortedVals) > 0 {
			sort.Float64s(p.sortedVals)
			stats.Q25 = calculateQuantile(p.sortedVals, 0.25)
			stats.Q50 = calculateQuantile(p.sortedVals, 0.50)
			stats.Q75 = calculateQuantile(p.sortedVals, 0.75)
		}
	}

	// Calculate string statistics
	if stats.Type == "string" {
		stats.Unique = len(p.valueCounts)
		stats.Top = p.topValue
		stats.Freq = p.topFreq

		// For string columns, use first/last values for min/max display
		if len(p.valueCounts) > 0 {
			keys := make([]string, 0, len(p.valueCounts))
			for k := range p.valueCounts {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			if len(keys) > 0 {
				stats.Min = keys[0]
				stats.Max = keys[len(keys)-1]
			}
		}
	}

	return stats
}

// calculateQuantile calculates the quantile from sorted values
func calculateQuantile(sortedVals []float64, quantile float64) float64 {
	if len(sortedVals) == 0 {
		return 0
	}

	if len(sortedVals) == 1 {
		return sortedVals[0]
	}

	index := quantile * float64(len(sortedVals)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper {
		return sortedVals[lower]
	}

	weight := index - float64(lower)
	return sortedVals[lower]*(1-weight) + sortedVals[upper]*weight
}
