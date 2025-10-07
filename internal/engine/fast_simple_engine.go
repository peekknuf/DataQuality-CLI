package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

// FastSimpleEngine provides simple, fast CSV processing without over-engineering
type FastSimpleEngine struct {
	FilePath string
}

// NewFastSimpleEngine creates a new fast simple engine
func NewFastSimpleEngine(filePath string) *FastSimpleEngine {
	return &FastSimpleEngine{
		FilePath: filePath,
	}
}

// Describe processes the CSV file with maximum speed and simplicity
func (e *FastSimpleEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	// Open file directly
	file, err := os.Open(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %v", err)
		return result
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Read headers
	headers, err := reader.Read()
	if err != nil {
		result.Error = fmt.Errorf("failed to read headers: %v", err)
		return result
	}

	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	// Create simple column processors
	processors := make([]*FastColumnProcessor, len(headers))
	for i, header := range headers {
		processors[i] = NewFastColumnProcessor(header)
	}

	// Process all records efficiently
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

		// Process each column
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
	if result.RowCount > 0 {
		result.NullPercentage = float64(totalNulls) / float64(result.RowCount*len(headers)) * 100
	}
	result.ProcessingTime = time.Since(startTime)

	return result
}

// FastColumnProcessor provides simple, fast column statistics
type FastColumnProcessor struct {
	name       string
	count      int
	nullCount  int
	sum        float64
	sumSquared float64
	min        float64
	max        float64
	sortedVals []float64

	// String statistics
	valueCounts map[string]int
	topValue    string
	topFreq     int

	// Type detection
	hasInt    bool
	hasFloat  bool
	hasString bool
}

// NewFastColumnProcessor creates a new fast column processor
func NewFastColumnProcessor(name string) *FastColumnProcessor {
	return &FastColumnProcessor{
		name:        name,
		min:         math.MaxFloat64,
		max:         -math.MaxFloat64,
		valueCounts: make(map[string]int),
	}
}

// ProcessValue processes a single value
func (p *FastColumnProcessor) ProcessValue(value string) {
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
func (p *FastColumnProcessor) updateNumericStats(value float64) {
	p.sum += value
	p.sumSquared += value * value

	if value < p.min {
		p.min = value
	}
	if value > p.max {
		p.max = value
	}

	// Add to sorted values for quantiles
	p.sortedVals = append(p.sortedVals, value)
}

// updateStringStats updates string statistics
func (p *FastColumnProcessor) updateStringStats(value string) {
	p.valueCounts[value]++
	if p.valueCounts[value] > p.topFreq {
		p.topFreq = p.valueCounts[value]
		p.topValue = value
	}
}

// GetStats returns the final column statistics
func (p *FastColumnProcessor) GetStats() ColumnStats {
	stats := ColumnStats{
		Name:      p.name,
		Count:     p.count,
		NullCount: p.nullCount,
		Type:      p.determineColumnType(),
	}

	p.calculateNumericStats(&stats)
	p.calculateStringStats(&stats)

	return stats
}

func (p *FastColumnProcessor) determineColumnType() string {
	if p.hasInt && !p.hasFloat && !p.hasString {
		return "int"
	}
	if (p.hasFloat || p.hasInt) && !p.hasString {
		return "float"
	}
	return "string"
}

func (p *FastColumnProcessor) calculateNumericStats(stats *ColumnStats) {
	if stats.Type != "int" && stats.Type != "float" {
		return
	}

	validCount := p.count - p.nullCount
	if validCount > 0 {
		stats.Mean = p.sum / float64(validCount)
		stats.Std = p.calculateStandardDeviation(validCount, stats.Mean)
	}

	p.setNumericMinMax(stats)
	p.calculateQuantiles(stats)
}

func (p *FastColumnProcessor) calculateStandardDeviation(validCount int, mean float64) float64 {
	variance := (p.sumSquared / float64(validCount)) - (mean * mean)
	if variance > 0 {
		return math.Sqrt(variance)
	}
	return 0
}

func (p *FastColumnProcessor) setNumericMinMax(stats *ColumnStats) {
	if p.min != math.MaxFloat64 {
		stats.Min = fmt.Sprintf("%.6g", p.min)
	}
	if p.max != -math.MaxFloat64 {
		stats.Max = fmt.Sprintf("%.6g", p.max)
	}
}

func (p *FastColumnProcessor) calculateQuantiles(stats *ColumnStats) {
	if len(p.sortedVals) > 0 {
		sort.Float64s(p.sortedVals)
		stats.Q25 = calculateQuantileFast(p.sortedVals, 0.25)
		stats.Q50 = calculateQuantileFast(p.sortedVals, 0.50)
		stats.Q75 = calculateQuantileFast(p.sortedVals, 0.75)
	}
}

func (p *FastColumnProcessor) calculateStringStats(stats *ColumnStats) {
	if stats.Type != "string" {
		return
	}

	stats.Unique = len(p.valueCounts)
	stats.Top = p.topValue
	stats.Freq = p.topFreq

	p.setStringMinMax(stats)
}

func (p *FastColumnProcessor) setStringMinMax(stats *ColumnStats) {
	if len(p.valueCounts) > 0 {
		keys := p.getSortedKeys()
		if len(keys) > 0 {
			stats.Min = keys[0]
			stats.Max = keys[len(keys)-1]
		}
	}
}

func (p *FastColumnProcessor) getSortedKeys() []string {
	keys := make([]string, 0, len(p.valueCounts))
	for k := range p.valueCounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// calculateQuantileFast calculates the quantile value from a sorted slice
func calculateQuantileFast(sortedVals []float64, quantile float64) float64 {
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
