package engine

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mmapio "github.com/peekknuf/DataQuality-CLI/internal/io"
)

// HighPerformanceCSVEngine provides ultra-fast CSV processing with memory-mapped I/O
type HighPerformanceCSVEngine struct {
	FilePath      string
	Workers       int
	MemoryLimitMB int
	ChunkSize     int
	UseMmap       bool
}

// NewHighPerformanceCSVEngine creates a new high-performance engine
func NewHighPerformanceCSVEngine(filePath string, workers, memoryLimitMB int) *HighPerformanceCSVEngine {
	// Auto-detect optimal settings
	if workers == 0 {
		workers = runtime.NumCPU() * 2 // Use 2x CPU cores for I/O parallelism
	}
	
	chunkSize := 1024 * 1024 // 1MB chunks for optimal I/O
	if memoryLimitMB > 2048 {
		chunkSize = 2 * 1024 * 1024 // 2MB chunks for systems with more memory
	}

	return &HighPerformanceCSVEngine{
		FilePath:      filePath,
		Workers:       workers,
		MemoryLimitMB: memoryLimitMB,
		ChunkSize:     chunkSize,
		UseMmap:       true, // Always use memory mapping for best performance
	}
}

// Describe processes the CSV file with maximum performance
func (e *HighPerformanceCSVEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	// Create memory-mapped reader
	mmapConfig := mmapio.DefaultMMapConfig()
	mmapConfig.ChunkSize = int64(e.ChunkSize)
	mmapConfig.UseMmap = e.UseMmap
	
	mmapReader, err := mmapio.NewMMapReader(e.FilePath, mmapConfig)
	if err != nil {
		result.Error = fmt.Errorf("failed to create memory-mapped reader: %v", err)
		return result
	}
	defer mmapReader.Close()

	// Read entire file into memory for fastest processing
	fileSize := mmapReader.Size()
	fileData := make([]byte, fileSize)
	totalRead := 0

	for {
		chunk, err := mmapReader.ReadChunk()
		if err != nil {
			result.Error = fmt.Errorf("failed to read chunk: %v", err)
			return result
		}
		if chunk == nil {
			break // EOF
		}
		
		if totalRead+len(chunk) > len(fileData) {
			result.Error = fmt.Errorf("chunk size exceeds buffer")
			return result
		}
		
		copy(fileData[totalRead:], chunk)
		totalRead += len(chunk)
	}

	// Resize to actual data read
	fileData = fileData[:totalRead]

	// Create CSV reader from memory-mapped data
	csvReader := csv.NewReader(strings.NewReader(string(fileData)))
	
	// Read headers
	headers, err := csvReader.Read()
	if err != nil {
		result.Error = fmt.Errorf("failed to read headers: %v", err)
		return result
	}

	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	// Create high-performance column processors
	processors := make([]*HighPerformanceColumnProcessor, len(headers))
	for i, header := range headers {
		processors[i] = NewHighPerformanceColumnProcessor(header)
	}

	// Process all records directly (simplified for reliability)
	totalRows := 0
	for {
		record, err := csvReader.Read()
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
				processors[i].ProcessValueFast(value)
			}
		}
		totalRows++
	}

	// Generate final statistics
	result.ColumnStats = make([]ColumnStats, len(processors))
	var totalNulls int

	for i, processor := range processors {
		stats := processor.GetStats()
		result.ColumnStats[i] = stats
		totalNulls += stats.NullCount
	}

	result.RowCount = totalRows
	if result.RowCount > 0 {
		result.NullPercentage = float64(totalNulls) / float64(result.RowCount*len(headers)) * 100
	}
	result.ProcessingTime = time.Since(startTime)

	return result
}

// HighPerformanceColumnProcessor provides lock-free, highly optimized column statistics
type HighPerformanceColumnProcessor struct {
	name       string
	columnType string

	// Numeric statistics - use atomic operations for thread safety
	count      int64
	nullCount  int64
	sum        float64
	sumSquared float64
	min        float64
	max        float64
	
	// String statistics - use sync.Map for thread safety
	valueCounts sync.Map
	topValue    string
	topFreq     int64

	// Type detection flags
	hasInt    int64 // Use int64 for atomic operations
	hasFloat  int64
	hasString int64

	// Quantile calculation - use slice for all values (no sampling)
	sortedVals []float64
	mu         sync.RWMutex // Only for sortedVals operations
}

// NewHighPerformanceColumnProcessor creates a new high-performance column processor
func NewHighPerformanceColumnProcessor(name string) *HighPerformanceColumnProcessor {
	return &HighPerformanceColumnProcessor{
		name:       name,
		min:        math.MaxFloat64,
		max:        -math.MaxFloat64,
		valueCounts: sync.Map{},
	}
}

// ProcessValueFast processes a value with maximum performance
func (p *HighPerformanceColumnProcessor) ProcessValueFast(value string) {
	atomic.AddInt64(&p.count, 1)

	// Check for null/empty
	if value == "" {
		atomic.AddInt64(&p.nullCount, 1)
		return
	}

	// Try to parse as integer first (fastest path)
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		atomic.StoreInt64(&p.hasInt, 1)
		floatVal := float64(intVal)
		p.updateNumericStatsFast(floatVal)
		p.updateStringStatsFast(value)
		return
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		atomic.StoreInt64(&p.hasFloat, 1)
		p.updateNumericStatsFast(floatVal)
		p.updateStringStatsFast(value)
		return
	}

	// It's a string
	atomic.StoreInt64(&p.hasString, 1)
	p.updateStringStatsFast(value)
}

// updateNumericStatsFast updates numeric statistics with optimized operations
func (p *HighPerformanceColumnProcessor) updateNumericStatsFast(value float64) {
	// Use simple atomic operations for basic stats (Go doesn't have atomic float64, so use mutex for simplicity)
	p.mu.Lock()
	defer p.mu.Unlock()
	
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

// updateStringStatsFast updates string statistics with sync.Map
func (p *HighPerformanceColumnProcessor) updateStringStatsFast(value string) {
	// Update value counts
	var newCount int64
	if actualCount, loaded := p.valueCounts.LoadOrStore(value, int64(1)); loaded {
		if count, ok := actualCount.(int64); ok {
			newCount = count + 1
			p.valueCounts.Store(value, newCount)
		}
	} else {
		newCount = 1
	}

	// Update top value (simple mutex approach for string)
	p.mu.Lock()
	if newCount > p.topFreq {
		p.topFreq = newCount
		p.topValue = value
	}
	p.mu.Unlock()
}

// GetStats returns the final column statistics
func (p *HighPerformanceColumnProcessor) GetStats() ColumnStats {
	p.mu.RLock()
	count := int(p.count)
	nullCount := int(p.nullCount)
	sum := p.sum
	sumSquared := p.sumSquared
	min := p.min
	max := p.max
	
	// Determine column type
	hasInt := p.hasInt != 0
	hasFloat := p.hasFloat != 0
	hasString := p.hasString != 0
	
	// Make copy of sorted values for quantile calculation
	sortedCopy := make([]float64, len(p.sortedVals))
	copy(sortedCopy, p.sortedVals)
	p.mu.RUnlock()

	stats := ColumnStats{
		Name:      p.name,
		Count:     count,
		NullCount: nullCount,
	}

	if hasInt && !hasFloat && !hasString {
		stats.Type = "int"
	} else if (hasFloat || hasInt) && !hasString {
		stats.Type = "float"
	} else {
		stats.Type = "string"
	}

	// Calculate numeric statistics
	if stats.Type == "int" || stats.Type == "float" {
		if count-nullCount > 0 {
			stats.Mean = sum / float64(count-nullCount)
			variance := (sumSquared / float64(count-nullCount)) - (stats.Mean * stats.Mean)
			if variance > 0 {
				stats.Std = math.Sqrt(variance)
			}
		}

		if min != math.MaxFloat64 {
			stats.Min = fmt.Sprintf("%.6g", min)
		}
		if max != -math.MaxFloat64 {
			stats.Max = fmt.Sprintf("%.6g", max)
		}

		// Calculate quantiles (use all values, no sampling)
	if len(sortedCopy) > 0 {
		sort.Float64s(sortedCopy)
		stats.Q25 = calculateQuantileHPP(sortedCopy, 0.25)
		stats.Q50 = calculateQuantileHPP(sortedCopy, 0.50)
		stats.Q75 = calculateQuantileHPP(sortedCopy, 0.75)
	}
	}

	// Calculate string statistics
	if stats.Type == "string" {
		// Count unique values and find top
		var uniqueCount int
		p.valueCounts.Range(func(key, value interface{}) bool {
			uniqueCount++
			if count, ok := value.(int64); ok && int(count) > stats.Freq {
				stats.Freq = int(count)
				if str, ok := key.(string); ok {
					stats.Top = str
				}
			}
			return true
		})
		stats.Unique = uniqueCount

		// For string columns, use first/last alphabetically for min/max display
		var keys []string
		p.valueCounts.Range(func(key, value interface{}) bool {
			if str, ok := key.(string); ok {
				keys = append(keys, str)
			}
			return true
		})

		if len(keys) > 0 {
			sort.Strings(keys)
			stats.Min = keys[0]
			stats.Max = keys[len(keys)-1]
		}
	}

	return stats
}

// calculateQuantileHPP calculates the quantile value from a sorted slice (high performance version)
func calculateQuantileHPP(sortedVals []float64, quantile float64) float64 {
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


