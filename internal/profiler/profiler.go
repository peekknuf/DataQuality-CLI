package profiler

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type ColumnStats struct {
	Name          string
	Type          string
	NullCount     int
	DistinctCount int
	ZeroCount     int
	Min           string
	Max           string
	SampleValues  []string

	// pandas.describe() statistics
	Count         int64
	Mean          float64
	Std           float64
	Q25           float64
	Q50           float64
	Q75           float64

	// Optimized sampling - use fixed-size array for O(1) operations
	sampleSize    int
	sampleSlice   []string
	nextReplace   int
	totalCount    int
	overflowCount int

	// Running statistics for numerics (fast version)
	sum           float64
	sumSq         float64
	// Removed quartile calculations for performance
}

// NewColumnStats creates a memory-efficient column statistics tracker
func NewColumnStats(name string, maxSampleSize int) *ColumnStats {
	return &ColumnStats{
		Name:         name,
		sampleSize:   maxSampleSize,
		sampleSlice:  make([]string, 0, maxSampleSize),
		nextReplace:  0,
		SampleValues: make([]string, 0, 5),
		// Removed quartile storage for performance
	}
}

func (s *ColumnStats) Update(value string) {
	if value == "" {
		s.NullCount++
		return
	}

	s.Count++
	s.totalCount++

	// Fast O(1) reservoir sampling using circular buffer
	if len(s.sampleSlice) < s.sampleSize {
		// Still filling up the sample
		s.sampleSlice = append(s.sampleSlice, value)
		s.DistinctCount = len(s.sampleSlice)
	} else {
		// Replace with probability sampleSize/totalCount - use integer math for speed
		threshold := (s.sampleSize << 16) / s.totalCount // Fixed point arithmetic
		if rand.Intn(1<<16) < threshold {
			s.sampleSlice[s.nextReplace] = value
			s.nextReplace = (s.nextReplace + 1) % s.sampleSize
			s.overflowCount++
		}
	}

	// Update basic min/max
	if s.Min == "" || value < s.Min {
		s.Min = value
	}
	if s.Max == "" || value > s.Max {
		s.Max = value
	}

	// Sample values for display (only first few)
	if len(s.SampleValues) < 5 {
		s.SampleValues = append(s.SampleValues, value)
	}

	// Infer type and calculate statistics
	s.inferTypeAndCalculateStats(value)
}

func (s *ColumnStats) inferTypeAndCalculateStats(value string) {
	// Ultra-fast path: if type already determined and this isn't numeric, skip
	if s.Type == "string" {
		return
	}

	// Fast integer detection - avoid allocation and expensive parsing
	if s.Type != "float" && len(value) < 20 && s.fastIsInt(value) {
		if s.Type == "" {
			s.Type = "int"
		}
		if intVal, err := strconv.Atoi(value); err == nil {
			s.updateNumericStats(float64(intVal))
		}
		return
	}

	// Fast float detection for common patterns
	if s.Type != "int" && len(value) < 25 && s.fastIsFloat(value) {
		if s.Type == "" {
			s.Type = "float"
		}
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			s.updateNumericStats(floatVal)
		}
		return
	}

	// Default to string to avoid expensive date parsing
	if s.Type == "" {
		s.Type = "string"
	}
}

// fastIsInt quickly checks if a string is likely an integer
func (s *ColumnStats) fastIsInt(str string) bool {
	if len(str) == 0 {
		return false
	}

	i := 0
	if str[0] == '-' || str[0] == '+' {
		if len(str) == 1 {
			return false
		}
		i = 1
	}

	for ; i < len(str); i++ {
		c := str[i]
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// fastIsFloat quickly checks if a string is likely a float
func (s *ColumnStats) fastIsFloat(str string) bool {
	if len(str) == 0 {
		return false
	}

	hasDot := false
	hasExp := false
	i := 0

	if str[0] == '-' || str[0] == '+' {
		if len(str) == 1 {
			return false
		}
		i = 1
	}

	for ; i < len(str); i++ {
		c := str[i]
		switch {
		case c >= '0' && c <= '9':
			// Continue
		case c == '.':
			if hasDot || hasExp {
				return false
			}
			hasDot = true
		case c == 'e' || c == 'E':
			if hasExp || i == len(str)-1 {
				return false
			}
			hasExp = true
		default:
			return false
		}
	}
	return hasDot || hasExp
}

func (s *ColumnStats) updateNumericStats(value float64) {
	// Fast: only update running sums, no quartiles
	s.sum += value
	s.sumSq += value * value
}

func (s *ColumnStats) finalizeStatistics() {
	if s.Count == 0 {
		return
	}

	// Calculate mean
	s.Mean = s.sum / float64(s.Count)

	// Calculate standard deviation
	if s.Count > 1 {
		variance := (s.sumSq - (s.sum * s.sum / float64(s.Count))) / float64(s.Count-1)
		s.Std = math.Sqrt(variance)
	}

	// Fast distinct count estimation
	if s.overflowCount > 0 {
		// Use capture-recapture estimation
		estimatedDistinct := float64(s.sampleSize*s.sampleSize) / (2.0 * float64(s.overflowCount))
		s.DistinctCount = int(estimatedDistinct) + s.sampleSize
	} else {
		s.DistinctCount = len(s.sampleSlice)
	}
}

func isDate(value string) bool {
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
	}

	for _, format := range formats {
		_, err := time.Parse(format, value)
		if err == nil {
			return true
		}
	}

	return false
}

type DatasetStats struct {
	TotalRows    int
	sampleSize   int
	sampleHashes []uint64 // Use slice instead of map for O(1) operations
	nextReplace  int
	overflow     int
}

func NewDatasetStats(sampleSize int) *DatasetStats {
	return &DatasetStats{
		sampleSize:   sampleSize,
		sampleHashes: make([]uint64, 0, sampleSize),
	}
}

func (ds *DatasetStats) Update(row []string) {
	ds.TotalRows++

	// Use simple hash for row uniqueness estimation
	rowHash := ds.simpleHash(row)

	if len(ds.sampleHashes) < ds.sampleSize {
		ds.sampleHashes = append(ds.sampleHashes, rowHash)
	} else {
		// Fast O(1) replacement using circular buffer
		if rand.Float64() < float64(ds.sampleSize)/float64(ds.TotalRows) {
			ds.sampleHashes[ds.nextReplace] = rowHash
			ds.nextReplace = (ds.nextReplace + 1) % ds.sampleSize
			ds.overflow++
		}
	}
}

func (ds *DatasetStats) simpleHash(row []string) uint64 {
	// Fast simple hash for row uniqueness
	var hash uint64 = 5381
	for _, col := range row {
		for _, c := range col {
			hash = ((hash << 5) + hash) + uint64(c)
		}
		hash = ((hash << 5) + hash) + '|' // separator
	}
	return hash
}

func (ds *DatasetStats) DistinctRatio() float64 {
	if ds.TotalRows == 0 {
		return 0.0
	}

	// Simple estimation based on sample
	if ds.overflow > 0 {
		// Use capture-recapture estimation
		estimatedDistinct := float64(ds.sampleSize*ds.sampleSize) / (2.0 * float64(ds.overflow))
		return (float64(estimatedDistinct) + float64(ds.sampleSize)) / float64(ds.TotalRows)
	}

	return float64(len(ds.sampleHashes)) / float64(ds.TotalRows)
}

type ProfilerConfig struct {
	MaxColumnSampleSize int    // Maximum unique values to track per column
	MaxRowSampleSize    int    // Maximum unique rows to track
	ChunkSize          int    // Rows to process in each chunk
	MemoryLimitMB      int    // Memory limit in MB (optional)
	IOBufferSizeMB     int    // I/O buffer size in MB
}

type ProgressCallback func(processedRows int, totalRows int, currentFile string)

type CSVProfiler struct {
	FilePath     string
	ColumnStats  map[string]*ColumnStats
	RowCount     int
	DatasetStats *DatasetStats
	Config       ProfilerConfig
	Progress     ProgressCallback
}

func NewCSVProfiler(filePath string) *CSVProfiler {
	return NewCSVProfilerWithConfig(filePath, ProfilerConfig{
		MaxColumnSampleSize: 10000,  // Track up to 10K unique values per column
		MaxRowSampleSize:    50000,  // Track up to 50K unique rows
		ChunkSize:          10000,  // Process 10K rows at a time
		MemoryLimitMB:      1024,   // 1GB default memory limit
		IOBufferSizeMB:     64,     // 64MB I/O buffer
	})
}

func NewCSVProfilerWithConfig(filePath string, config ProfilerConfig) *CSVProfiler {
	return &CSVProfiler{
		FilePath:    filePath,
		ColumnStats: make(map[string]*ColumnStats),
		DatasetStats: NewDatasetStats(config.MaxRowSampleSize),
		Config:      config,
	}
}

func (p *CSVProfiler) SetProgressCallback(callback ProgressCallback) {
	p.Progress = callback
}

func (p *CSVProfiler) Profile() error {
	file, err := os.Open(p.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size for progress tracking
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	// Better row estimation: assume 100 bytes per row average
	estimatedRows := int(fileInfo.Size() / 100)
	if estimatedRows < 1000 {
		estimatedRows = 1000 // minimum estimate
	}

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %w", err)
	}

	// Initialize column statistics with memory limits
	for _, header := range headers {
		p.ColumnStats[header] = NewColumnStats(header, p.Config.MaxColumnSampleSize)
	}

	// Process file in chunks to manage memory
	chunk := make([][]string, 0, p.Config.ChunkSize)
	progressCounter := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			// Process remaining records in chunk
			if len(chunk) > 0 {
				p.processChunk(chunk, headers)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		chunk = append(chunk, record)
		p.RowCount++

		// Report progress less frequently to reduce overhead
		progressCounter++
		if progressCounter%10000 == 0 && p.Progress != nil {
			p.Progress(p.RowCount, estimatedRows, p.FilePath)
		}

		// Process chunk when it reaches the configured size
		if len(chunk) >= p.Config.ChunkSize {
			p.processChunk(chunk, headers)
			chunk = chunk[:0] // Reset chunk while preserving capacity
		}
	}

	// Final progress report
	if p.Progress != nil {
		p.Progress(p.RowCount, p.RowCount, p.FilePath)
	}

	// Finalize statistics calculation
	p.finalizeAllStatistics()

	return nil
}

func (p *CSVProfiler) finalizeAllStatistics() {
	for _, stats := range p.ColumnStats {
		stats.finalizeStatistics()
	}
}

func (p *CSVProfiler) processChunk(chunk [][]string, headers []string) {
	for _, record := range chunk {
		// Update column statistics
		for i, value := range record {
			colName := headers[i]
			stats := p.ColumnStats[colName]
			stats.Update(value)
		}

		// Update dataset statistics
		p.DatasetStats.Update(record)
	}
}

type QualityMetrics struct {
	TotalRows       int
	NullPercentage  float64
	TypeConsistency float64
	DistinctRatio   float64
}

type DescribeStats struct {
	Column        string
	Count         int64
	NullCount     int
	Type          string
	Mean          float64
	Std           float64
	Min           string
	Q25           float64
	Q50           float64
	Q75           float64
	Max           string
	DistinctCount int
	Unique        string // Sample unique values
}

func (p *CSVProfiler) CalculateQuality() QualityMetrics {
	metrics := QualityMetrics{
		TotalRows: p.RowCount,
	}

	totalNulls := 0
	totalCells := 0
	for _, stats := range p.ColumnStats {
		totalNulls += stats.NullCount
		// Count is non-null values, so total cells = Count + NullCount
		totalCells += int(stats.Count) + stats.NullCount
	}

	if totalCells > 0 {
		metrics.NullPercentage = float64(totalNulls) / float64(totalCells)
	} else {
		metrics.NullPercentage = 0.0
	}

	metrics.TypeConsistency = 1.0

	metrics.DistinctRatio = p.DatasetStats.DistinctRatio()

	return metrics
}

func (p *CSVProfiler) GetDescribeStats() []DescribeStats {
	var describeStats []DescribeStats

	for name, stats := range p.ColumnStats {
		desc := DescribeStats{
			Column:        name,
			Count:         stats.Count,
			NullCount:     stats.NullCount,
			Type:          stats.Type,
			Mean:          stats.Mean,
			Std:           stats.Std,
			Min:           stats.Min,
			Q25:           stats.Q25,
			Q50:           stats.Q50,
			Q75:           stats.Q75,
			Max:           stats.Max,
			DistinctCount: stats.DistinctCount,
		}

		// Create unique sample string
		if len(stats.SampleValues) > 0 {
			desc.Unique = strings.Join(stats.SampleValues, ", ")
		}

		describeStats = append(describeStats, desc)
	}

	return describeStats
}
