package engine

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AdaptiveStreamingEngine provides file-size-aware processing
type AdaptiveStreamingEngine struct {
	FilePath string
}

// NewAdaptiveStreamingEngine creates a new adaptive streaming engine
func NewAdaptiveStreamingEngine(filePath string) *AdaptiveStreamingEngine {
	return &AdaptiveStreamingEngine{
		FilePath: filePath,
	}
}

// FileStats contains file metadata for adaptive processing
type FileStats struct {
	Size     int64
	IsLarge  bool
	IsHuge   bool
	Strategy string
}

// AdaptiveStats for size-aware processing
type AdaptiveStats struct {
	Name        string
	Count       int
	NullCount   int
	Type        string
	Min         string
	Max         string
	Unique      int
	Top         string
	Freq        int
	SampleSum   int64
	SampleCount int
	HasFloat    bool

	// Adaptive sampling
	SampleRate int // Sample every Nth row for large files
	MaxSamples int // Maximum samples to collect
}

// getFileStats determines optimal processing strategy based on file size
func getFileStats(filePath string) (FileStats, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return FileStats{}, err
	}

	size := fileInfo.Size()

	// Determine strategy based on file size
	var strategy string
	isLarge := size > 10*1024*1024 // > 10MB
	isHuge := size > 50*1024*1024  // > 50MB

	if isHuge {
		strategy = "memory-efficient"
	} else if isLarge {
		strategy = "streaming-optimized"
	} else {
		strategy = "standard"
	}

	return FileStats{
		Size:     size,
		IsLarge:  isLarge,
		IsHuge:   isHuge,
		Strategy: strategy,
	}, nil
}

// Describe processes CSV with adaptive algorithms based on file size
func (e *AdaptiveStreamingEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	fileStats, err := getFileStats(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to get file stats: %v", err)
		return result
	}

	file, scanner, headers, err := e.setupFileScanner(fileStats)
	if err != nil {
		result.Error = err
		return result
	}
	defer file.Close()

	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	stats := e.initializeStats(headers, fileStats)
	rowCount := e.processRows(scanner, stats, fileStats)
	result.ColumnStats, result.RowCount = e.convertToResult(stats, rowCount, len(headers), fileStats)

	var totalNulls int
	for _, stat := range stats {
		totalNulls += stat.NullCount
	}

	if result.RowCount > 0 {
		result.NullPercentage = float64(totalNulls) / float64(result.RowCount*len(headers)) * 100
	}
	result.ProcessingTime = time.Since(startTime)

	return result
}

func (e *AdaptiveStreamingEngine) setupFileScanner(fileStats FileStats) (*os.File, *bufio.Scanner, []string, error) {
	file, err := os.Open(e.FilePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	bufferSize := e.getAdaptiveBufferSize(fileStats)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, 10*1024*1024)

	if !scanner.Scan() {
		return nil, nil, nil, fmt.Errorf("failed to read headers: %v", scanner.Err())
	}

	headerLine := scanner.Text()
	headers := strings.Split(headerLine, ",")
	return file, scanner, headers, nil
}

func (e *AdaptiveStreamingEngine) getAdaptiveBufferSize(fileStats FileStats) int {
	if fileStats.IsHuge {
		return 1024 * 1024 // 1MB for huge files
	}
	if fileStats.IsLarge {
		return 256 * 1024 // 256KB for large files
	}
	return 64 * 1024 // 64KB default
}

func (e *AdaptiveStreamingEngine) initializeStats(headers []string, fileStats FileStats) []*AdaptiveStats {
	stats := make([]*AdaptiveStats, len(headers))
	for i, header := range headers {
		sampleRate, maxSamples := e.getSamplingParameters(fileStats)
		stats[i] = &AdaptiveStats{
			Name:        strings.TrimSpace(header),
			Min:         "~",
			Max:         "~",
			SampleSum:   0,
			SampleCount: 0,
			HasFloat:    false,
			SampleRate:  sampleRate,
			MaxSamples:  maxSamples,
		}
	}
	return stats
}

func (e *AdaptiveStreamingEngine) getSamplingParameters(fileStats FileStats) (int, int) {
	if fileStats.IsHuge {
		return 50, 2000 // Sample every 50th row, max 2000 samples
	}
	if fileStats.IsLarge {
		return 10, 5000 // Sample every 10th row, max 5000 samples
	}
	return 1, 10000 // Sample every row, max 10000 samples
}

func (e *AdaptiveStreamingEngine) processRows(scanner *bufio.Scanner, stats []*AdaptiveStats, fileStats FileStats) int {
	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		values := strings.Split(line, ",")
		e.processRow(values, stats, rowCount, fileStats)
		rowCount++
	}
	return rowCount
}

func (e *AdaptiveStreamingEngine) processRow(values []string, stats []*AdaptiveStats, rowCount int, fileStats FileStats) {
	for i, value := range values {
		if i >= len(stats) {
			break
		}

		stats[i].Count++
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			stats[i].NullCount++
			continue
		}

		e.detectDataType(stats[i], trimmed)
		e.updateMinMax(stats[i], trimmed)

		if e.shouldSample(stats[i], rowCount) {
			e.updateFrequency(stats[i], trimmed, rowCount, fileStats)
			e.updateSampleSum(stats[i], trimmed)
		}
	}
}

func (e *AdaptiveStreamingEngine) detectDataType(stat *AdaptiveStats, value string) {
	if stat.Type != "" {
		return
	}

	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		stat.Type = "int"
	} else if _, err := strconv.ParseFloat(value, 64); err == nil {
		stat.Type = "float"
		stat.HasFloat = true
	} else {
		stat.Type = "string"
	}
}

func (e *AdaptiveStreamingEngine) updateMinMax(stat *AdaptiveStats, value string) {
	if stat.Min == "~" || value < stat.Min {
		stat.Min = value
	}
	if stat.Max == "~" || value > stat.Max {
		stat.Max = value
	}
}

func (e *AdaptiveStreamingEngine) shouldSample(stat *AdaptiveStats, rowCount int) bool {
	return rowCount%stat.SampleRate == 0 ||
		rowCount < 1000 ||
		stat.SampleCount < stat.MaxSamples
}

func (e *AdaptiveStreamingEngine) updateFrequency(stat *AdaptiveStats, value string, rowCount int, fileStats FileStats) {
	freqCheckRate := e.getFrequencyCheckRate(fileStats)
	if rowCount%freqCheckRate == 0 || rowCount < 1000 {
		if value == stat.Top {
			stat.Freq++
		} else if stat.Freq == 0 || rowCount%1000 == 0 {
			stat.Top = value
			stat.Freq = 1
		}
	}
}

func (e *AdaptiveStreamingEngine) getFrequencyCheckRate(fileStats FileStats) int {
	if fileStats.IsHuge {
		return 500
	}
	if fileStats.IsLarge {
		return 100
	}
	return 1
}

func (e *AdaptiveStreamingEngine) updateSampleSum(stat *AdaptiveStats, value string) {
	if stat.Type != "int" && stat.Type != "float" {
		return
	}

	if val, err := strconv.ParseInt(value, 10, 64); err == nil {
		stat.SampleSum += val
		stat.SampleCount++
	} else if stat.HasFloat {
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			stat.SampleSum += int64(val * 100)
			stat.SampleCount++
		}
	}
}

func (e *AdaptiveStreamingEngine) convertToResult(stats []*AdaptiveStats, rowCount int, headerCount int, fileStats FileStats) ([]ColumnStats, int) {
	columnStats := make([]ColumnStats, len(stats))
	var totalNulls int

	for i, stat := range stats {
		colStat := e.createColumnStat(stat, fileStats)
		columnStats[i] = colStat
		totalNulls += stat.NullCount
	}

	return columnStats, rowCount
}

func (e *AdaptiveStreamingEngine) createColumnStat(stat *AdaptiveStats, fileStats FileStats) ColumnStats {
	colStat := ColumnStats{
		Name:      stat.Name,
		Count:     stat.Count,
		NullCount: stat.NullCount,
		Type:      stat.Type,
		Min:       stat.Min,
		Max:       stat.Max,
		Top:       stat.Top,
		Freq:      stat.Freq,
	}

	colStat.Unique = e.estimateUniqueCount(stat, fileStats)
	e.ensureMinimumUniqueCounts(&colStat, stat)

	if stat.SampleCount > 0 && (stat.Type == "int" || stat.Type == "float") {
		colStat.Mean = e.calculateMean(stat)
	}

	return colStat
}

func (e *AdaptiveStreamingEngine) estimateUniqueCount(stat *AdaptiveStats, fileStats FileStats) int {
	if stat.Type == "string" {
		if fileStats.IsHuge {
			return stat.Count / 100
		}
		if fileStats.IsLarge {
			return stat.Count / 25
		}
		return stat.Count / 10
	}

	// For numeric types
	if fileStats.IsHuge {
		return stat.Count / 500
	}
	if fileStats.IsLarge {
		return stat.Count / 100
	}
	return stat.Count / 50
}

func (e *AdaptiveStreamingEngine) ensureMinimumUniqueCounts(colStat *ColumnStats, stat *AdaptiveStats) {
	if colStat.Unique < 1 {
		colStat.Unique = 1
	}
	if stat.Type != "string" && colStat.Unique < 10 {
		colStat.Unique = 10
	}
}

func (e *AdaptiveStreamingEngine) calculateMean(stat *AdaptiveStats) float64 {
	if stat.HasFloat {
		return float64(stat.SampleSum) / float64(stat.SampleCount) / 100.0
	}
	return float64(stat.SampleSum) / float64(stat.SampleCount)
}
