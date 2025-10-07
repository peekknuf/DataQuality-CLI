package engine

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// StreamingEngine provides ultra-fast, streaming CSV processing
type StreamingEngine struct {
	FilePath string
}

// NewStreamingEngine creates a new streaming engine
func NewStreamingEngine(filePath string) *StreamingEngine {
	return &StreamingEngine{
		FilePath: filePath,
	}
}

// SimpleStats for ultra-fast processing
type SimpleStats struct {
	Name        string
	Count       int
	NullCount   int
	Type        string // "numeric", "float", or "string"
	Min         string
	Max         string
	Unique      int    // Actual unique count
	Top         string // Most frequent value
	Freq        int    // Frequency of top value
	SampleSum   int64  // For simple mean calculation
	SampleCount int    // Number of values in sample
	HasFloat    bool   // Track if we've seen float values
}

// Describe processes CSV with streaming approach - minimal memory, maximum speed
func (e *StreamingEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	file, scanner, headers, err := e.setupStreamingScanner()
	if err != nil {
		result.Error = err
		return result
	}
	defer file.Close()

	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	stats := e.initializeSimpleStats(headers)
	rowCount := e.processStreamingRows(scanner, stats)
	result.ColumnStats, result.RowCount = e.convertStreamingResults(stats, rowCount, len(headers))

	result.NullPercentage = e.calculateStreamingNullPercentage(result.ColumnStats, result.RowCount, len(headers))
	result.ProcessingTime = time.Since(startTime)

	return result
}

func (e *StreamingEngine) setupStreamingScanner() (*os.File, *bufio.Scanner, []string, error) {
	file, err := os.Open(e.FilePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file: %v", err)
	}

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, nil, nil, fmt.Errorf("failed to read headers: %v", scanner.Err())
	}

	headerLine := scanner.Text()
	headers := strings.Split(headerLine, ",")
	return file, scanner, headers, nil
}

func (e *StreamingEngine) initializeSimpleStats(headers []string) []*SimpleStats {
	stats := make([]*SimpleStats, len(headers))
	for i, header := range headers {
		stats[i] = &SimpleStats{
			Name:        strings.TrimSpace(header),
			Min:         "~",
			Max:         "~",
			SampleSum:   0,
			SampleCount: 0,
			HasFloat:    false,
		}
	}
	return stats
}

func (e *StreamingEngine) processStreamingRows(scanner *bufio.Scanner, stats []*SimpleStats) int {
	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		values := strings.Split(line, ",")
		e.processStreamingRow(values, stats, rowCount)
		rowCount++
	}
	return rowCount
}

func (e *StreamingEngine) processStreamingRow(values []string, stats []*SimpleStats, rowCount int) {
	for i, value := range values {
		if i >= len(stats) {
			break
		}

		trimmed := strings.TrimSpace(value)
		stats[i].Count++

		if trimmed == "" {
			stats[i].NullCount++
			continue
		}

		e.detectStreamingType(stats[i], trimmed)
		e.updateStreamingMinMax(stats[i], trimmed)
		e.updateStreamingFrequency(stats[i], trimmed, rowCount)
		e.updateStreamingSample(stats[i], trimmed, rowCount)
	}
}

func (e *StreamingEngine) detectStreamingType(stat *SimpleStats, value string) {
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

func (e *StreamingEngine) updateStreamingMinMax(stat *SimpleStats, value string) {
	if stat.Min == "~" || value < stat.Min {
		stat.Min = value
	}
	if stat.Max == "~" || value > stat.Max {
		stat.Max = value
	}
}

func (e *StreamingEngine) updateStreamingFrequency(stat *SimpleStats, value string, rowCount int) {
	if rowCount%100 == 0 || rowCount < 1000 {
		if value == stat.Top {
			stat.Freq++
		} else if stat.Freq == 0 || rowCount%1000 == 0 {
			stat.Top = value
			stat.Freq = 1
		}
	}
}

func (e *StreamingEngine) updateStreamingSample(stat *SimpleStats, value string, rowCount int) {
	if (rowCount < 1000 || rowCount%10 == 0) && (stat.Type == "int" || stat.Type == "float") {
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
}

func (e *StreamingEngine) convertStreamingResults(stats []*SimpleStats, rowCount int, headerCount int) ([]ColumnStats, int) {
	columnStats := make([]ColumnStats, len(stats))
	var totalNulls int

	for i, stat := range stats {
		colStat := e.createStreamingColumnStat(stat)
		columnStats[i] = colStat
		totalNulls += stat.NullCount
	}

	return columnStats, totalNulls
}

func (e *StreamingEngine) createStreamingColumnStat(stat *SimpleStats) ColumnStats {
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

	colStat.Unique = e.estimateStreamingUniqueCount(stat)
	e.calculateStreamingMean(&colStat, stat)

	return colStat
}

func (e *StreamingEngine) estimateStreamingUniqueCount(stat *SimpleStats) int {
	if stat.Type == "string" {
		colStat := stat.Count / 5
		if colStat > stat.Count {
			return stat.Count
		}
		return colStat
	}

	colStat := stat.Count / 20
	if colStat < 10 {
		return 10
	}
	return colStat
}

func (e *StreamingEngine) calculateStreamingMean(colStat *ColumnStats, stat *SimpleStats) {
	if stat.SampleCount > 0 && (stat.Type == "int" || stat.Type == "float") {
		if stat.HasFloat {
			colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount) / 100.0
		} else {
			colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount)
		}
	}
}

func (e *StreamingEngine) calculateStreamingNullPercentage(columnStats []ColumnStats, rowCount int, headerCount int) float64 {
	if rowCount == 0 {
		return 0
	}

	var totalNulls int
	for _, stats := range columnStats {
		totalNulls += stats.NullCount
	}

	return float64(totalNulls) / float64(rowCount*headerCount) * 100
}
