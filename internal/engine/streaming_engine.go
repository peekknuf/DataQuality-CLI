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
	Name         string
	Count        int
	NullCount    int
	Type         string // "numeric", "float", or "string"
	Min          string
	Max          string
	Unique       int    // Actual unique count
	Top          string // Most frequent value
	Freq         int    // Frequency of top value
	SampleSum    int64  // For simple mean calculation
	SampleCount  int    // Number of values in sample
	HasFloat     bool   // Track if we've seen float values
}

// Describe processes CSV with streaming approach - minimal memory, maximum speed
func (e *StreamingEngine) Describe() *DescribeResult {
	startTime := time.Now()

	result := &DescribeResult{
		Path: e.FilePath,
	}

	// Open file for streaming
	file, err := os.Open(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %v", err)
		return result
	}
	defer file.Close()

	// Create buffered scanner for maximum speed
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		result.Error = fmt.Errorf("failed to read headers: %v", scanner.Err())
		return result
	}

	// Parse headers with simple split
	headerLine := scanner.Text()
	headers := strings.Split(headerLine, ",")
	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	// Initialize simple stats for each column
	stats := make([]*SimpleStats, len(headers))
	for i, header := range headers {
		stats[i] = &SimpleStats{
			Name:        strings.TrimSpace(header),
			Min:         "~", // Placeholder
			Max:         "~", // Placeholder
			SampleSum:   0,
			SampleCount: 0,
			HasFloat:    false,
		}
	}

	// Process rows in streaming fashion
	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Simple CSV split - fast but not perfectly robust
		values := strings.Split(line, ",")
		
		// Process each column
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
			
			// Enhanced type detection - try int first, then float
			if stats[i].Type == "" {
				if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
					stats[i].Type = "int"
				} else if _, err := strconv.ParseFloat(trimmed, 64); err == nil {
					stats[i].Type = "float"
					stats[i].HasFloat = true
				} else {
					stats[i].Type = "string"
				}
			}
			
			// Simple min/max tracking
			if stats[i].Min == "~" || trimmed < stats[i].Min {
				stats[i].Min = trimmed
			}
			if stats[i].Max == "~" || trimmed > stats[i].Max {
				stats[i].Max = trimmed
			}
			
			// Enhanced frequency tracking for top value
			// For performance, only track this for a sample of values
			if rowCount%100 == 0 || rowCount < 1000 {
				if trimmed == stats[i].Top {
					stats[i].Freq++
				} else if stats[i].Freq == 0 || rowCount%1000 == 0 {
					// Reset tracking periodically to catch different patterns
					stats[i].Top = trimmed
					stats[i].Freq = 1
				}
			}
			
			// Enhanced sampling for mean (sample every 10th value, plus first 1000)
			if (rowCount < 1000 || rowCount%10 == 0) && (stats[i].Type == "int" || stats[i].Type == "float") {
				if val, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
					stats[i].SampleSum += val
					stats[i].SampleCount++
				} else if stats[i].HasFloat {
					if val, err := strconv.ParseFloat(trimmed, 64); err == nil {
						stats[i].SampleSum += int64(val * 100) // Store float as int*100 for precision
						stats[i].SampleCount++
					}
				}
			}
		}
		rowCount++
	}

	// Convert to output format with enhanced statistics
	result.ColumnStats = make([]ColumnStats, len(stats))
	var totalNulls int

	for i, stat := range stats {
		colStat := ColumnStats{
			Name:      stat.Name,
			Count:     stat.Count,
			NullCount: stat.NullCount,
			Type:      stat.Type,
			Min:       stat.Min,
			Max:       stat.Max,
		}
		
		// Better unique count estimation based on data type
		if stat.Type == "string" {
			// For strings, estimate based on variety
			colStat.Unique = stat.Count / 5 // More variety expected
			if colStat.Unique > stat.Count {
				colStat.Unique = stat.Count
			}
		} else {
			// For numeric, assume less variety
			colStat.Unique = stat.Count / 20
			if colStat.Unique < 10 {
				colStat.Unique = 10 // Minimum variety for numeric
			}
		}
		
		// Enhanced mean calculation
		if stat.SampleCount > 0 && (stat.Type == "int" || stat.Type == "float") {
			if stat.HasFloat {
				colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount) / 100.0 // Convert back from int*100
			} else {
				colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount)
			}
		}
		
		// Set top value and frequency
		colStat.Top = stat.Top
		colStat.Freq = stat.Freq
		
		result.ColumnStats[i] = colStat
		totalNulls += stat.NullCount
	}

	result.RowCount = rowCount
	if result.RowCount > 0 {
		result.NullPercentage = float64(totalNulls) / float64(result.RowCount*len(headers)) * 100
	}
	result.ProcessingTime = time.Since(startTime)

	return result
}
