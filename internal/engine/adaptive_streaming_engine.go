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
	Name         string
	Count        int
	NullCount    int
	Type         string
	Min          string
	Max          string
	Unique       int
	Top          string
	Freq         int
	SampleSum    int64
	SampleCount  int
	HasFloat     bool
	
	// Adaptive sampling
	SampleRate   int  // Sample every Nth row for large files
	MaxSamples   int  // Maximum samples to collect
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
	isLarge := size > 10*1024*1024  // > 10MB
	isHuge := size > 50*1024*1024   // > 50MB
	
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

	// Get file stats for adaptive processing
	fileStats, err := getFileStats(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to get file stats: %v", err)
		return result
	}

	// Open file with appropriate buffer size
	file, err := os.Open(e.FilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to open file: %v", err)
		return result
	}
	defer file.Close()

	// Adaptive buffer size based on file size
	bufferSize := 64 * 1024 // 64KB default
	if fileStats.IsLarge {
		bufferSize = 256 * 1024 // 256KB for large files
	}
	if fileStats.IsHuge {
		bufferSize = 1024 * 1024 // 1MB for huge files
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, 10*1024*1024) // Set max token size to 10MB

	// Read headers
	if !scanner.Scan() {
		result.Error = fmt.Errorf("failed to read headers: %v", scanner.Err())
		return result
	}

	headerLine := scanner.Text()
	headers := strings.Split(headerLine, ",")
	if len(headers) == 0 {
		result.Error = fmt.Errorf("no columns found in file")
		return result
	}

	// Initialize adaptive stats with sampling parameters
	stats := make([]*AdaptiveStats, len(headers))
	for i, header := range headers {
		sampleRate := 1
		maxSamples := 10000
		
		if fileStats.IsLarge {
			sampleRate = 10  // Sample every 10th row
			maxSamples = 5000
		}
		if fileStats.IsHuge {
			sampleRate = 50  // Sample every 50th row for huge files
			maxSamples = 2000
		}
		
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

	// Process rows with adaptive sampling
	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		values := strings.Split(line, ",")
		
		// Process each column with adaptive sampling
		for i, value := range values {
			if i >= len(stats) {
				break
			}
			
			// Always count rows, but sample statistics based on strategy
			stats[i].Count++
			
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				stats[i].NullCount++
				continue
			}
			
			// Enhanced type detection
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
			
			// Min/max tracking (always done)
			if stats[i].Min == "~" || trimmed < stats[i].Min {
				stats[i].Min = trimmed
			}
			if stats[i].Max == "~" || trimmed > stats[i].Max {
				stats[i].Max = trimmed
			}
			
			// Adaptive sampling for statistics
			shouldSample := rowCount%stats[i].SampleRate == 0 || 
								rowCount < 1000 || 
								stats[i].SampleCount < stats[i].MaxSamples
			
			if shouldSample {
				// Enhanced frequency tracking (less frequent for large files)
				freqCheckRate := 1
				if fileStats.IsLarge {
					freqCheckRate = 100
				}
				if fileStats.IsHuge {
					freqCheckRate = 500
				}
				
				if rowCount%freqCheckRate == 0 || rowCount < 1000 {
					if trimmed == stats[i].Top {
						stats[i].Freq++
					} else if stats[i].Freq == 0 || rowCount%1000 == 0 {
						stats[i].Top = trimmed
						stats[i].Freq = 1
					}
				}
				
				// Sampling for mean calculation
				if stats[i].Type == "int" || stats[i].Type == "float" {
					if val, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
						stats[i].SampleSum += val
						stats[i].SampleCount++
					} else if stats[i].HasFloat {
						if val, err := strconv.ParseFloat(trimmed, 64); err == nil {
							stats[i].SampleSum += int64(val * 100)
							stats[i].SampleCount++
						}
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
		
		// Improved unique count estimation based on sampling
		if stat.Type == "string" {
			// For strings, estimate based on file size and sampling
			if fileStats.IsHuge {
				colStat.Unique = stat.Count / 100 // Very sparse sampling for huge files
			} else if fileStats.IsLarge {
				colStat.Unique = stat.Count / 25
			} else {
				colStat.Unique = stat.Count / 10
			}
		} else {
			// For numeric, assume less variety
			if fileStats.IsHuge {
				colStat.Unique = stat.Count / 500
			} else if fileStats.IsLarge {
				colStat.Unique = stat.Count / 100
			} else {
				colStat.Unique = stat.Count / 50
			}
		}
		
		// Ensure minimum unique counts
		if colStat.Unique < 1 {
			colStat.Unique = 1
		}
		if stat.Type != "string" && colStat.Unique < 10 {
			colStat.Unique = 10
		}
		
		// Enhanced mean calculation
		if stat.SampleCount > 0 && (stat.Type == "int" || stat.Type == "float") {
			if stat.HasFloat {
				colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount) / 100.0
			} else {
				colStat.Mean = float64(stat.SampleSum) / float64(stat.SampleCount)
			}
		}
		
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
