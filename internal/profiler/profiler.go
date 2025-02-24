package profiler

import (
	"encoding/csv"
	"fmt"
	"io"
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
	uniqueValues  map[string]struct{}
}

func (s *ColumnStats) Update(value string) {
	if value == "" {
		s.NullCount++
		return
	}

	if s.uniqueValues == nil {
		s.uniqueValues = make(map[string]struct{})
	}
	if _, exists := s.uniqueValues[value]; !exists {
		s.uniqueValues[value] = struct{}{}
		s.DistinctCount = len(s.uniqueValues)
	}

	if s.Min == "" || value < s.Min {
		s.Min = value
	}
	if s.Max == "" || value > s.Max {
		s.Max = value
	}

	if len(s.SampleValues) < 5 {
		s.SampleValues = append(s.SampleValues, value)
	}

	s.inferType(value)
}

func (s *ColumnStats) inferType(value string) {
	if _, err := strconv.Atoi(value); err == nil {
		s.Type = "int"
		return
	}

	if _, err := strconv.ParseFloat(value, 64); err == nil {
		s.Type = "float"
		return
	}

	if isDate(value) {
		s.Type = "date"
		return
	}

	s.Type = "string"
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
	UniqueRows map[string]struct{}
	TotalRows  int
}

func (ds *DatasetStats) Update(row []string) {
	if ds.UniqueRows == nil {
		ds.UniqueRows = make(map[string]struct{})
	}

	rowKey := strings.Join(row, "|")
	if _, exists := ds.UniqueRows[rowKey]; !exists {
		ds.UniqueRows[rowKey] = struct{}{}
	}
	ds.TotalRows++
}

func (ds *DatasetStats) DistinctRatio() float64 {
	if ds.TotalRows == 0 {
		return 0.0
	}
	return float64(len(ds.UniqueRows)) / float64(ds.TotalRows)
}

type CSVProfiler struct {
	FilePath     string
	ColumnStats  map[string]*ColumnStats
	RowCount     int
	DatasetStats DatasetStats
}

func NewCSVProfiler(filePath string) *CSVProfiler {
	return &CSVProfiler{
		FilePath:    filePath,
		ColumnStats: make(map[string]*ColumnStats),
		DatasetStats: DatasetStats{
			UniqueRows: make(map[string]struct{}),
		},
	}
}

func (p *CSVProfiler) Profile() error {
	file, err := os.Open(p.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %w", err)
	}

	for _, header := range headers {
		p.ColumnStats[header] = &ColumnStats{
			Name:         header,
			SampleValues: make([]string, 0, 5),
		}
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		p.RowCount++

		for i, value := range record {
			colName := headers[i]
			stats := p.ColumnStats[colName]
			stats.Update(value)
		}

		p.DatasetStats.Update(record)
	}

	return nil
}

type QualityMetrics struct {
	TotalRows       int
	NullPercentage  float64
	TypeConsistency float64
	DistinctRatio   float64
}

func (p *CSVProfiler) CalculateQuality() QualityMetrics {
	metrics := QualityMetrics{
		TotalRows: p.RowCount,
	}

	totalNulls := 0
	for _, stats := range p.ColumnStats {
		totalNulls += stats.NullCount
	}
	metrics.NullPercentage = float64(totalNulls) / float64(p.RowCount*len(p.ColumnStats))

	metrics.TypeConsistency = 1.0

	metrics.DistinctRatio = p.DatasetStats.DistinctRatio()

	return metrics
}
