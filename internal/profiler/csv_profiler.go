package profiler

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type CSVProfiler struct {
	FilePath    string
	ColumnStats map[string]*ColumnStats
	RowCount    int
}

func NewCSVProfiler(filePath string) *CSVProfiler {
	return &CSVProfiler{
		FilePath:    filePath,
		ColumnStats: make(map[string]*ColumnStats),
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
	}

	return nil
}
