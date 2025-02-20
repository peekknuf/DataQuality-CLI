package profiler

import (
	"strconv"
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
	// Handle null values
	if value == "" {
		s.NullCount++
		return
	}

	// Track distinct values
	if s.uniqueValues == nil {
		s.uniqueValues = make(map[string]struct{})
	}
	s.uniqueValues[value] = struct{}{}
	s.DistinctCount = len(s.uniqueValues)

	// Update min/max
	if s.Min == "" || value < s.Min {
		s.Min = value
	}
	if s.Max == "" || value > s.Max {
		s.Max = value
	}

	// Track sample values
	if len(s.SampleValues) < 5 {
		s.SampleValues = append(s.SampleValues, value)
	}

	// Infer type
	s.inferType(value)
}

func (s *ColumnStats) inferType(value string) {
	// Try parsing as int
	if _, err := strconv.Atoi(value); err == nil {
		s.Type = "int"
		return
	}

	// Try parsing as float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		s.Type = "float"
		return
	}

	// Check for date format
	if isDate(value) {
		s.Type = "date"
		return
	}

	// Default to string
	s.Type = "string"
}

func isDate(value string) bool {
	// Try common date formats
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
