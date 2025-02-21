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
	if value == "" {
		s.NullCount++
		return
	}

	if s.uniqueValues == nil {
		s.uniqueValues = make(map[string]struct{})
	}
	s.uniqueValues[value] = struct{}{}
	s.DistinctCount = len(s.uniqueValues)

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
