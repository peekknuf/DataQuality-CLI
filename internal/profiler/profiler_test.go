package profiler

import (
	"math"
	"os"
	"testing"
)

func createTestCSV(content string) string {
	filename := "test.csv"
	os.WriteFile(filename, []byte(content), 0644)
	return filename
}

func TestCSVProfiler(t *testing.T) {
	file := createTestCSV(`A,B,C
1,2,3
4,5,6
1,2,3
7,8,9
,10,11`)

	defer os.Remove(file)

	profiler := NewCSVProfiler(file)
	err := profiler.Profile()
	if err != nil {
		t.Fatalf("Profile() failed: %v", err)
	}

	metrics := profiler.CalculateQuality()

	if metrics.TotalRows != 5 {
		t.Errorf("Expected 5 rows, got %d", metrics.TotalRows)
	}

	// Allow for some tolerance in null percentage calculation
	expectedNullPercentage := 1.0 / 15.0 // 1 null out of 15 total values
	if math.Abs(metrics.NullPercentage-expectedNullPercentage) > 0.01 {
		t.Errorf("Expected Null Percentage ~%f, got %f", expectedNullPercentage, metrics.NullPercentage)
	}

	// Allow for tolerance in distinct ratio (using estimation)
	if metrics.DistinctRatio < 0.6 || metrics.DistinctRatio > 1.0 {
		t.Errorf("Expected Distinct Ratio between 0.6 and 1.0, got %f", metrics.DistinctRatio)
	}

	// Test pandas.describe() functionality
	describeStats := profiler.GetDescribeStats()
	if len(describeStats) != 3 {
		t.Errorf("Expected 3 columns in describe stats, got %d", len(describeStats))
	}

	// Check numeric column A
	var colA *DescribeStats
	for _, stats := range describeStats {
		if stats.Column == "A" {
			colA = &stats
			break
		}
	}

	if colA == nil {
		t.Fatal("Column A not found in describe stats")
	}

	if colA.Type != "int" {
		t.Errorf("Expected column A type to be int, got %s", colA.Type)
	}

	if colA.Count != 4 { // 4 non-null values
		t.Errorf("Expected column A count to be 4, got %d", colA.Count)
	}

	if colA.NullCount != 1 {
		t.Errorf("Expected column A null count to be 1, got %d", colA.NullCount)
	}
}
