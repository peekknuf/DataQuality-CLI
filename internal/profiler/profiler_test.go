package profiler

import (
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

	if metrics.NullPercentage != 0.06666666666666667 {
		t.Errorf("Expected Null Percentage ~0.067, got %f", metrics.NullPercentage)
	}

	if metrics.DistinctRatio != 0.8 {
		t.Errorf("Expected Distinct Ratio 0.8, got %f", metrics.DistinctRatio)
	}
}
