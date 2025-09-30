package profiler

import (
	"fmt"
	"os"
	"testing"
)

// BenchmarkMemoryUsage compares memory usage between old and new approaches
func BenchmarkMemoryUsage(b *testing.B) {
	// Create a large test CSV file
	filename := "large_test.csv"
	defer os.Remove(filename)

	// Generate test data with many unique values
	file, err := os.Create(filename)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	// Write header
	file.WriteString("col1,col2,col3,col4,col5\n")

	// Write 100,000 rows with varied data
	for i := 0; i < 100000; i++ {
		row := fmt.Sprintf("value%d,unique_%d,data_%d,item_%d,entry_%d\n",
			i%1000, i, i%500, i%200, i%100)
		file.WriteString(row)
	}

	b.ResetTimer()

	// Test with different configurations
	configs := []struct {
		name      string
		config    ProfilerConfig
	}{
		{
			name: "Small_Memory",
			config: ProfilerConfig{
				MaxColumnSampleSize: 1000,
				MaxRowSampleSize:    5000,
				ChunkSize:          5000,
				MemoryLimitMB:      100,
			},
		},
		{
			name: "Medium_Memory",
			config: ProfilerConfig{
				MaxColumnSampleSize: 10000,
				MaxRowSampleSize:    50000,
				ChunkSize:          10000,
				MemoryLimitMB:      512,
			},
		},
		{
			name: "Large_Memory",
			config: ProfilerConfig{
				MaxColumnSampleSize: 50000,
				MaxRowSampleSize:    100000,
				ChunkSize:          20000,
				MemoryLimitMB:      1024,
			},
		},
	}

	for _, tc := range configs {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				profiler := NewCSVProfilerWithConfig(filename, tc.config)
				err := profiler.Profile()
				if err != nil {
					b.Fatalf("Profile failed: %v", err)
				}

				// Verify results are reasonable
				if profiler.RowCount != 100000 {
					b.Errorf("Expected 100000 rows, got %d", profiler.RowCount)
				}

				metrics := profiler.CalculateQuality()
				if metrics.NullPercentage < 0 || metrics.NullPercentage > 1 {
					b.Errorf("Invalid null percentage: %f", metrics.NullPercentage)
				}
			}
		})
	}
}

// BenchmarkChunkSize tests performance with different chunk sizes
func BenchmarkChunkSize(b *testing.B) {
	// Create test file
	filename := "chunk_test.csv"
	defer os.Remove(filename)

	file, err := os.Create(filename)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	file.WriteString("col1,col2,col3\n")
	for i := 0; i < 50000; i++ {
		file.WriteString(fmt.Sprintf("val%d,val%d,val%d\n", i, i%100, i%50))
	}

	chunkSizes := []int{1000, 5000, 10000, 20000}

	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("Chunk_%d", chunkSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				config := ProfilerConfig{
					MaxColumnSampleSize: 10000,
					MaxRowSampleSize:    50000,
					ChunkSize:          chunkSize,
					MemoryLimitMB:      512,
				}

				profiler := NewCSVProfilerWithConfig(filename, config)
				err := profiler.Profile()
				if err != nil {
					b.Fatalf("Profile failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSamplingAccuracy tests the accuracy of our sampling approach
func BenchmarkSamplingAccuracy(b *testing.B) {
	filename := "accuracy_test.csv"
	defer os.Remove(filename)

	file, err := os.Create(filename)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}
	defer file.Close()

	file.WriteString("col1\n")
	// Create data with 10000 unique values
	for i := 0; i < 10000; i++ {
		file.WriteString(fmt.Sprintf("unique_value_%d\n", i))
	}

	sampleSizes := []int{100, 500, 1000, 5000}

	for _, sampleSize := range sampleSizes {
		b.Run(fmt.Sprintf("Sample_%d", sampleSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				config := ProfilerConfig{
					MaxColumnSampleSize: sampleSize,
					MaxRowSampleSize:    sampleSize * 5,
					ChunkSize:          1000,
					MemoryLimitMB:      100,
				}

				profiler := NewCSVProfilerWithConfig(filename, config)
				err := profiler.Profile()
				if err != nil {
					b.Fatalf("Profile failed: %v", err)
				}

				// Check that we got a reasonable estimate
				colStats := profiler.ColumnStats["col1"]
				if colStats.DistinctCount <= 0 {
					b.Errorf("Expected positive distinct count, got %d", colStats.DistinctCount)
				}
			}
		})
	}
}