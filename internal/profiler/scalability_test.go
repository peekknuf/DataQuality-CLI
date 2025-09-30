package profiler

import (
	"testing"
)

func TestReservoirSampling(t *testing.T) {
	stats := NewColumnStats("test", 100)

	// Add more unique values than the sample size
	for i := 0; i < 1000; i++ {
		stats.Update(string(rune('A' + i%26)) + string(rune('a' + i%26)))
	}

	// Should not exceed sample size in the slice
	if len(stats.sampleSlice) > 100 {
		t.Errorf("Expected sample slice size <= 100, got %d", len(stats.sampleSlice))
	}

	// Should have reasonable distinct count estimation
	if stats.DistinctCount < 0 || stats.DistinctCount > 1000 {
		t.Errorf("Expected distinct count between 0 and 1000, got %d", stats.DistinctCount)
	}

	// Should have processed all values
	if stats.Count != 1000 {
		t.Errorf("Expected count 1000, got %d", stats.Count)
	}
}

func TestDatasetStatsSampling(t *testing.T) {
	ds := NewDatasetStats(50)

	// Add many unique rows
	for i := 0; i < 500; i++ {
		row := []string{"value1", "value2", string(rune('A' + i%26))}
		ds.Update(row)
	}

	// Should not exceed sample size
	if len(ds.sampleHashes) > 50 {
		t.Errorf("Expected sample hash count <= 50, got %d", len(ds.sampleHashes))
	}

	// Should have reasonable distinct ratio
	ratio := ds.DistinctRatio()
	if ratio <= 0 || ratio > 1 {
		t.Errorf("Expected ratio between 0 and 1, got %f", ratio)
	}

	// Should have processed all rows
	if ds.TotalRows != 500 {
		t.Errorf("Expected total rows 500, got %d", ds.TotalRows)
	}
}

func TestProfilerConfig(t *testing.T) {
	config := ProfilerConfig{
		MaxColumnSampleSize: 1000,
		MaxRowSampleSize:    5000,
		ChunkSize:          5000,
		MemoryLimitMB:      512,
	}

	profiler := NewCSVProfilerWithConfig("test.csv", config)

	if profiler.Config.MaxColumnSampleSize != 1000 {
		t.Errorf("Expected MaxColumnSampleSize 1000, got %d", profiler.Config.MaxColumnSampleSize)
	}

	if profiler.Config.ChunkSize != 5000 {
		t.Errorf("Expected ChunkSize 5000, got %d", profiler.Config.ChunkSize)
	}
}

func TestChunkProcessing(t *testing.T) {
	config := ProfilerConfig{
		MaxColumnSampleSize: 100,
		MaxRowSampleSize:    500,
		ChunkSize:          10,
		MemoryLimitMB:      100,
	}

	profiler := NewCSVProfilerWithConfig("test.csv", config)

	// Initialize column stats (normally done in Profile method)
	headers := []string{"col1", "col2", "col3"}
	for _, header := range headers {
		profiler.ColumnStats[header] = NewColumnStats(header, config.MaxColumnSampleSize)
	}

	// Simulate chunk processing
	chunk := make([][]string, 0, config.ChunkSize)

	// Add data to chunk
	for i := 0; i < 15; i++ {
		row := []string{"val1", "val2", "val3"}
		chunk = append(chunk, row)
		profiler.RowCount++
	}

	// Process first chunk
	firstChunk := chunk[:10]
	profiler.processChunk(firstChunk, headers)

	// Process remaining chunk
	remainingChunk := chunk[10:]
	profiler.processChunk(remainingChunk, headers)

	// Verify row count
	if profiler.RowCount != 15 {
		t.Errorf("Expected row count 15, got %d", profiler.RowCount)
	}
}