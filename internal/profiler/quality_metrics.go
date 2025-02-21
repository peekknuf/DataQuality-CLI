package profiler

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

	totalDistinct := 0
	for _, stats := range p.ColumnStats {
		totalDistinct += stats.DistinctCount
	}
	metrics.DistinctRatio = float64(totalDistinct) / float64(p.RowCount*len(p.ColumnStats))

	return metrics
}
