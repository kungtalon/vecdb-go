package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vecdb-go/internal/common/math"
	"vecdb-go/internal/filter"
)

func setupHNSW(nrow, dim int, metricType MetricType) (*HNSWIndex, *math.Matrix32, []int64, error) {
	// Use settings similar to the Rust tests
	hnswParams := &HNSWParams{
		EFConstruction: 20,
		M:              100,
	}

	index, err := NewHNSWIndex(dim, metricType, hnswParams.EFConstruction, hnswParams.M)
	if err != nil {
		return nil, nil, nil, err
	}

	// Generate test data: sequential floats from 1 to nrow*dim
	data := make([]float32, nrow*dim)
	for i := 0; i < nrow*dim; i++ {
		data[i] = float32(i + 1)
	}

	matrix := &math.Matrix32{
		Rows: nrow,
		Cols: dim,
		Data: data,
	}

	// Generate labels from 1 to nrow
	labels := make([]int64, nrow)
	for i := 0; i < nrow; i++ {
		labels[i] = int64(i + 1)
	}

	return index, matrix, labels, nil
}

func TestHNSWInsertMany(t *testing.T) {
	index, data, labels, err := setupHNSW(2, 4, L2)
	require.NoError(t, err, "Failed to setup")

	// Test with parallel insert option
	params := NewInsertParams(data, labels).With(&HnswParams{Parallel: true})
	err = index.Insert(params)
	require.NoError(t, err, "Insert failed")

	ntotal := index.index.Ntotal()
	assert.Equal(t, int64(2), ntotal)
}

func TestHNSWSearch(t *testing.T) {
	index, data, labels, err := setupHNSW(2, 4, L2)
	require.NoError(t, err, "Failed to setup")

	params := NewInsertParams(data, labels)
	err = index.Insert(params)
	require.NoError(t, err, "Insert failed")

	query := []float32{1.1, 2.1, 2.9, 3.9}
	k := 2
	searchQuery := NewSearchQuery(query).With(&HnswSearchOption{EfSearch: 20})
	result, err := index.Search(searchQuery, k)
	require.NoError(t, err, "Search failed")

	assert.Len(t, result.Labels, k)
	assert.Equal(t, labels[0], result.Labels[0])

	t.Logf("Search result: %v", result)
}

func TestHNSWSearchWithParams(t *testing.T) {
	index, data, labels, err := setupHNSW(4, 5, L2)
	require.NoError(t, err, "Failed to setup")

	params := NewInsertParams(data, labels)
	err = index.Insert(params)
	require.NoError(t, err, "Insert failed")

	query := []float32{1.1, 2.1, 2.9, 3.9, 5.0}
	k := 3

	// Test search without filter
	searchQuery := NewSearchQuery(query).With(&HnswSearchOption{EfSearch: 20})
	originalResult, err := index.Search(searchQuery, k)
	require.NoError(t, err, "Search without filter failed")

	assert.Equal(t, labels[0], originalResult.Labels[0])

	// Test search with filter (exclude first label)
	idFilter := filter.NewIdFilter()
	for _, label := range labels[1:] {
		idFilter.Add(uint64(label))
	}

	searchQueryWithFilter := NewSearchQuery(query).
		With(&HnswSearchOption{EfSearch: 20}).
		WithFilter(idFilter)
	result, err := index.Search(searchQueryWithFilter, k)
	require.NoError(t, err, "Search with filter failed")

	assert.Len(t, result.Labels, k)
	assert.NotEqual(t, labels[0], result.Labels[0], "First label should be filtered out")
}

func TestHNSWSearchWithEmptyFilter(t *testing.T) {
	index, data, labels, err := setupHNSW(4, 5, L2)
	require.NoError(t, err, "Failed to setup")

	params := NewInsertParams(data, labels)
	err = index.Insert(params)
	require.NoError(t, err, "Insert failed")

	query := []float32{1.1, 2.1, 2.9, 3.9, 5.0}
	k := 3

	// Test search with empty filter - should behave like no filter
	idFilter := filter.NewIdFilter()
	searchQueryWithEmptyFilter := NewSearchQuery(query).
		With(&HnswSearchOption{EfSearch: 20}).
		WithFilter(idFilter)
	result, err := index.Search(searchQueryWithEmptyFilter, k)
	require.NoError(t, err, "Search with empty filter failed")

	assert.Len(t, result.Labels, k)
	// Should return the closest label (label[0]) since filter is empty
	assert.Equal(t, labels[0], result.Labels[0])
}
