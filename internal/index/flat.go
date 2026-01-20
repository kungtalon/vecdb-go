package index

import (
	"fmt"
	"sync"

	faiss "github.com/blevesearch/go-faiss"
)

type FlatIndex struct {
	index faiss.Index
	mu    sync.Mutex
}

var _ Index = (*FlatIndex)(nil)

func NewFlatIndex(dim int, metric MetricType) (*FlatIndex, error) {
	var metricType int
	switch metric {
	case L2:
		metricType = faiss.MetricL2
	case IP:
		metricType = faiss.MetricInnerProduct
	default:
		return nil, fmt.Errorf("unsupported metric type")
	}
	idx, err := faiss.IndexFactory(dim, "IDMap,Flat", metricType)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize index: %w", err)
	}
	return &FlatIndex{index: idx}, nil
}

func (fi *FlatIndex) Insert(params *InsertParams) error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	n, _ := params.Data.Dims()
	if n != len(params.Labels) {
		return fmt.Errorf("data and labels length mismatch")
	}
	if n == 0 {
		return nil
	}
	// Get raw data from matrix without copying
	flat := params.Data.RawData()
	err := fi.index.AddWithIDs(flat, params.Labels)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	return nil
}

func (fi *FlatIndex) Search(query *SearchQuery, k int) (*SearchResult, error) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	ntotal := fi.index.Ntotal()
	if k > int(ntotal) {
		k = int(ntotal)
	}
	if k == 0 {
		return &SearchResult{Distances: []float32{}, Labels: []int64{}}, nil
	}
	var labels []int64
	var distances []float32
	var err error

	if query.IdFilter != nil && !query.IdFilter.IsEmpty() {
		// Use FAISS SearchWithIDs for filtering during search (not post-filtering)
		selector, err := query.IdFilter.AsSelector()
		if err != nil {
			return nil, fmt.Errorf("failed to create selector: %w", err)
		}
		distances, labels, err = fi.index.SearchWithIDs(query.Vector, int64(k), selector, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to search with filter: %w", err)
		}
	} else {
		distances, labels, err = fi.index.Search(query.Vector, int64(k))
		if err != nil {
			return nil, fmt.Errorf("failed to search: %w", err)
		}
	}
	return &SearchResult{Distances: distances, Labels: labels}, nil
}
