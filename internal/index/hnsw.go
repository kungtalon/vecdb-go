package index

import (
	"fmt"
	"sync"

	faiss "github.com/blevesearch/go-faiss"
)

type HNSWIndex struct {
	index faiss.Index
	mu    sync.Mutex
}

var _ Index = (*HNSWIndex)(nil)

func NewHNSWIndex(dim int, metric MetricType, efConstruction int, M int) (*HNSWIndex, error) {
	var metricType int
	switch metric {
	case L2:
		metricType = faiss.MetricL2
	case IP:
		metricType = faiss.MetricInnerProduct
	default:
		return nil, fmt.Errorf("unsupported metric type")
	}
	idx, err := faiss.IndexFactory(dim, fmt.Sprintf("IDMap,HNSW%d", M), metricType)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize index: %w", err)
	}
	return &HNSWIndex{index: idx}, nil
}

func (hi *HNSWIndex) Insert(params *InsertParams) error {
	hi.mu.Lock()
	defer hi.mu.Unlock()
	n, _ := params.Data.Dims()
	if n != len(params.Labels) {
		return fmt.Errorf("data and labels length mismatch")
	}
	if n == 0 {
		return nil
	}
	// Get raw data from matrix without copying
	flat := params.Data.RawData()
	err := hi.index.AddWithIDs(flat, params.Labels)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	return nil
}

func (hi *HNSWIndex) Search(query *SearchQuery, k int) (*SearchResult, error) {
	hi.mu.Lock()
	defer hi.mu.Unlock()
	ntotal := hi.index.Ntotal()
	if k > int(ntotal) {
		k = int(ntotal)
	}
	if k == 0 {
		return &SearchResult{Distances: []float32{}, Labels: []int64{}}, nil
	}
	var labels []int64
	var distances []float32
	var err error

	if query.IdFilter != nil {
		// Use FAISS SearchWithIDs for filtering during search (not post-filtering)
		selector, err := query.IdFilter.AsSelector()
		if err != nil {
			return nil, fmt.Errorf("failed to create selector: %w", err)
		}
		distances, labels, err = hi.index.SearchWithIDs(query.Vector, int64(k), selector, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to search with filter: %w", err)
		}
	} else {
		distances, labels, err = hi.index.Search(query.Vector, int64(k))
		if err != nil {
			return nil, fmt.Errorf("failed to search: %w", err)
		}
	}
	return &SearchResult{Distances: distances, Labels: labels}, nil
}
