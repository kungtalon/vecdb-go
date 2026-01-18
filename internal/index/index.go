package index

import "fmt"

var (
	ErrInvalidHNSWParams    = fmt.Errorf("invalid HNSW parameters")
	ErrUnsupportedIndexType = fmt.Errorf("unsupported index type")
)

type HNSWParams struct {
	EFConstruction int
	M              int
}

type Index interface {
	Insert(params *InsertParams) error
	Search(query *SearchQuery, k int) (*SearchResult, error)
}

func NewIndex(indexType string, dim int, metric MetricType, hnswParams *HNSWParams) (Index, error) {
	switch indexType {
	case "flat":
		return NewFlatIndex(dim, metric)
	case "hnsw":
		if hnswParams == nil {
			return nil, ErrInvalidHNSWParams
		}
		return NewHNSWIndex(dim, metric, hnswParams.EFConstruction, hnswParams.M)
	default:
		return nil, ErrUnsupportedIndexType
	}
}

type MetricType string

const (
	IP MetricType = "ip"
	L2 MetricType = "l2"
)

type SearchResult struct {
	Distances []float32
	Labels    []int64
}
