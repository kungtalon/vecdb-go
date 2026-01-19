package index

import (
	"vecdb-go/internal/common/math"
	"vecdb-go/internal/filter"
)

type SearchQuery struct {
	Vector   []float32
	IdFilter *filter.IdFilter
	Hnsw     *HnswSearchOption
}

type SearchOption interface {
	SetQuery(query *SearchQuery)
}

type HnswSearchOption struct {
	EfSearch uint32
}

func (o *HnswSearchOption) SetQuery(query *SearchQuery) {
	query.Hnsw = o
}

func NewSearchQuery(vector []float32) *SearchQuery {
	return &SearchQuery{
		Vector: vector,
	}
}

func (q *SearchQuery) With(option SearchOption) *SearchQuery {
	option.SetQuery(q)
	return q
}

// WithFilter adds an IdFilter to the search query
func (q *SearchQuery) WithFilter(filter *filter.IdFilter) *SearchQuery {
	q.IdFilter = filter
	return q
}

type InsertParams struct {
	Data       *math.Matrix32
	Labels     []int64
	HnswParams *HnswParams
}

type HnswParams struct {
	Parallel bool
}

func NewInsertParams(data *math.Matrix32, labels []int64) *InsertParams {
	return &InsertParams{
		Data:   data,
		Labels: labels,
	}
}

func (p *InsertParams) With(option InsertOption) *InsertParams {
	option.SetParams(p)
	return p
}

type InsertOption interface {
	SetParams(params *InsertParams)
}

func (o *HnswParams) SetParams(params *InsertParams) {
	params.HnswParams = o
}
