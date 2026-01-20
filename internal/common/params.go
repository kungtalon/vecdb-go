package common

import "vecdb-go/internal/common/math"

// IndexType represents the type of vector index
type IndexType string

const (
	IndexTypeFlat IndexType = "flat"
	IndexTypeHnsw IndexType = "hnsw"
)

// MetricType represents the distance metric type
type MetricType string

const (
	MetricTypeL2 MetricType = "l2"
	MetricTypeIP MetricType = "ip"
)

// DatabaseParams contains parameters for database initialization
type DatabaseParams struct {
	FilePath    string           `json:"file_path" toml:"file_path"`
	Dim         int              `json:"dim" toml:"dim"`
	MetricType  MetricType       `json:"metric_type" toml:"metric_type"`
	IndexType   IndexType        `json:"index_type" toml:"index_type"`
	EncoderType string           `json:"encoder_type,omitempty" toml:"encoder_type,omitempty"` // "binary" or "text"
	HnswParams  *HnswIndexOption `json:"hnsw_params,omitempty" toml:"hnsw_params,omitempty"`
	Version     string           `json:"version" toml:"version"`
}

// HnswIndexOption contains HNSW index creation parameters
type HnswIndexOption struct {
	EFConstruction int `json:"ef_construction" toml:"ef_construction"`
	M              int `json:"m" toml:"m"`
}

// HnswParams contains HNSW insertion parameters
type HnswParams struct {
	EFConstruction int `json:"ef_construction"`
}

// HnswSearchOption contains HNSW search parameters
type HnswSearchOption struct {
	EfSearch uint32 `json:"ef_search"`
}

// DocMap represents a document with arbitrary key-value pairs
type DocMap map[string]any

// VdbUpsertArgs contains arguments for upserting data into the vector database
type VdbUpsertArgs struct {
	Vectors    math.Matrix32    `json:"vectors"`
	Docs       []map[string]any `json:"docs"`
	Attributes []map[string]any `json:"attributes"`
	HnswParams *HnswParams      `json:"hnsw_params,omitempty"`
}

// IntFilterInput defines an integer field filter
type IntFilterInput struct {
	Field  string `json:"field"`
	Op     string `json:"op"` // "equal" or "not_equal"
	Target int64  `json:"target"`
}

// VdbSearchArgs contains arguments for searching the vector database
type VdbSearchArgs struct {
	Query        []float32         `json:"query"`
	K            int               `json:"k"`
	FilterInputs []IntFilterInput  `json:"filter_inputs,omitempty"`
	HnswParams   *HnswSearchOption `json:"hnsw_params,omitempty"`
}

// Validate checks if VdbUpsertArgs has consistent dimensions
func (args *VdbUpsertArgs) Validate() (string, int, int) {
	if len(args.Docs) != args.Vectors.Rows {
		return "docs", len(args.Docs), args.Vectors.Rows
	}

	if len(args.Attributes) > 0 && len(args.Attributes) != args.Vectors.Rows {
		return "attributes", len(args.Attributes), args.Vectors.Rows
	}

	return "", 0, 0
}
