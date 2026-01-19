package common

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
	Dim        int              `json:"dim"`
	MetricType MetricType       `json:"metric_type"`
	IndexType  IndexType        `json:"index_type"`
	HnswParams *HnswIndexOption `json:"hnsw_params,omitempty"`
	Version    string           `json:"version"`
}

// HnswIndexOption contains HNSW index creation parameters
type HnswIndexOption struct {
	EFConstruction int `json:"ef_construction"`
	M              int `json:"m"`
}

// HnswParams contains HNSW insertion parameters
type HnswParams struct {
	EFConstruction int `json:"ef_construction"`
}

// HnswSearchOption contains HNSW search parameters
type HnswSearchOption struct {
	EfSearch uint32 `json:"ef_search"`
}

// VectorArgs contains vector data in flat format
type VectorArgs struct {
	FlatData []float32 `json:"flat_data"`
	DataRow  int       `json:"data_row"`
	DataDim  int       `json:"data_dim"`
}

// DocMap represents a document with arbitrary key-value pairs
type DocMap map[string]any

// VdbUpsertArgs contains arguments for upserting data into the vector database
type VdbUpsertArgs struct {
	Vectors    VectorArgs       `json:"vectors"`
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
	if len(args.Docs) != args.Vectors.DataRow {
		return "docs", len(args.Docs), args.Vectors.DataRow
	}

	if len(args.Attributes) > 0 && len(args.Attributes) != args.Vectors.DataRow {
		return "attributes", len(args.Attributes), args.Vectors.DataRow
	}

	if args.Vectors.DataDim*args.Vectors.DataRow != len(args.Vectors.FlatData) {
		return "flat_data", len(args.Vectors.FlatData), args.Vectors.DataDim * args.Vectors.DataRow
	}

	return "", 0, 0
}
