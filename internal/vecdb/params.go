package vecdb

type DatabaseParams struct {
    FilePath string `json:"file_path"`
    // Add other database parameters as needed
}

type VdbSearchArgs struct {
    Query       []float32          `json:"query"`
    FilterInputs map[string]string  `json:"filter_inputs"` // Adjust type as necessary
    K           int                `json:"k"`
    HNSWParams  *HNSWParams        `json:"hnsw_params"` // Define HNSWParams struct as needed
}

type VdbUpsertArgs struct {
    Data   [][]float32 `json:"data"`
    Labels []int64     `json:"labels"`
}

type HNSWParams struct {
    // Define parameters specific to HNSW indexing
}