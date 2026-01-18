package vecdb

type DatabaseParams struct {
    // Define fields for database parameters
    FilePath string `json:"file_path"`
    // Add other necessary fields
}

type DocMap struct {
    // Define fields for document representation
    ID    int64       `json:"id"`
    Data  interface{} `json:"data"` // Adjust type as necessary
}

type VdbSearchArgs struct {
    Query        []float32 `json:"query"`
    FilterInputs map[string]interface{} `json:"filter_inputs"` // Adjust type as necessary
    K            int       `json:"k"`
    HNSWParams   interface{} `json:"hnsw_params"` // Define a proper type if needed
}

type VdbUpsertArgs struct {
    ID    int64       `json:"id"`
    Data  interface{} `json:"data"` // Adjust type as necessary
}