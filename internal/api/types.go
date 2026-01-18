package api

type VdbSearchArgs struct {
    Query        string              `json:"query"`
    FilterInputs map[string][]string `json:"filter_inputs,omitempty"`
    K            int                 `json:"k"`
    HnswParams   *HnswParams         `json:"hnsw_params,omitempty"`
}

type HnswParams struct {
    M              int `json:"m"`
    EfConstruction int `json:"ef_construction"`
    EfSearch       int `json:"ef_search"`
}

type VdbUpsertArgs struct {
    Vectors [][]float32 `json:"vectors"`
    Labels  []int64     `json:"labels"`
}

type DocMap struct {
    ID      int64       `json:"id"`
    Vector  []float32   `json:"vector"`
    Metadata interface{} `json:"metadata,omitempty"`
}