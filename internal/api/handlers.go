package api

import (
	"net/http"
	"sync"

	"vecdb-go/internal/common"
	"vecdb-go/internal/vecdb"

	"github.com/gin-gonic/gin"
)

type VectorSearchRequest struct {
	Query        []float32               `json:"query"`
	FilterInputs []common.IntFilterInput `json:"filter_inputs,omitempty"`
	K            int                     `json:"k"`
}

type VectorUpsertRequest struct {
	Data   [][]float32 `json:"data"`
	Labels []int64     `json:"labels"`
}

type VectorSearchResponse struct {
	Results []common.DocMap `json:"results"`
}

type VectorUpsertResponse struct {
	Message string `json:"message"`
}

var vdb *vecdb.VectorDatabase
var mu sync.Mutex

func Initialize(db *vecdb.VectorDatabase) {
	vdb = db
}

func HandleVectorSearch(c *gin.Context) {
	var payload VectorSearchRequest

	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	searchArgs := common.VdbSearchArgs{
		Query:        payload.Query,
		K:            payload.K,
		FilterInputs: payload.FilterInputs,
	}

	mu.Lock()
	results, err := vdb.Query(searchArgs)
	mu.Unlock()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, VectorSearchResponse{Results: results})
}

func HandleVectorUpsert(c *gin.Context) {
	var payload VectorUpsertRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Convert 2D array to flat array
	var flatData []float32
	var dataRow int
	var dataDim int
	if len(payload.Data) > 0 {
		dataRow = len(payload.Data)
		dataDim = len(payload.Data[0])
		flatData = make([]float32, 0, dataRow*dataDim)
		for _, row := range payload.Data {
			flatData = append(flatData, row...)
		}
	}

	upsertArgs := common.VdbUpsertArgs{
		Vectors: common.VectorArgs{
			FlatData: flatData,
			DataRow:  dataRow,
			DataDim:  dataDim,
		},
		Docs:       make([]map[string]any, dataRow),
		Attributes: make([]map[string]any, dataRow),
	}

	mu.Lock()
	err := vdb.Upsert(upsertArgs)
	mu.Unlock()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, VectorUpsertResponse{Message: "Upsert successful"})
}
