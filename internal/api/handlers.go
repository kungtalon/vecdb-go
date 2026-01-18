package api

import (
	"net/http"

	"sync"

	"github.com/gin-gonic/gin"
)

type VectorSearchRequest struct {
	Query        []float32      `json:"query"`
	FilterInputs map[int64]bool `json:"filter_inputs,omitempty"`
	K            int            `json:"k"`
}

type VectorUpsertRequest struct {
	Data   [][]float32 `json:"data"`
	Labels []int64     `json:"labels"`
}

type VectorSearchResponse struct {
	Results []DocMap `json:"results"`
}

type VectorUpsertResponse struct {
	Message string `json:"message"`
}

type DocMap struct {
	ID int64 `json:"id"`
}

var vdb *VectorDatabase
var mu sync.Mutex

func Initialize(db *VectorDatabase) {
	vdb = db
}

func HandleVectorSearch(c *gin.Context) {
	var payload VectorSearchRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	mu.Lock()
	results, err := vdb.Query(payload.Query, payload.FilterInputs, payload.K)
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

	mu.Lock()
	err := vdb.Upsert(payload.Data, payload.Labels)
	mu.Unlock()

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, VectorUpsertResponse{Message: "Upsert successful"})
}
