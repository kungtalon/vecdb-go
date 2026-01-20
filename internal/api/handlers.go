package api

import (
	"log/slog"
	"net/http"

	"vecdb-go/internal/common"
	"vecdb-go/internal/common/math"
	"vecdb-go/internal/vecdb"

	"github.com/gin-gonic/gin"
)

type VectorSearchRequest struct {
	Query        []float32               `json:"query"`
	FilterInputs []common.IntFilterInput `json:"filter_inputs,omitempty"`
	K            int                     `json:"k"`
}

type VectorUpsertRequest struct {
	Data       math.Matrix32      `json:"data"`
	Docs       []map[string]any   `json:"docs,omitempty"`
	Attributes []map[string]any   `json:"attributes,omitempty"`
	HnswParams *common.HnswParams `json:"hnsw_params,omitempty"`
}

type VectorSearchResponse struct {
	Results []common.DocMap `json:"results"`
}

type VectorUpsertResponse struct {
	Message string `json:"message"`
}

var vdb *vecdb.VectorDatabase

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

	results, err := vdb.Query(searchArgs)
	if err != nil {
		slog.Error("failed to search", "error", err)
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

	upsertArgs := common.VdbUpsertArgs{
		Vectors:    payload.Data,
		Docs:       payload.Docs,
		Attributes: payload.Attributes,
		HnswParams: payload.HnswParams,
	}

	err := vdb.Upsert(upsertArgs)
	if err != nil {
		slog.Error("failed to upsert", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, VectorUpsertResponse{Message: "Upsert successful"})
}
