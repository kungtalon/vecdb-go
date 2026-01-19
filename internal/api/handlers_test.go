package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorUpsertRequest_Unmarshal(t *testing.T) {
	tests := []struct {
		name           string
		jsonData       string
		wantRows       int
		wantCols       int
		wantData       []float32
		wantDocsCount  int
		wantAttrsCount int
		expectError    bool
	}{
		{
			name: "valid 2x3 matrix with docs and attributes",
			jsonData: `{
				"data": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
				"docs": [{"name": "doc1"}, {"name": "doc2"}],
				"attributes": [{"category": 1}, {"category": 2}]
			}`,
			wantRows:       2,
			wantCols:       3,
			wantData:       []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
			wantDocsCount:  2,
			wantAttrsCount: 2,
		},
		{
			name: "valid matrix without docs and attributes",
			jsonData: `{
				"data": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
			}`,
			wantRows:       2,
			wantCols:       3,
			wantData:       []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
			wantDocsCount:  0,
			wantAttrsCount: 0,
		},
		{
			name: "valid 3x2 matrix",
			jsonData: `{
				"data": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
				"docs": [{"id": 1}, {"id": 2}, {"id": 3}],
				"attributes": [{"type": 1}, {"type": 2}, {"type": 1}]
			}`,
			wantRows:       3,
			wantCols:       2,
			wantData:       []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
			wantDocsCount:  3,
			wantAttrsCount: 3,
		},
		{
			name: "single row matrix",
			jsonData: `{
				"data": [[7.0, 8.0, 9.0]],
				"docs": [{"single": true}],
				"attributes": [{"priority": 10}]
			}`,
			wantRows:       1,
			wantCols:       3,
			wantData:       []float32{7.0, 8.0, 9.0},
			wantDocsCount:  1,
			wantAttrsCount: 1,
		},
		{
			name: "empty matrix",
			jsonData: `{
				"data": [],
				"docs": [],
				"attributes": []
			}`,
			wantRows:       0,
			wantCols:       0,
			wantData:       []float32{},
			wantDocsCount:  0,
			wantAttrsCount: 0,
		},
		{
			name: "inconsistent row lengths",
			jsonData: `{
				"data": [[1.0, 2.0], [3.0, 4.0, 5.0]],
				"docs": [{"a": 1}, {"b": 2}]
			}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req VectorUpsertRequest
			err := json.Unmarshal([]byte(tt.jsonData), &req)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRows, req.Data.Rows, "Rows mismatch")
			assert.Equal(t, tt.wantCols, req.Data.Cols, "Cols mismatch")
			assert.Equal(t, tt.wantData, req.Data.Data, "Data mismatch")
			assert.Equal(t, tt.wantDocsCount, len(req.Docs), "Docs count mismatch")
			assert.Equal(t, tt.wantAttrsCount, len(req.Attributes), "Attributes count mismatch")

			// Verify RawData() returns the same as Data
			assert.Equal(t, tt.wantData, req.Data.RawData(), "RawData() mismatch")

			// Verify Dims() returns correct dimensions
			rows, cols := req.Data.Dims()
			assert.Equal(t, tt.wantRows, rows, "Dims() rows mismatch")
			assert.Equal(t, tt.wantCols, cols, "Dims() cols mismatch")

			// Verify docs and attributes content if provided
			if tt.wantDocsCount > 0 {
				assert.NotNil(t, req.Docs, "Docs should not be nil")
			}
			if tt.wantAttrsCount > 0 {
				assert.NotNil(t, req.Attributes, "Attributes should not be nil")
			}
		})
	}
}

func TestVectorSearchRequest_Unmarshal(t *testing.T) {
	tests := []struct {
		name             string
		jsonData         string
		wantQuery        []float32
		wantK            int
		wantFilterInputs int
		expectError      bool
	}{
		{
			name: "basic search request",
			jsonData: `{
				"query": [1.0, 2.0, 3.0],
				"k": 5
			}`,
			wantQuery:        []float32{1.0, 2.0, 3.0},
			wantK:            5,
			wantFilterInputs: 0,
		},
		{
			name: "search with filters",
			jsonData: `{
				"query": [1.0, 2.0, 3.0],
				"k": 10,
				"filter_inputs": [
					{"field": "category", "op": "equal", "target": 1}
				]
			}`,
			wantQuery:        []float32{1.0, 2.0, 3.0},
			wantK:            10,
			wantFilterInputs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req VectorSearchRequest
			err := json.Unmarshal([]byte(tt.jsonData), &req)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantQuery, req.Query, "Query mismatch")
			assert.Equal(t, tt.wantK, req.K, "K mismatch")
			assert.Equal(t, tt.wantFilterInputs, len(req.FilterInputs), "FilterInputs count mismatch")
		})
	}
}
