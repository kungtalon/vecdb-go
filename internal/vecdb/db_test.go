package vecdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"vecdb-go/internal/common"
	"vecdb-go/internal/common/math"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestIndexParams creates test database parameters
func createTestIndexParams(metricType common.MetricType, indexType common.IndexType) common.DatabaseParams {
	params := common.DatabaseParams{
		Dim:        3,
		MetricType: metricType,
		IndexType:  indexType,
		Version:    "0.1.0",
	}

	// Add HNSW parameters for HNSW index type
	if indexType == common.IndexTypeHnsw {
		params.HnswParams = &common.HnswIndexOption{
			EFConstruction: 200,
			M:              16,
		}
	}

	return params
}

// testPath manages temporary test directories
type testPath struct {
	dbPath string
}

func newTestPath() *testPath {
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	dbPath := filepath.Join("/tmp/test_db", uuid.New().String(), timestamp)
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		panic(fmt.Sprintf("Failed to create test directory: %v", err))
	}
	return &testPath{dbPath: dbPath}
}

func (tp *testPath) cleanup() {
	if _, err := os.Stat(tp.dbPath); err == nil {
		os.RemoveAll(tp.dbPath)
	}
}

func (tp *testPath) path() string {
	return tp.dbPath
}

// Test cases for different index and metric types
func TestVectorDatabaseNew_FlatL2(t *testing.T) {
	testVectorDatabaseNew(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseNew_HnswL2(t *testing.T) {
	testVectorDatabaseNew(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseNew_FlatIP(t *testing.T) {
	testVectorDatabaseNew(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseNew_HnswIP(t *testing.T) {
	testVectorDatabaseNew(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseNew(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	require.NotNil(t, db)

	assert.Equal(t, params.Dim, db.params.Dim)
	assert.Equal(t, params.MetricType, db.params.MetricType)
	assert.Equal(t, params.IndexType, db.params.IndexType)

	err = db.Close()
	assert.NoError(t, err)
}

func TestVectorDatabaseUpsert_FlatL2(t *testing.T) {
	testVectorDatabaseUpsert(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseUpsert_HnswL2(t *testing.T) {
	testVectorDatabaseUpsert(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseUpsert_FlatIP(t *testing.T) {
	testVectorDatabaseUpsert(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseUpsert_HnswIP(t *testing.T) {
	testVectorDatabaseUpsert(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseUpsert(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	defer db.Close()

	// Create test data
	args := common.VdbUpsertArgs{
		Vectors: math.Matrix32{
			Rows: 2,
			Cols: 3,
			Data: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
		},
		Docs: []map[string]any{
			{"name": "doc1"},
			{"name": "doc2"},
		},
		Attributes: []map[string]any{
			{"category": float64(1)},
			{"category": float64(2)},
		},
	}

	err = db.Upsert(args)
	assert.NoError(t, err)
}

func TestVectorDatabaseQuery_FlatL2(t *testing.T) {
	testVectorDatabaseQuery(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseQuery_HnswL2(t *testing.T) {
	testVectorDatabaseQuery(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseQuery_FlatIP(t *testing.T) {
	testVectorDatabaseQuery(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseQuery_HnswIP(t *testing.T) {
	testVectorDatabaseQuery(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseQuery(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	args := common.VdbUpsertArgs{
		Vectors: math.Matrix32{
			Rows: 3,
			Cols: 3,
			Data: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0},
		},
		Docs: []map[string]any{
			{"name": "doc1"},
			{"name": "doc2"},
			{"name": "doc3"},
		},
		Attributes: []map[string]any{
			{"category": float64(1)},
			{"category": float64(2)},
			{"category": float64(1)},
		},
	}

	err = db.Upsert(args)
	require.NoError(t, err)

	// Query the database
	searchArgs := common.VdbSearchArgs{
		Query: []float32{1.0, 2.0, 3.0},
		K:     2,
	}

	results, err := db.Query(searchArgs)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.LessOrEqual(t, len(results), 2)

	// Verify results contain expected fields
	for _, doc := range results {
		assert.Contains(t, doc, "id")
		assert.Contains(t, doc, "name")
		assert.Contains(t, doc, "attributes")
	}
}

func TestVectorDatabaseUpsertWithWrongDim_FlatL2(t *testing.T) {
	testVectorDatabaseUpsertWithWrongDim(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseUpsertWithWrongDim_HnswL2(t *testing.T) {
	testVectorDatabaseUpsertWithWrongDim(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseUpsertWithWrongDim_FlatIP(t *testing.T) {
	testVectorDatabaseUpsertWithWrongDim(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseUpsertWithWrongDim_HnswIP(t *testing.T) {
	testVectorDatabaseUpsertWithWrongDim(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseUpsertWithWrongDim(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	defer db.Close()

	// Try to upsert data with wrong dimensions
	args := common.VdbUpsertArgs{
		Vectors: math.Matrix32{
			Rows: 1,
			Cols: 2,
			Data: []float32{1.0, 2.0}, // Wrong dimension (2 instead of 3)
		},
		Docs: []map[string]any{
			{"name": "doc1"},
		},
		Attributes: []map[string]any{
			{"category": float64(1)},
		},
	}

	err = db.Upsert(args)
	assert.Error(t, err)
}

func TestVectorDatabaseQueryWithNoResults_FlatL2(t *testing.T) {
	testVectorDatabaseQueryWithNoResults(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseQueryWithNoResults_HnswL2(t *testing.T) {
	testVectorDatabaseQueryWithNoResults(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseQueryWithNoResults_FlatIP(t *testing.T) {
	testVectorDatabaseQueryWithNoResults(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseQueryWithNoResults_HnswIP(t *testing.T) {
	testVectorDatabaseQueryWithNoResults(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseQueryWithNoResults(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	defer db.Close()

	// Query empty database
	searchArgs := common.VdbSearchArgs{
		Query: []float32{1.0, 2.0, 3.0},
		K:     5,
	}

	results, err := db.Query(searchArgs)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestVectorDatabaseQueryWithFilter_FlatL2(t *testing.T) {
	testVectorDatabaseQueryWithFilter(t, common.IndexTypeFlat, common.MetricTypeL2)
}

func TestVectorDatabaseQueryWithFilter_HnswL2(t *testing.T) {
	testVectorDatabaseQueryWithFilter(t, common.IndexTypeHnsw, common.MetricTypeL2)
}

func TestVectorDatabaseQueryWithFilter_FlatIP(t *testing.T) {
	testVectorDatabaseQueryWithFilter(t, common.IndexTypeFlat, common.MetricTypeIP)
}

func TestVectorDatabaseQueryWithFilter_HnswIP(t *testing.T) {
	testVectorDatabaseQueryWithFilter(t, common.IndexTypeHnsw, common.MetricTypeIP)
}

func testVectorDatabaseQueryWithFilter(t *testing.T, indexType common.IndexType, metricType common.MetricType) {
	tp := newTestPath()
	defer tp.cleanup()

	params := createTestIndexParams(metricType, indexType)
	db, err := NewVectorDatabase(tp.path(), params)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data with different categories
	args := common.VdbUpsertArgs{
		Vectors: math.Matrix32{
			Rows: 4,
			Cols: 3,
			Data: []float32{
				1.0, 2.0, 3.0,
				4.0, 5.0, 6.0,
				7.0, 8.0, 9.0,
				10.0, 11.0, 12.0,
			},
		},
		Docs: []map[string]any{
			{"name": "doc1"},
			{"name": "doc2"},
			{"name": "doc3"},
			{"name": "doc4"},
		},
		Attributes: []map[string]any{
			{"category": float64(1), "priority": float64(10)},
			{"category": float64(2), "priority": float64(20)},
			{"category": float64(1), "priority": float64(30)},
			{"category": float64(3), "priority": float64(40)},
		},
	}

	err = db.Upsert(args)
	require.NoError(t, err)

	// Query with filter for category=1
	searchArgs := common.VdbSearchArgs{
		Query: []float32{1.0, 2.0, 3.0},
		K:     10,
		FilterInputs: []common.IntFilterInput{
			{
				Field:  "category",
				Op:     "equal",
				Target: 1,
			},
		},
	}

	results, err := db.Query(searchArgs)
	require.NoError(t, err)
	assert.NotEmpty(t, results)

	// Verify all results have category=1
	for _, doc := range results {
		attrs, ok := doc["attributes"].(map[string]any)
		require.True(t, ok, "attributes should be a map")

		category, ok := attrs["category"]
		require.True(t, ok, "category should exist in attributes")

		// Handle both int64 and float64
		var catValue int64
		switch v := category.(type) {
		case float64:
			catValue = int64(v)
		case int64:
			catValue = v
		case int:
			catValue = int64(v)
		default:
			t.Fatalf("unexpected category type: %T", category)
		}

		assert.Equal(t, int64(1), catValue, "filtered results should have category=1")
	}

	// Query with filter for category != 2
	searchArgs2 := common.VdbSearchArgs{
		Query: []float32{1.0, 2.0, 3.0},
		K:     10,
		FilterInputs: []common.IntFilterInput{
			{
				Field:  "category",
				Op:     "not_equal",
				Target: 2,
			},
		},
	}

	results2, err := db.Query(searchArgs2)
	require.NoError(t, err)
	assert.NotEmpty(t, results2)

	// Verify no results have category=2
	for _, doc := range results2 {
		attrs, ok := doc["attributes"].(map[string]any)
		require.True(t, ok, "attributes should be a map")

		category, ok := attrs["category"]
		require.True(t, ok, "category should exist in attributes")

		// Handle both int64 and float64
		var catValue int64
		switch v := category.(type) {
		case float64:
			catValue = int64(v)
		case int64:
			catValue = v
		case int:
			catValue = int64(v)
		default:
			t.Fatalf("unexpected category type: %T", category)
		}

		assert.NotEqual(t, int64(2), catValue, "filtered results should not have category=2")
	}
}
