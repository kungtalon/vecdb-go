package vecdb

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"vecdb-go/internal/common"
	"vecdb-go/internal/common/math"
	"vecdb-go/internal/filter"
	"vecdb-go/internal/index"
	"vecdb-go/internal/persistence"
	"vecdb-go/internal/scalar"
)

const (
	ScalarDBFileSuffix = "scalar.db"
	IndexFileSuffix    = "index.bin"
	FilterFileSuffix   = "filter.bin"
	WalFileSuffix      = "vdb.log"
)

// VectorDatabase is the main database structure managing scalar data, vector index, and filters
type VectorDatabase struct {
	mu sync.RWMutex

	params        *common.DatabaseParams
	scalarStorage scalar.ScalarStorage
	vectorIndex   index.Index
	filterIndex   *filter.IntFilterIndex
	persistence   *persistence.Persistence

	// Background sync control
	stopSync chan struct{}
	syncDone sync.WaitGroup
}

// NewVectorDatabase creates a new vector database instance
func NewVectorDatabase(params *common.DatabaseParams) (*VectorDatabase, error) {
	// Initialize scalar storage
	scalarDBPath := filepath.Join(params.FilePath, ScalarDBFileSuffix)
	scalarStorage, err := scalar.NewScalarStorage(
		&scalar.ScalarOption{
			DIR:     scalarDBPath,
			Buckets: []string{scalar.NamespaceDocs, scalar.NamespaceWals},
		})
	if err != nil {
		return nil, fmt.Errorf("failed to create scalar storage: %w", err)
	}

	// Initialize vector index
	var hnswParams *index.HNSWParams

	if params.HnswParams != nil {
		hnswParams = &index.HNSWParams{
			EFConstruction: params.HnswParams.EFConstruction,
			M:              params.HnswParams.M,
		}
	}

	vectorIndex, err := index.NewIndex(
		string(params.IndexType),
		params.Dim,
		params.MetricType,
		hnswParams,
	)
	if err != nil {
		scalarStorage.Close()
		return nil, fmt.Errorf("failed to create vector index: %w", err)
	}

	// Initialize filter index
	filterIndex := filter.NewIntFilterIndex()

	// Initialize persistence layer with encoder based on config
	walPath := filepath.Join(params.FilePath, WalFileSuffix)
	encoder := persistence.EncoderFactory(params.EncoderType, persistence.WALVersion)
	slog.Info("Using encoder for persistence", "encoder_type", encoder.Name())

	pers, err := persistence.NewPersistenceWithEncoder(walPath, encoder)
	if err != nil {
		scalarStorage.Close()
		return nil, fmt.Errorf("failed to create persistence layer: %w", err)
	}

	db := &VectorDatabase{
		params:        params,
		scalarStorage: scalarStorage,
		vectorIndex:   vectorIndex,
		filterIndex:   filterIndex,
		persistence:   pers,
		stopSync:      make(chan struct{}),
	}

	// Restore from WAL if exists
	if err := pers.Restore(scalarStorage, filterIndex, vectorIndex, params.Dim); err != nil {
		slog.Warn("Failed to restore from WAL, continuing with empty database", "error", err)
	}

	// Start background sync goroutine
	db.syncDone.Add(1)
	go db.backgroundSync()

	return db, nil
}

// Upsert inserts or updates vectors and their associated documents/attributes
func (db *VectorDatabase) Upsert(args common.VdbUpsertArgs) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Validate input arguments
	if field, got, expected := args.Validate(); field != "" {
		return fmt.Errorf("unexpected length of field %s: %d, expected length is %d", field, got, expected)
	}

	// Validate vector dimensions match database parameters
	if args.Vectors.Cols != db.params.Dim {
		return fmt.Errorf("vector dimension %d does not match database dimension %d", args.Vectors.Cols, db.params.Dim)
	}

	// Generate unique IDs for the new vectors
	ids, err := db.scalarStorage.GenIncrIDs(scalar.NamespaceDocs, args.Vectors.Rows)
	if err != nil {
		return fmt.Errorf("failed to generate IDs: %w", err)
	}

	slog.Info("Upserting vector data", "ids", ids)

	// Process attributes - ensure we have a slice of the right length
	attributes := args.Attributes
	if len(attributes) == 0 {
		attributes = make([]map[string]any, args.Vectors.Rows)
		for i := range attributes {
			attributes[i] = make(map[string]any)
		}
	}

	// Write each record to WAL instead of directly inserting
	for i := 0; i < args.Vectors.Rows; i++ {
		var doc map[string]any

		if i < len(args.Docs) && args.Docs[i] != nil {
			doc = args.Docs[i]
		} else {
			doc = make(map[string]any)
		}

		attr := attributes[i]

		// Extract vector for this row
		vector := make([]float32, args.Vectors.Cols)
		for j := 0; j < args.Vectors.Cols; j++ {
			vector[j] = args.Vectors.At(i, j)
		}

		// Write to WAL with eager=true to maintain backward compatibility with tests
		// This will immediately sync the data to scalar storage, filter index, and vector index
		if err := db.persistence.Write(
			ids[i],
			vector,
			doc,
			attr,
			true, // eager mode for backward compatibility
			db.scalarStorage,
			db.filterIndex,
			db.vectorIndex,
			db.params.Dim,
		); err != nil {
			return fmt.Errorf("failed to write to WAL for id %d: %w", ids[i], err)
		}
	}

	return nil
}

// insertVectors inserts vectors into the vector index
func (db *VectorDatabase) insertVectors(ids []uint64, mat *math.Matrix32) error {
	// Validate vector dimensions match database parameters
	if mat.Cols != db.params.Dim {
		return fmt.Errorf("vector dimension %d does not match database dimension %d", mat.Cols, db.params.Dim)
	}

	// Convert uint64 IDs to int64 labels
	labels := make([]int64, len(ids))
	for i, id := range ids {
		labels[i] = int64(id)
	}

	insertParams := &index.InsertParams{
		Data:   mat,
		Labels: labels,
	}

	if err := db.vectorIndex.Insert(insertParams); err != nil {
		return fmt.Errorf("unable to upsert vector data: %w", err)
	}

	return nil
}

// insertDoc inserts a document into scalar storage
func (db *VectorDatabase) insertDoc(doc common.DocMap, attributes map[string]any, id uint64) error {
	// Add id and attributes to the document
	doc["id"] = id
	doc["attributes"] = attributes

	// Serialize document to JSON
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("unable to serialize doc data: %w", err)
	}

	// Store in scalar storage
	key := scalar.EncodeID(id)
	if err := db.scalarStorage.Put(scalar.NamespaceDocs, key, docBytes); err != nil {
		return fmt.Errorf("unable to upsert scalar data: %w", err)
	}

	return nil
}

// insertAttribute indexes attributes in the filter index
func (db *VectorDatabase) insertAttribute(attr map[string]any, id uint64) error {
	for key, value := range attr {
		switch v := value.(type) {
		case int:
			db.filterIndex.Upsert(key, int64(v), id)
		case int64:
			db.filterIndex.Upsert(key, v, id)
		case float64:
			// JSON numbers are float64, convert to int64 if whole number
			if v == float64(int64(v)) {
				db.filterIndex.Upsert(key, int64(v), id)
			} else {
				return fmt.Errorf("unsupported attribute type for key %s: %v", key, value)
			}
		default:
			return fmt.Errorf("unsupported attribute type for key %s: %T", key, value)
		}
	}

	return nil
}

// revertAttributes removes attributes from the filter index (for rollback)
func (db *VectorDatabase) revertAttributes(attrs []map[string]any, ids []uint64) {
	for i, attr := range attrs {
		if i >= len(ids) {
			break
		}
		for key, value := range attr {
			switch v := value.(type) {
			case int:
				db.filterIndex.Remove(key, int64(v), ids[i])
			case int64:
				db.filterIndex.Remove(key, v, ids[i])
			case float64:
				if v == float64(int64(v)) {
					db.filterIndex.Remove(key, int64(v), ids[i])
				}
			}
		}
	}
}

// Query searches the vector database
func (db *VectorDatabase) Query(searchArgs common.VdbSearchArgs) ([]common.DocMap, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Sync any pending WAL records before querying
	if db.persistence.GetPendingCount() > 0 {
		// Need to upgrade to write lock for sync
		db.mu.RUnlock()
		db.mu.Lock()

		// Check again after acquiring write lock
		if db.persistence.GetPendingCount() > 0 {
			if err := db.persistence.Sync(
				db.scalarStorage,
				db.filterIndex,
				db.vectorIndex,
				db.params.Dim,
			); err != nil {
				db.mu.Unlock()
				db.mu.RLock()
				return nil, fmt.Errorf("failed to sync WAL before query: %w", err)
			}
		}

		db.mu.Unlock()
		db.mu.RLock()
	}

	// Create search query
	query := index.NewSearchQuery(searchArgs.Query)

	// Validate query vector dimension
	if len(query.Vector) != db.params.Dim {
		return nil, fmt.Errorf("query vector length %d does not match index dimension %d",
			len(query.Vector), db.params.Dim)
	}

	// Add HNSW parameters if provided
	if searchArgs.HnswParams != nil {
		hnswOpt := &index.HnswSearchOption{
			EfSearch: searchArgs.HnswParams.EfSearch,
		}
		query = query.With(hnswOpt)
	}

	// Apply filters if provided
	if len(searchArgs.FilterInputs) > 0 {
		bitmap := filter.NewIdFilter().GetBitmap()

		for _, filterInput := range searchArgs.FilterInputs {
			var op filter.FilterOp
			switch filterInput.Op {
			case "equal":
				op = filter.Equal
			case "not_equal":
				op = filter.NotEqual
			default:
				return nil, fmt.Errorf("unsupported filter operation: %s", filterInput.Op)
			}

			input := &filter.IntFilterInput{
				Field:  filterInput.Field,
				Op:     op,
				Target: filterInput.Target,
			}

			bitmap = db.filterIndex.Apply(input, bitmap)
		}

		query = query.WithFilter(filter.NewIdFilterFrom(bitmap))
	}

	// Execute search
	searchResult, err := db.vectorIndex.Search(query, searchArgs.K)
	if err != nil {
		return nil, fmt.Errorf("unable to query vector data: %w", err)
	}

	slog.Debug("Search completed", "result", searchResult)

	if len(searchResult.Labels) == 0 {
		return []common.DocMap{}, nil
	}

	// Convert labels to uint64 IDs, filtering out invalid labels (-1)
	ids := make([]uint64, 0, len(searchResult.Labels))
	for _, label := range searchResult.Labels {
		if label >= 0 {
			ids = append(ids, uint64(label))
		}
	}

	// Retrieve documents from scalar storage
	documents, err := db.scalarStorage.MultiGetValue(scalar.NamespaceDocs, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve documents: %w", err)
	}

	// Convert scalar.DocMap to common.DocMap
	result := make([]common.DocMap, len(documents))
	for i, doc := range documents {
		result[i] = common.DocMap(doc)
	}

	return result, nil
}

// Close closes the database and releases resources
func (db *VectorDatabase) Close() error {
	// Stop background sync goroutine first (without holding lock)
	close(db.stopSync)
	db.syncDone.Wait()

	// Now acquire lock for cleanup
	db.mu.Lock()
	defer db.mu.Unlock()

	// Flush and close persistence layer
	if db.persistence != nil {
		if err := db.persistence.Flush(); err != nil {
			slog.Warn("Failed to flush persistence layer", "error", err)
		}
		if err := db.persistence.Close(); err != nil {
			slog.Warn("Failed to close persistence layer", "error", err)
		}
	}

	if db.scalarStorage != nil {
		return db.scalarStorage.Close()
	}

	return nil
}

// backgroundSync periodically syncs pending WAL records
func (db *VectorDatabase) backgroundSync() {
	defer db.syncDone.Done()

	ticker := time.NewTicker(5 * time.Second) // Sync every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if there are pending records
			if db.persistence.GetPendingCount() > 0 {
				db.mu.Lock()

				// Double-check after acquiring lock
				if db.persistence.GetPendingCount() > 0 {
					if err := db.persistence.Sync(
						db.scalarStorage,
						db.filterIndex,
						db.vectorIndex,
						db.params.Dim,
					); err != nil {
						slog.Error("Background sync failed", "error", err)
					} else {
						slog.Debug("Background sync completed", "records", db.persistence.GetPendingCount())
					}
				}

				db.mu.Unlock()
			}

		case <-db.stopSync:
			// Perform final sync before stopping
			db.mu.Lock()
			if db.persistence.GetPendingCount() > 0 {
				if err := db.persistence.Sync(
					db.scalarStorage,
					db.filterIndex,
					db.vectorIndex,
					db.params.Dim,
				); err != nil {
					slog.Error("Final sync failed", "error", err)
				}
			}
			db.mu.Unlock()
			return
		}
	}
}
