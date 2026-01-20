package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"vecdb-go/internal/common"
	commonMath "vecdb-go/internal/common/math"
	"vecdb-go/internal/filter"
	"vecdb-go/internal/index"
	"vecdb-go/internal/scalar"
)

// WAL record format:
// [4 bytes: record length]
// [8 bytes: log ID]
// [1 byte: operation type]
// [8 bytes: vector ID]
// [4 bytes: dimension]
// [dimension * 4 bytes: vector data]
// [4 bytes: doc length]
// [doc length bytes: doc JSON]
// [4 bytes: attributes length]
// [attributes length bytes: attributes JSON]
// [4 bytes: CRC32 checksum]

const (
	WALVersion = "v1"
)

type Persistence struct {
	filePath    string
	walWriter   *os.File
	mu          sync.Mutex
	version     string
	counter     atomic.Uint64
	bufWriter   *bufio.Writer
	pendingLogs []WALRecord
	encoder     WALEncoder
}

type WALOperation int

const (
	Insert WALOperation = iota
	Delete
)

type WALRecord struct {
	LogID      uint64
	Version    string
	Operation  WALOperation
	VectorID   uint64
	Vector     []float32
	Doc        map[string]any
	Attributes map[string]any
}

// WALRecordData contains the data components needed to apply a WAL record
type WALRecordData struct {
	VectorID   uint64
	Vector     []float32
	Doc        map[string]any
	Attributes map[string]any
}

// NewPersistence creates a new persistence layer with binary encoding
func NewPersistence(filePath string) (*Persistence, error) {
	return NewPersistenceWithEncoder(filePath, NewBinaryWALEncoder(WALVersion))
}

// NewPersistenceWithEncoder creates a new persistence layer with custom encoder
func NewPersistenceWithEncoder(filePath string, encoder WALEncoder) (*Persistence, error) {
	// Open WAL file in append mode, create if not exists
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	p := &Persistence{
		filePath:    filePath,
		walWriter:   file,
		version:     WALVersion,
		bufWriter:   bufio.NewWriter(file),
		pendingLogs: make([]WALRecord, 0, 100),
		encoder:     encoder,
	}

	// Initialize counter from existing WAL if any
	if err := p.initCounter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to initialize counter: %w", err)
	}

	return p, nil
}

// initCounter initializes the counter from existing WAL records
func (p *Persistence) initCounter() error {
	// Get file size
	stat, err := p.walWriter.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}

	if stat.Size() == 0 {
		// Empty file, start from 0
		p.counter.Store(0)
		return nil
	}

	// Read existing records to find max log ID
	reader, err := os.Open(p.filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer reader.Close()

	var maxLogID uint64 = 0
	bufReader := bufio.NewReader(reader)

	for {
		record, err := p.encoder.DecodeRecord(bufReader)
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("Error reading WAL record during init, may be corrupted", "error", err)
			break
		}
		if record.LogID > maxLogID {
			maxLogID = record.LogID
		}
	}

	p.counter.Store(maxLogID)
	slog.Info("Initialized WAL counter", "maxLogID", maxLogID)
	return nil
}

// Write writes a new record to WAL
// If eager is true, Sync is called immediately after writing
func (p *Persistence) Write(
	vectorID uint64,
	vector []float32,
	doc map[string]any,
	attributes map[string]any,
	eager bool,
	scalarStorage scalar.ScalarStorage,
	filterIndex *filter.IntFilterIndex,
	vectorIndex index.Index,
	dim int,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	logID := p.counter.Add(1)

	record := WALRecord{
		LogID:      logID,
		Version:    p.version,
		Operation:  Insert,
		VectorID:   vectorID,
		Vector:     vector,
		Doc:        doc,
		Attributes: attributes,
	}

	if err := p.encoder.EncodeRecord(p.bufWriter, &record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// Store in pending logs for sync
	p.pendingLogs = append(p.pendingLogs, record)

	// If eager mode, sync immediately
	if eager {
		return p.syncLocked(scalarStorage, filterIndex, vectorIndex, dim)
	}

	return nil
}

// WriteOnly writes a record to WAL without syncing (for testing)
func (p *Persistence) WriteOnly(vectorID uint64, vector []float32, doc map[string]any, attributes map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	logID := p.counter.Add(1)

	record := WALRecord{
		LogID:      logID,
		Version:    p.version,
		Operation:  Insert,
		VectorID:   vectorID,
		Vector:     vector,
		Doc:        doc,
		Attributes: attributes,
	}

	if err := p.encoder.EncodeRecord(p.bufWriter, &record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// Store in pending logs for sync
	p.pendingLogs = append(p.pendingLogs, record)

	return nil
}

// Flush flushes buffered WAL data to disk
func (p *Persistence) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := p.walWriter.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	return nil
}

// Close closes the persistence layer
func (p *Persistence) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.bufWriter != nil {
		if err := p.bufWriter.Flush(); err != nil {
			return err
		}
	}

	if p.walWriter != nil {
		return p.walWriter.Close()
	}

	return nil
}

// Sync applies all pending WAL records to the database components
// Order: Scalar storage -> Filter index -> Vector index (vector last for easier rollback)
func (p *Persistence) Sync(
	scalarStorage scalar.ScalarStorage,
	filterIndex *filter.IntFilterIndex,
	vectorIndex index.Index,
	dim int,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.syncLocked(scalarStorage, filterIndex, vectorIndex, dim)
}

// syncLocked applies all pending WAL records (caller must hold lock)
func (p *Persistence) syncLocked(
	scalarStorage scalar.ScalarStorage,
	filterIndex *filter.IntFilterIndex,
	vectorIndex index.Index,
	dim int,
) error {
	if len(p.pendingLogs) == 0 {
		return nil
	}

	slog.Info("Syncing WAL records", "count", len(p.pendingLogs))

	// First flush WAL to disk for durability
	if err := p.bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	if err := p.walWriter.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}

	// Track successfully applied records for rollback
	appliedScalar := make([]uint64, 0, len(p.pendingLogs))
	appliedFilter := make([]WALRecordData, 0, len(p.pendingLogs))

	// Phase 1: Apply to scalar storage
	for _, record := range p.pendingLogs {
		if record.Operation == Insert {
			doc := make(map[string]any)
			for k, v := range record.Doc {
				doc[k] = v
			}
			doc["id"] = record.VectorID
			doc["attributes"] = record.Attributes

			docBytes, err := json.Marshal(doc)
			if err != nil {
				// Rollback scalar storage
				p.rollbackScalar(scalarStorage, appliedScalar)
				return fmt.Errorf("failed to marshal doc for vector %d: %w", record.VectorID, err)
			}

			key := scalar.EncodeID(record.VectorID)
			if err := scalarStorage.Put(scalar.NamespaceDocs, key, docBytes); err != nil {
				// Rollback scalar storage
				p.rollbackScalar(scalarStorage, appliedScalar)
				return fmt.Errorf("failed to insert scalar data for vector %d: %w", record.VectorID, err)
			}

			appliedScalar = append(appliedScalar, record.VectorID)
		}
	}

	// Phase 2: Apply to filter index
	for _, record := range p.pendingLogs {
		if record.Operation == Insert && len(record.Attributes) > 0 {
			for key, value := range record.Attributes {
				var intValue int64
				switch v := value.(type) {
				case int:
					intValue = int64(v)
				case int64:
					intValue = v
				case float64:
					if v == float64(int64(v)) {
						intValue = int64(v)
					} else {
						// Rollback
						p.rollbackScalar(scalarStorage, appliedScalar)
						p.rollbackFilter(filterIndex, appliedFilter)
						return fmt.Errorf("unsupported attribute type for key %s: %v", key, value)
					}
				default:
					// Rollback
					p.rollbackScalar(scalarStorage, appliedScalar)
					p.rollbackFilter(filterIndex, appliedFilter)
					return fmt.Errorf("unsupported attribute type for key %s: %T", key, value)
				}

				filterIndex.Upsert(key, intValue, record.VectorID)
			}

			appliedFilter = append(appliedFilter, WALRecordData{
				VectorID:   record.VectorID,
				Attributes: record.Attributes,
			})
		}
	}

	// Phase 3: Apply to vector index (last operation)
	// Prepare batch data for vector insertion
	vectorIDs := make([]uint64, 0, len(p.pendingLogs))
	vectors := make([][]float32, 0, len(p.pendingLogs))

	for _, record := range p.pendingLogs {
		if record.Operation == Insert {
			vectorIDs = append(vectorIDs, record.VectorID)
			vectors = append(vectors, record.Vector)
		}
	}

	if len(vectors) > 0 {
		// Create matrix from vectors
		mat := commonMath.NewMatrix32Empty(len(vectors), dim)
		for i, vec := range vectors {
			for j, val := range vec {
				mat.Set(i, j, val)
			}
		}

		// Convert uint64 IDs to int64 labels for FAISS
		labels := make([]int64, len(vectorIDs))
		for i, id := range vectorIDs {
			labels[i] = int64(id)
		}

		insertParams := &index.InsertParams{
			Data:   mat,
			Labels: labels,
		}

		if err := vectorIndex.Insert(insertParams); err != nil {
			// Rollback all changes
			p.rollbackScalar(scalarStorage, appliedScalar)
			p.rollbackFilter(filterIndex, appliedFilter)
			// Note: Vector index cannot be easily rolled back, but since it's last,
			// we haven't inserted anything yet
			return fmt.Errorf("failed to insert vectors: %w", err)
		}
	}

	// Clear pending logs after successful sync
	p.pendingLogs = make([]WALRecord, 0, 100)

	slog.Info("Successfully synced WAL records")
	return nil
}

// rollbackScalar removes scalar storage entries
func (p *Persistence) rollbackScalar(scalarStorage scalar.ScalarStorage, ids []uint64) {
	slog.Warn("Rolling back scalar storage changes", "count", len(ids))
	for _, id := range ids {
		key := scalar.EncodeID(id)
		// Best effort deletion, ignore errors
		_ = scalarStorage.Put(scalar.NamespaceDocs, key, nil)
	}
}

// rollbackFilter removes filter index entries
func (p *Persistence) rollbackFilter(filterIndex *filter.IntFilterIndex, records []WALRecordData) {
	slog.Warn("Rolling back filter index changes", "count", len(records))
	for _, record := range records {
		for key, value := range record.Attributes {
			intValue, ok := common.ToInt64(value)
			if !ok {
				continue
			}

			filterIndex.Remove(key, intValue, record.VectorID)
		}
	}
}

// Restore loads and replays all WAL records from disk
func (p *Persistence) Restore(
	scalarStorage scalar.ScalarStorage,
	filterIndex *filter.IntFilterIndex,
	vectorIndex index.Index,
	dim int,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Info("Restoring from WAL", "file", p.filePath)

	// Check if WAL file exists and has content
	stat, err := p.walWriter.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat WAL file: %w", err)
	}

	if stat.Size() == 0 {
		slog.Info("WAL file is empty, nothing to restore")
		return nil
	}

	// Open file for reading
	reader, err := os.Open(p.filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer reader.Close()

	bufReader := bufio.NewReader(reader)
	records := make([]WALRecord, 0)
	recordCount := 0
	corruptedCount := 0

	// Read all records with checksum verification
	for {
		record, err := p.encoder.DecodeRecord(bufReader)
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("Skipping corrupted WAL record", "error", err, "position", recordCount)
			corruptedCount++
			// Stop at first corruption to avoid cascading issues
			break
		}

		records = append(records, *record)
		recordCount++
	}

	slog.Info("Read WAL records", "total", recordCount, "corrupted", corruptedCount)

	if len(records) == 0 {
		return nil
	}

	// Store records as pending logs
	p.pendingLogs = records

	// Unlock before calling Sync (which will re-acquire the lock)
	p.mu.Unlock()

	// Apply all records using Sync (not syncLocked, as we don't hold the lock)
	err = p.Sync(scalarStorage, filterIndex, vectorIndex, dim)

	// Re-lock before returning
	p.mu.Lock()

	if err != nil {
		return fmt.Errorf("failed to apply WAL records during restore: %w", err)
	}

	slog.Info("Successfully restored from WAL", "records", recordCount)

	// Truncate WAL file after successful restore
	if err := p.truncateWAL(); err != nil {
		slog.Warn("Failed to truncate WAL after restore", "error", err)
	}

	return nil
}

// truncateWAL truncates the WAL file after successful restore/sync
func (p *Persistence) truncateWAL() error {
	// Close current writer
	if err := p.bufWriter.Flush(); err != nil {
		return err
	}
	if err := p.walWriter.Close(); err != nil {
		return err
	}

	// Truncate file
	if err := os.Truncate(p.filePath, 0); err != nil {
		return err
	}

	// Reopen file
	file, err := os.OpenFile(p.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	p.walWriter = file
	p.bufWriter = bufio.NewWriter(file)

	return nil
}

// GetPendingCount returns the number of pending WAL records
func (p *Persistence) GetPendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pendingLogs)
}
