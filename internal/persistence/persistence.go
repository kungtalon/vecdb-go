package persistence

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"os"
	"sync"
	"sync/atomic"

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

// NewPersistence creates a new persistence layer
func NewPersistence(filePath string) (*Persistence, error) {
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
		record, err := p.readRecord(bufReader)
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
func (p *Persistence) Write(vectorID uint64, vector []float32, doc map[string]any, attributes map[string]any) error {
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

	if err := p.writeRecord(&record); err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}

	// Store in pending logs for sync
	p.pendingLogs = append(p.pendingLogs, record)

	return nil
}

// writeRecord serializes and writes a single WAL record
func (p *Persistence) writeRecord(record *WALRecord) error {
	// Serialize doc and attributes
	docBytes, err := json.Marshal(record.Doc)
	if err != nil {
		return fmt.Errorf("failed to marshal doc: %w", err)
	}

	attrBytes, err := json.Marshal(record.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal attributes: %w", err)
	}

	dim := len(record.Vector)
	vectorBytes := dim * 4

	// Calculate total record size
	// 4 (length) + 8 (logID) + 1 (op) + 8 (vectorID) + 4 (dim) + vectorBytes + 4 (docLen) + docLen + 4 (attrLen) + attrLen + 4 (checksum)
	recordSize := 4 + 8 + 1 + 8 + 4 + vectorBytes + 4 + len(docBytes) + 4 + len(attrBytes) + 4

	// Write record length
	if err := binary.Write(p.bufWriter, binary.BigEndian, uint32(recordSize-4)); err != nil {
		return err
	}

	// Start checksum calculation
	crc := crc32.NewIEEE()
	multiWriter := io.MultiWriter(p.bufWriter, crc)

	// Write log ID
	if err := binary.Write(multiWriter, binary.BigEndian, record.LogID); err != nil {
		return err
	}

	// Write operation
	if err := binary.Write(multiWriter, binary.BigEndian, uint8(record.Operation)); err != nil {
		return err
	}

	// Write vector ID
	if err := binary.Write(multiWriter, binary.BigEndian, record.VectorID); err != nil {
		return err
	}

	// Write dimension
	if err := binary.Write(multiWriter, binary.BigEndian, uint32(dim)); err != nil {
		return err
	}

	// Write vector data
	for _, val := range record.Vector {
		if err := binary.Write(multiWriter, binary.BigEndian, val); err != nil {
			return err
		}
	}

	// Write doc length and data
	if err := binary.Write(multiWriter, binary.BigEndian, uint32(len(docBytes))); err != nil {
		return err
	}
	if _, err := multiWriter.Write(docBytes); err != nil {
		return err
	}

	// Write attributes length and data
	if err := binary.Write(multiWriter, binary.BigEndian, uint32(len(attrBytes))); err != nil {
		return err
	}
	if _, err := multiWriter.Write(attrBytes); err != nil {
		return err
	}

	// Write checksum
	checksum := crc.Sum32()
	if err := binary.Write(p.bufWriter, binary.BigEndian, checksum); err != nil {
		return err
	}

	return nil
}

// readRecord reads a single WAL record from reader
func (p *Persistence) readRecord(reader *bufio.Reader) (*WALRecord, error) {
	// Read record length
	var recordLen uint32
	if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
		return nil, err
	}

	// Read entire record into buffer for checksum verification
	recordData := make([]byte, recordLen)
	if _, err := io.ReadFull(reader, recordData); err != nil {
		return nil, fmt.Errorf("failed to read record data: %w", err)
	}

	// Extract checksum (last 4 bytes)
	if len(recordData) < 4 {
		return nil, fmt.Errorf("record too short")
	}

	checksumBytes := recordData[len(recordData)-4:]
	dataBytes := recordData[:len(recordData)-4]

	expectedChecksum := binary.BigEndian.Uint32(checksumBytes)
	actualChecksum := crc32.ChecksumIEEE(dataBytes)

	if expectedChecksum != actualChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}

	// Parse record data
	record := &WALRecord{Version: p.version}
	offset := 0

	// Read log ID
	record.LogID = binary.BigEndian.Uint64(dataBytes[offset : offset+8])
	offset += 8

	// Read operation
	record.Operation = WALOperation(dataBytes[offset])
	offset += 1

	// Read vector ID
	record.VectorID = binary.BigEndian.Uint64(dataBytes[offset : offset+8])
	offset += 8

	// Read dimension
	dim := binary.BigEndian.Uint32(dataBytes[offset : offset+4])
	offset += 4

	// Read vector data
	record.Vector = make([]float32, dim)
	for i := uint32(0); i < dim; i++ {
		bits := binary.BigEndian.Uint32(dataBytes[offset : offset+4])
		record.Vector[i] = math.Float32frombits(bits)
		offset += 4
	}

	// Read doc length and data
	docLen := binary.BigEndian.Uint32(dataBytes[offset : offset+4])
	offset += 4
	docBytes := dataBytes[offset : offset+int(docLen)]
	offset += int(docLen)

	if err := json.Unmarshal(docBytes, &record.Doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal doc: %w", err)
	}

	// Read attributes length and data
	attrLen := binary.BigEndian.Uint32(dataBytes[offset : offset+4])
	offset += 4
	attrBytes := dataBytes[offset : offset+int(attrLen)]

	if err := json.Unmarshal(attrBytes, &record.Attributes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attributes: %w", err)
	}

	return record, nil
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
			var intValue int64
			switch v := value.(type) {
			case int:
				intValue = int64(v)
			case int64:
				intValue = v
			case float64:
				if v == float64(int64(v)) {
					intValue = int64(v)
				}
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
		record, err := p.readRecord(bufReader)
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

	// Unlock before calling Sync to avoid deadlock
	p.mu.Unlock()

	// Apply all records using Sync
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
