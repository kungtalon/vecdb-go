package persistence

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strings"
)

// WALEncoder defines the interface for encoding and decoding WAL records
type WALEncoder interface {
	// EncodeRecord writes a WAL record to the writer
	EncodeRecord(writer io.Writer, record *WALRecord) error

	// DecodeRecord reads a WAL record from the reader
	DecodeRecord(reader *bufio.Reader) (*WALRecord, error)

	// Name returns the encoder name for identification
	Name() string
}

func EncoderFactory(encoderType, version string) WALEncoder {
	var encoder WALEncoder

	if encoderType == "text" {
		encoder = NewTextWALEncoder(version)
	} else {
		// Default to binary encoder
		encoder = NewBinaryWALEncoder(version)
	}

	return encoder
}

// BinaryWALEncoder implements binary encoding with CRC32 checksum
type BinaryWALEncoder struct {
	version string
}

// NewBinaryWALEncoder creates a new binary WAL encoder
func NewBinaryWALEncoder(version string) *BinaryWALEncoder {
	return &BinaryWALEncoder{version: version}
}

func (e *BinaryWALEncoder) Name() string {
	return "binary"
}

func (e *BinaryWALEncoder) EncodeRecord(writer io.Writer, record *WALRecord) error {
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
	recordSize := 4 + 8 + 1 + 8 + 4 + vectorBytes + 4 + len(docBytes) + 4 + len(attrBytes) + 4

	// Write record length
	if err := binary.Write(writer, binary.BigEndian, uint32(recordSize-4)); err != nil {
		return err
	}

	// Start checksum calculation
	crc := crc32.NewIEEE()
	multiWriter := io.MultiWriter(writer, crc)

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
	if err := binary.Write(writer, binary.BigEndian, checksum); err != nil {
		return err
	}

	return nil
}

func (e *BinaryWALEncoder) DecodeRecord(reader *bufio.Reader) (*WALRecord, error) {
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
	record := &WALRecord{Version: e.version}
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

// TextWALEncoder implements human-readable text encoding for debugging
type TextWALEncoder struct {
	version string
}

// NewTextWALEncoder creates a new text WAL encoder
func NewTextWALEncoder(version string) *TextWALEncoder {
	return &TextWALEncoder{version: version}
}

func (e *TextWALEncoder) Name() string {
	return "text"
}

func (e *TextWALEncoder) EncodeRecord(writer io.Writer, record *WALRecord) error {
	// Convert vector to base64
	vectorBuf := new(bytes.Buffer)
	for _, val := range record.Vector {
		if err := binary.Write(vectorBuf, binary.BigEndian, val); err != nil {
			return fmt.Errorf("failed to encode vector: %w", err)
		}
	}
	vectorBase64 := base64.StdEncoding.EncodeToString(vectorBuf.Bytes())

	// Marshal doc and attributes as compressed JSON (no indentation)
	docJSON, err := json.Marshal(record.Doc)
	if err != nil {
		return fmt.Errorf("failed to marshal doc: %w", err)
	}

	attrJSON, err := json.Marshal(record.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal attributes: %w", err)
	}

	// Escape any quotes or newlines in JSON strings for CSV compatibility
	docStr := strings.ReplaceAll(string(docJSON), "\"", "\\\"")
	docStr = strings.ReplaceAll(docStr, "\n", "\\n")
	attrStr := strings.ReplaceAll(string(attrJSON), "\"", "\\\"")
	attrStr = strings.ReplaceAll(attrStr, "\n", "\\n")

	// Write CSV-like format: log_id,version,operation,vector_id,vector_base64,doc_json,attributes_json
	_, err = fmt.Fprintf(writer, "%d,%s,%s,%d,%s,\"%s\",\"%s\"\n",
		record.LogID,
		record.Version,
		record.Operation.String(),
		record.VectorID,
		vectorBase64,
		docStr,
		attrStr,
	)
	return err
}

func (e *TextWALEncoder) DecodeRecord(reader *bufio.Reader) (*WALRecord, error) {
	// Read one line (CSV format)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return nil, io.EOF
	}

	// Parse CSV-like format: log_id,version,operation,vector_id,vector_base64,"doc_json","attributes_json"
	// We need to handle quoted fields properly
	parts := parseCSVLine(line)
	if len(parts) != 7 {
		return nil, fmt.Errorf("invalid CSV format: expected 7 fields, got %d", len(parts))
	}

	record := &WALRecord{}

	// Parse log_id
	var logID uint64
	if _, err := fmt.Sscanf(parts[0], "%d", &logID); err != nil {
		return nil, fmt.Errorf("failed to parse log_id: %w", err)
	}
	record.LogID = logID

	// Parse version
	record.Version = parts[1]

	// Parse operation
	opStr := parts[2]
	if opStr == "Insert" || opStr == "insert" {
		record.Operation = Insert
	} else if opStr == "Delete" || opStr == "delete" {
		record.Operation = Delete
	} else {
		return nil, fmt.Errorf("unknown operation: %s", opStr)
	}

	// Parse vector_id
	var vectorID uint64
	if _, err := fmt.Sscanf(parts[3], "%d", &vectorID); err != nil {
		return nil, fmt.Errorf("failed to parse vector_id: %w", err)
	}
	record.VectorID = vectorID

	// Decode vector from base64
	vectorBytes, err := base64.StdEncoding.DecodeString(parts[4])
	if err != nil {
		return nil, fmt.Errorf("failed to decode vector base64: %w", err)
	}

	// Convert bytes to []float32
	if len(vectorBytes)%4 != 0 {
		return nil, fmt.Errorf("invalid vector data length")
	}
	dim := len(vectorBytes) / 4
	record.Vector = make([]float32, dim)
	vectorBuf := bytes.NewReader(vectorBytes)
	for i := 0; i < dim; i++ {
		if err := binary.Read(vectorBuf, binary.BigEndian, &record.Vector[i]); err != nil {
			return nil, fmt.Errorf("failed to read vector float: %w", err)
		}
	}

	// Unescape and parse doc JSON
	docStr := strings.ReplaceAll(parts[5], "\\\"", "\"")
	docStr = strings.ReplaceAll(docStr, "\\n", "\n")
	if err := json.Unmarshal([]byte(docStr), &record.Doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal doc: %w", err)
	}

	// Unescape and parse attributes JSON
	attrStr := strings.ReplaceAll(parts[6], "\\\"", "\"")
	attrStr = strings.ReplaceAll(attrStr, "\\n", "\n")
	if err := json.Unmarshal([]byte(attrStr), &record.Attributes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attributes: %w", err)
	}

	return record, nil
}

// parseCSVLine parses a CSV line handling quoted fields
func parseCSVLine(line string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false
	escapeNext := false

	for i := 0; i < len(line); i++ {
		ch := line[i]

		if escapeNext {
			current.WriteByte(ch)
			escapeNext = false
			continue
		}

		if ch == '\\' {
			escapeNext = true
			current.WriteByte(ch)
			continue
		}

		if ch == '"' {
			inQuotes = !inQuotes
			continue
		}

		if ch == ',' && !inQuotes {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	// Add last field
	if current.Len() > 0 || len(parts) > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// String returns a string representation of the operation
func (op WALOperation) String() string {
	switch op {
	case Insert:
		return "Insert"
	case Delete:
		return "Delete"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}
