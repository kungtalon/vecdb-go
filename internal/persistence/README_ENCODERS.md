# WAL Encoder Interface

The persistence layer now supports pluggable encoders for writing and reading WAL (Write-Ahead Log) records. This allows you to choose between different encoding formats depending on your needs.

## Interface

```go
type WALEncoder interface {
    // EncodeRecord writes a WAL record to the writer
    EncodeRecord(writer io.Writer, record *WALRecord) error
    
    // DecodeRecord reads a WAL record from the reader
    DecodeRecord(reader *bufio.Reader) (*WALRecord, error)
    
    // Name returns the encoder name for identification
    Name() string
}
```

## Available Encoders

### 1. BinaryWALEncoder (Production Default)

**Features:**
- Compact binary format with CRC32 checksum
- Optimized for storage efficiency
- Fast encoding/decoding
- Data integrity verification

**Usage:**
```go
// Default - uses binary encoder automatically
p, err := persistence.NewPersistence("data.wal")

// Explicit binary encoder
encoder := persistence.NewBinaryWALEncoder("v1")
p, err := persistence.NewPersistenceWithEncoder("data.wal", encoder)
```

**Binary Format:**
```
[4 bytes: record length]
[8 bytes: log ID]
[1 byte: operation type]
[8 bytes: vector ID]
[4 bytes: dimension]
[dimension * 4 bytes: vector data]
[4 bytes: doc length]
[doc length bytes: doc JSON]
[4 bytes: attributes length]
[attributes length bytes: attributes JSON]
[4 bytes: CRC32 checksum]
```

### 2. TextWALEncoder (Debugging)

**Features:**
- Human-readable JSON format
- Easy to inspect and debug
- Can be viewed/edited with any text editor
- Approximately 2x larger than binary format

**Usage:**
```go
// Create persistence with text encoder
encoder := persistence.NewTextWALEncoder("v1")
p, err := persistence.NewPersistenceWithEncoder("data.wal", encoder)
```

**Text Format Example:**
```
=== WAL RECORD ===
{
  "log_id": 1,
  "version": "v1",
  "operation": "Insert",
  "vector_id": 42,
  "vector": [1.0, 2.0, 3.0],
  "doc": {
    "title": "Example Document",
    "content": "..."
  },
  "attributes": {
    "category": 1,
    "priority": 10
  }
}
```

## Use Cases

### Production Use (Binary)
- **Default for database operations** - Efficient storage and fast I/O
- **Long-term storage** - Minimal disk space usage
- **High-throughput systems** - Fast serialization/deserialization

### Development/Debugging (Text)
- **Inspecting WAL contents** - See exactly what was written
- **Debugging issues** - Human-readable format for troubleshooting
- **Testing** - Easy to verify correctness of WAL operations
- **Documentation** - Generate examples for documentation

## Example: Switching Between Encoders

```go
package main

import (
    "fmt"
    "vecdb-go/internal/persistence"
)

func main() {
    // Production: Binary encoder for efficiency
    prodWAL, _ := persistence.NewPersistence("production.wal")
    defer prodWAL.Close()
    
    // Development: Text encoder for debugging
    textEncoder := persistence.NewTextWALEncoder("v1")
    debugWAL, _ := persistence.NewPersistenceWithEncoder("debug.wal", textEncoder)
    defer debugWAL.Close()
    
    // Write same data to both
    vector := []float32{1.0, 2.0, 3.0}
    doc := map[string]any{"title": "Test"}
    attrs := map[string]any{"category": int64(1)}
    
    prodWAL.Write(1, vector, doc, attrs)
    debugWAL.Write(1, vector, doc, attrs)
    
    // Production: Binary file is compact
    // Debug: Text file is readable with cat/less
}
```

## Performance Comparison

| Encoder | File Size | Speed | Use Case |
|---------|-----------|-------|----------|
| Binary  | 100%      | Fast  | Production |
| Text    | ~200-250% | Moderate | Debugging |

## Implementation Notes

- Both encoders are fully compatible with the persistence layer
- The encoder choice doesn't affect database functionality
- You can convert between formats by reading with one encoder and writing with another
- Checksums are only in binary format (text format relies on JSON validation)

## Running the Demo

```bash
# See both encoders in action
go run examples/wal_encoder_demo.go
```

This will create example WAL files with both encoders and show the differences in file size and readability.
