# Filter Implementation

This package provides ID filtering capabilities for vector searches, translated from the Rust implementation.

## Key Components

### IdFilter (`filter.go`)
- **Uses Roaring Bitmaps**: Efficient compressed bitmap implementation for storing and filtering IDs
- **FAISS Integration**: Converts to FAISS `Selector` for use in `SearchWithIDs`
- **Methods**:
  - `NewIdFilter()`: Create empty filter
  - `Add(id uint64)`: Add single ID
  - `AddAll(ids []uint64)`: Add multiple IDs
  - `Filter(id uint64) bool`: Check if ID is in filter
  - `AsSelector()`: Convert to FAISS selector for search

### IntFilterIndex (`index.go`)
- **Attribute-based Filtering**: Filter by integer field values
- **Operations**: `Equal`, `NotEqual`
- **Methods**:
  - `Upsert(field, value, id)`: Add ID to field-value index
  - `Remove(field, value, id)`: Remove ID from field-value index
  - `Apply(input, bitmap)`: Apply filter to bitmap

## Usage Example

```go
import (
    "vecdb-go/internal/filter"
    "vecdb-go/internal/index"
)

// Create a filter with specific IDs
idFilter := filter.NewIdFilter()
idFilter.AddAll([]uint64{1, 2, 3, 5, 8, 13})

// Create search query with filter
query := index.NewSearchQuery(vector).WithFilter(idFilter)

// Search will use FAISS SearchWithIDs (filtering during search, not after)
results, err := idx.Search(query, k)
```

## Key Differences from Old Implementation

### Before (Post-filtering)
- Used `map[int64]bool` for ID filter
- Called `Search()` with larger k, then filtered results manually
- Less efficient for large result sets

### After (During-search filtering)
- Uses Roaring Bitmap for efficient ID storage
- Calls `SearchWithIDs()` with FAISS selector
- Filtering happens in FAISS C++ layer during search
- More efficient and accurate

## Rust Parity

This implementation matches the Rust version:
- ✅ `IdFilter` with roaring bitmap
- ✅ FAISS `IdSelector` integration
- ✅ `IntFilterIndex` for attribute filtering
- ✅ `FilterOp` enum (Equal/NotEqual)
- ✅ During-search filtering instead of post-filtering
