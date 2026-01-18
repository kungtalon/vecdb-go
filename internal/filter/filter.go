package filter

import (
	"github.com/RoaringBitmap/roaring"
	faiss "github.com/blevesearch/go-faiss"
)

// IdFilter wraps a roaring bitmap for efficient ID filtering
type IdFilter struct {
	bitmap *roaring.Bitmap
}

// NewIdFilter creates a new empty IdFilter
func NewIdFilter() *IdFilter {
	return &IdFilter{
		bitmap: roaring.New(),
	}
}

// NewIdFilterFrom creates an IdFilter from an existing bitmap
func NewIdFilterFrom(bitmap *roaring.Bitmap) *IdFilter {
	return &IdFilter{
		bitmap: bitmap,
	}
}

// Add adds an ID to the filter
func (f *IdFilter) Add(id uint64) {
	f.bitmap.Add(uint32(id))
}

// AddAll adds multiple IDs to the filter
func (f *IdFilter) AddAll(ids []uint64) {
	for _, id := range ids {
		f.Add(id)
	}
}

// Filter checks if an ID is in the filter
func (f *IdFilter) Filter(id uint64) bool {
	return f.bitmap.Contains(uint32(id))
}

// AsSelector converts the IdFilter to a FAISS IdSelector for use in searches
func (f *IdFilter) AsSelector() (faiss.Selector, error) {
	ids := make([]int64, 0, f.bitmap.GetCardinality())
	iter := f.bitmap.Iterator()
	for iter.HasNext() {
		id := iter.Next()
		ids = append(ids, int64(id))
	}
	return faiss.NewIDSelectorBatch(ids)
}

// GetBitmap returns the underlying roaring bitmap
func (f *IdFilter) GetBitmap() *roaring.Bitmap {
	return f.bitmap
}

// Clone creates a copy of the filter
func (f *IdFilter) Clone() *IdFilter {
	return &IdFilter{
		bitmap: f.bitmap.Clone(),
	}
}
