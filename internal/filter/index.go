package filter

import (
	"github.com/RoaringBitmap/roaring"
)

// FilterOp represents a filter operation type
type FilterOp int

const (
	Equal FilterOp = iota
	NotEqual
)

// IntFilterInput defines an integer field filter
type IntFilterInput struct {
	Field  string
	Op     FilterOp
	Target int64
}

// IntFilterIndex manages attribute-based filtering using roaring bitmaps
type IntFilterIndex struct {
	// intFieldFilters maps field name -> value -> bitmap of IDs
	intFieldFilters map[string]map[int64]*roaring.Bitmap
}

// NewIntFilterIndex creates a new integer filter index
func NewIntFilterIndex() *IntFilterIndex {
	return &IntFilterIndex{
		intFieldFilters: make(map[string]map[int64]*roaring.Bitmap),
	}
}

// Upsert adds or updates an ID for a field-value pair
func (idx *IntFilterIndex) Upsert(field string, value int64, id uint64) {
	filterMapByValue, exists := idx.intFieldFilters[field]
	if !exists {
		filterMapByValue = make(map[int64]*roaring.Bitmap)
		idx.intFieldFilters[field] = filterMapByValue
	}

	bitmap, exists := filterMapByValue[value]
	if !exists {
		bitmap = roaring.New()
		filterMapByValue[value] = bitmap
	}

	bitmap.Add(uint32(id))
}

// Remove removes an ID from a field-value pair
func (idx *IntFilterIndex) Remove(field string, value int64, id uint64) {
	filterMapByValue, exists := idx.intFieldFilters[field]
	if !exists {
		return
	}

	bitmap, exists := filterMapByValue[value]
	if !exists {
		return
	}

	bitmap.Remove(uint32(id))
	if bitmap.IsEmpty() {
		delete(filterMapByValue, value)
	}
}

// Apply applies the filter to an existing bitmap
func (idx *IntFilterIndex) Apply(input *IntFilterInput, bitmap *roaring.Bitmap) *roaring.Bitmap {
	if input.Op == Equal {
		valueToMap, exists := idx.intFieldFilters[input.Field]
		if !exists {
			return bitmap.Clone()
		}

		curBitmap, exists := valueToMap[input.Target]
		if !exists {
			return bitmap.Clone()
		}

		return roaring.Or(bitmap, curBitmap)
	}

	if input.Op == NotEqual {
		valueToMap, exists := idx.intFieldFilters[input.Field]
		if !exists {
			return bitmap.Clone()
		}

		resBitmap := bitmap.Clone()
		for value, curBitmap := range valueToMap {
			if value == input.Target {
				continue
			}
			resBitmap.Or(curBitmap)
		}

		return resBitmap
	}

	return bitmap.Clone()
}
