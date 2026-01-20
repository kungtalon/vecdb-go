package math

import (
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
)

// Matrix32 represents a matrix with float32 data in row-major order
type Matrix32 struct {
	Rows int
	Cols int
	Data []float32 // row-major: Data[i*Cols + j] = element at row i, col j
}

func NewMatrix32(data [][]float32) (*Matrix32, error) {
	if len(data) == 0 {
		return &Matrix32{Rows: 0, Cols: 0, Data: []float32{}}, nil
	}

	rows := len(data)
	cols := len(data[0])

	// Check that all rows have the same length
	if _, hasAny := lo.Find(data, func(row []float32) bool {
		return len(row) != cols
	}); hasAny {
		return nil, fmt.Errorf("inconsistent row lengths in input data")
	}

	flatData := make([]float32, rows*cols)
	for i := range rows {
		copy(flatData[i*cols:(i+1)*cols], data[i])
	}

	return &Matrix32{Rows: rows, Cols: cols, Data: flatData}, nil
}

// NewMatrix32Empty creates an empty matrix with the given dimensions
func NewMatrix32Empty(rows, cols int) *Matrix32 {
	return &Matrix32{
		Rows: rows,
		Cols: cols,
		Data: make([]float32, rows*cols),
	}
}

func (m *Matrix32) Size() int {
	return m.Rows * m.Cols
}

// Dims returns the number of rows and columns
func (m *Matrix32) Dims() (int, int) {
	return m.Rows, m.Cols
}

// RawData returns the underlying float32 slice
func (m *Matrix32) RawData() []float32 {
	return m.Data
}

// At returns the element at row i, column j
func (m *Matrix32) At(i, j int) float32 {
	return m.Data[i*m.Cols+j]
}

// Set sets the element at row i, column j
func (m *Matrix32) Set(i, j int, val float32) {
	m.Data[i*m.Cols+j] = val
}

// UnmarshalJSON implements json.Unmarshaler interface
// Accepts JSON in the format: [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
func (m *Matrix32) UnmarshalJSON(data []byte) error {
	var temp [][]float32
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to unmarshal matrix: %w", err)
	}

	if len(temp) == 0 {
		m.Rows = 0
		m.Cols = 0
		m.Data = []float32{}
		return nil
	}

	rows := len(temp)
	cols := len(temp[0])

	// Check that all rows have the same length
	for i, row := range temp {
		if len(row) != cols {
			return fmt.Errorf("inconsistent row length at row %d: expected %d, got %d", i, cols, len(row))
		}
	}

	// Flatten to row-major order
	m.Rows = rows
	m.Cols = cols
	m.Data = make([]float32, rows*cols)

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			m.Data[i*cols+j] = temp[i][j]
		}
	}

	return nil
}
