package math

import (
	"encoding/json"
	"fmt"
)

// Matrix32 represents a matrix with float32 data in row-major order
type Matrix32 struct {
	Rows int
	Cols int
	Data []float32 // row-major: Data[i*Cols + j] = element at row i, col j
}

// Dims returns the number of rows and columns
func (m *Matrix32) Dims() (int, int) {
	return m.Rows, m.Cols
}

// RawData returns the underlying float32 slice
func (m *Matrix32) RawData() []float32 {
	return m.Data
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
