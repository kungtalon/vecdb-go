package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"vecdb-go/internal/filter"
	"vecdb-go/internal/index"
	"vecdb-go/internal/persistence"
	"vecdb-go/internal/scalar"
)

// Example demonstrating the use of text encoder for debugging WAL files
func main() {
	// Create a temporary directory for this example
	tmpDir := "example_wal_output"
	os.MkdirAll(tmpDir, 0755)
	defer func() {
		fmt.Printf("\nCleaning up %s...\n", tmpDir)
		os.RemoveAll(tmpDir)
	}()

	fmt.Println("=== WAL Encoder Example ===\n")

	// Example 1: Using Binary Encoder (production default)
	fmt.Println("1. Creating WAL with Binary Encoder (production default):")
	binaryPath := filepath.Join(tmpDir, "binary.wal")
	createExampleWAL(binaryPath, persistence.NewBinaryWALEncoder("v1"), "binary", tmpDir)

	// Example 2: Using Text Encoder (for debugging)
	fmt.Println("\n2. Creating WAL with Text Encoder (for debugging):")
	textPath := filepath.Join(tmpDir, "text.wal")
	createExampleWAL(textPath, persistence.NewTextWALEncoder("v1"), "text", tmpDir)

	// Show the difference
	showFileSizes(binaryPath, textPath)
	showTextContent(textPath)
}

func createExampleWAL(walPath string, encoder persistence.WALEncoder, encoderType string, tmpDir string) {
	// Create minimal dependencies for the demo
	scalarPath := filepath.Join(tmpDir, "scalar_"+encoderType+".db")
	scalarStorage, err := scalar.NewScalarStorage(&scalar.ScalarOption{
		DIR:     scalarPath,
		Buckets: []string{scalar.NamespaceDocs},
	})
	if err != nil {
		log.Fatalf("Failed to create scalar storage: %v", err)
	}
	defer scalarStorage.Close()

	filterIndex := filter.NewIntFilterIndex()
	vectorIndex, err := index.NewIndex("flat", 3, "l2", nil)
	if err != nil {
		log.Fatalf("Failed to create vector index: %v", err)
	}

	p, err := persistence.NewPersistenceWithEncoder(walPath, encoder)
	if err != nil {
		log.Fatalf("Failed to create persistence: %v", err)
	}
	defer p.Close()

	// Write sample records
	records := []struct {
		id         uint64
		vector     []float32
		doc        map[string]any
		attributes map[string]any
	}{
		{
			id:     1,
			vector: []float32{0.1, 0.2, 0.3},
			doc:    map[string]any{"title": "First Document", "content": "This is the first document"},
			attributes: map[string]any{"category": int64(1), "priority": int64(10)},
		},
		{
			id:     2,
			vector: []float32{0.4, 0.5, 0.6},
			doc:    map[string]any{"title": "Second Document", "content": "This is the second document"},
			attributes: map[string]any{"category": int64(2), "priority": int64(5)},
		},
		{
			id:     3,
			vector: []float32{0.7, 0.8, 0.9},
			doc:    map[string]any{"title": "Third Document", "content": "This is the third document"},
			attributes: map[string]any{"category": int64(1), "priority": int64(15)},
		},
	}

	for _, record := range records {
		// Write with eager=false since we're just creating a demo WAL file
		err := p.Write(record.id, record.vector, record.doc, record.attributes, false, scalarStorage, filterIndex, vectorIndex, 3)
		if err != nil {
			log.Fatalf("Failed to write record: %v", err)
		}
	}

	err = p.Flush()
	if err != nil {
		log.Fatalf("Failed to flush: %v", err)
	}

	fmt.Printf("   ✓ Created %s WAL file: %s\n", encoderType, walPath)
	fmt.Printf("   ✓ Wrote %d records\n", len(records))
}

func showFileSizes(binaryPath, textPath string) {
	binaryInfo, err := os.Stat(binaryPath)
	if err != nil {
		log.Fatalf("Failed to stat binary file: %v", err)
	}

	textInfo, err := os.Stat(textPath)
	if err != nil {
		log.Fatalf("Failed to stat text file: %v", err)
	}

	fmt.Println("\n3. Comparing file sizes:")
	fmt.Printf("   Binary WAL: %d bytes\n", binaryInfo.Size())
	fmt.Printf("   Text WAL:   %d bytes\n", textInfo.Size())
	fmt.Printf("   Text is %.2fx larger (trade-off for readability)\n", 
		float64(textInfo.Size())/float64(binaryInfo.Size()))
}

func showTextContent(textPath string) {
	content, err := os.ReadFile(textPath)
	if err != nil {
		log.Fatalf("Failed to read text file: %v", err)
	}

	fmt.Println("\n4. Text WAL Content (human-readable for debugging):")
	fmt.Println("   " + textPath)
	fmt.Println("   ─────────────────────────────────────────────────")
	fmt.Printf("%s", content)
	fmt.Println("   ─────────────────────────────────────────────────")
}
