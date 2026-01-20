package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"vecdb-go/internal/filter"
	"vecdb-go/internal/index"
	"vecdb-go/internal/scalar"
)

func TestTextWALEncoder(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test-text.wal")

	// Create persistence layer with text encoder
	textEncoder := NewTextWALEncoder(WALVersion)
	p, err := NewPersistenceWithEncoder(walPath, textEncoder)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	defer p.Close()

	// Write some records
	doc1 := map[string]any{"text": "hello world", "title": "Test Document"}
	attr1 := map[string]any{"category": int64(1), "priority": int64(10)}
	vector1 := []float32{1.0, 2.0, 3.0}

	err = p.WriteOnly(1, vector1, doc1, attr1)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	doc2 := map[string]any{"text": "another document", "title": "Second Document"}
	attr2 := map[string]any{"category": int64(2), "priority": int64(5)}
	vector2 := []float32{4.0, 5.0, 6.0}

	err = p.WriteOnly(2, vector2, doc2, attr2)
	if err != nil {
		t.Fatalf("Failed to write record: %v", err)
	}

	// Flush to disk
	err = p.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Check that file is readable as text
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	t.Logf("WAL file content:\n%s", string(content))

	// Verify pending count
	if p.GetPendingCount() != 2 {
		t.Errorf("Expected 2 pending records, got %d", p.GetPendingCount())
	}
}

func TestTextWALEncoderRestore(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test-text.wal")
	scalarPath := filepath.Join(tmpDir, "scalar.db")

	// Phase 1: Write records with text encoder
	{
		textEncoder := NewTextWALEncoder(WALVersion)
		p, err := NewPersistenceWithEncoder(walPath, textEncoder)
		if err != nil {
			t.Fatalf("Failed to create persistence: %v", err)
		}

		doc1 := map[string]any{"text": "test"}
		attr1 := map[string]any{"score": int64(100)}
		vector1 := []float32{1.0, 2.0, 3.0}

		err = p.WriteOnly(1, vector1, doc1, attr1)
		if err != nil {
			t.Fatalf("Failed to write record: %v", err)
		}

		err = p.Flush()
		if err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		p.Close()
	}

	// Phase 2: Restore from text WAL
	{
		textEncoder := NewTextWALEncoder(WALVersion)
		p, err := NewPersistenceWithEncoder(walPath, textEncoder)
		if err != nil {
			t.Fatalf("Failed to create persistence: %v", err)
		}
		defer p.Close()

		scalarStorage, err := scalar.NewScalarStorage(&scalar.ScalarOption{
			DIR:     scalarPath,
			Buckets: []string{scalar.NamespaceDocs},
		})
		if err != nil {
			t.Fatalf("Failed to create scalar storage: %v", err)
		}
		defer scalarStorage.Close()

		filterIndex := filter.NewIntFilterIndex()

		vectorIndex, err := index.NewFlatIndex(3, index.L2)
		if err != nil {
			t.Fatalf("Failed to create vector index: %v", err)
		}

		err = p.Restore(scalarStorage, filterIndex, vectorIndex, 3)
		if err != nil {
			t.Fatalf("Failed to restore: %v", err)
		}

		// Verify data was restored
		doc, err := scalarStorage.GetValue(scalar.NamespaceDocs, 1)
		if err != nil {
			t.Fatalf("Failed to get doc: %v", err)
		}
		if doc["text"] != "test" {
			t.Errorf("Expected text=test, got %v", doc["text"])
		}
	}
}

func TestBinaryVsTextEncoder(t *testing.T) {
	tmpDir := t.TempDir()

	// Test data
	doc := map[string]any{"content": "Hello, World!", "type": "greeting"}
	attr := map[string]any{"importance": int64(5)}
	vector := []float32{1.1, 2.2, 3.3, 4.4}

	// Test with binary encoder
	binaryPath := filepath.Join(tmpDir, "binary.wal")
	{
		p, err := NewPersistence(binaryPath) // Uses binary by default
		if err != nil {
			t.Fatalf("Failed to create binary persistence: %v", err)
		}

		err = p.WriteOnly(42, vector, doc, attr)
		if err != nil {
			t.Fatalf("Failed to write binary record: %v", err)
		}

		err = p.Flush()
		if err != nil {
			t.Fatalf("Failed to flush binary: %v", err)
		}
		p.Close()
	}

	// Test with text encoder
	textPath := filepath.Join(tmpDir, "text.wal")
	{
		textEncoder := NewTextWALEncoder(WALVersion)
		p, err := NewPersistenceWithEncoder(textPath, textEncoder)
		if err != nil {
			t.Fatalf("Failed to create text persistence: %v", err)
		}

		err = p.WriteOnly(42, vector, doc, attr)
		if err != nil {
			t.Fatalf("Failed to write text record: %v", err)
		}

		err = p.Flush()
		if err != nil {
			t.Fatalf("Failed to flush text: %v", err)
		}
		p.Close()
	}

	// Compare file sizes
	binaryInfo, err := os.Stat(binaryPath)
	if err != nil {
		t.Fatalf("Failed to stat binary file: %v", err)
	}

	textInfo, err := os.Stat(textPath)
	if err != nil {
		t.Fatalf("Failed to stat text file: %v", err)
	}

	t.Logf("Binary encoder size: %d bytes", binaryInfo.Size())
	t.Logf("Text encoder size: %d bytes", textInfo.Size())
	t.Logf("Text is %.2fx larger than binary", float64(textInfo.Size())/float64(binaryInfo.Size()))

	// Read and display text content
	textContent, err := os.ReadFile(textPath)
	if err != nil {
		t.Fatalf("Failed to read text file: %v", err)
	}
	t.Logf("Text WAL content:\n%s", string(textContent))
}

func TestEncoderNames(t *testing.T) {
	binaryEncoder := NewBinaryWALEncoder(WALVersion)
	textEncoder := NewTextWALEncoder(WALVersion)

	if binaryEncoder.Name() != "binary" {
		t.Errorf("Expected binary encoder name 'binary', got '%s'", binaryEncoder.Name())
	}

	if textEncoder.Name() != "text" {
		t.Errorf("Expected text encoder name 'text', got '%s'", textEncoder.Name())
	}
}
