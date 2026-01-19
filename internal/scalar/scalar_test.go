package scalar

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"vecdb-go/internal/common"
)

func setupTestDB(t *testing.T) (ScalarStorage, string) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("test_nutsdb_%d", os.Getpid()))
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	db, err := NewScalarStorage(&ScalarOption{
		DIR:     tmpDir,
		Buckets: []string{NamespaceDocs, NamespaceWals, "default"},
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create test database: %v", err)
	}

	return db, tmpDir
}

func teardownTestDB(db ScalarStorage, tmpDir string) {
	db.Close()
	os.RemoveAll(tmpDir)
}

func TestPutAndGet(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	// Test basic Put and Get
	key := []byte("test_key")
	value := []byte("test_value")

	err := db.Put(NamespaceDocs, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := db.Get(NamespaceDocs, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Errorf("Expected %s, got %s", value, retrieved)
	}

	// Test Get non-existent key
	nonExistent, err := db.Get(NamespaceDocs, []byte("non_existent"))
	if err != nil {
		t.Fatalf("Get non-existent key failed: %v", err)
	}
	if nonExistent != nil {
		t.Errorf("Expected nil for non-existent key, got %v", nonExistent)
	}
}

func TestGetValueAndMultiGetValue(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	// Store some documents
	doc1 := common.DocMap{
		"name": "Alice",
		"age":  float64(30),
		"city": "New York",
	}
	doc2 := common.DocMap{
		"name": "Bob",
		"age":  float64(25),
		"city": "San Francisco",
	}
	doc3 := common.DocMap{
		"name": "Charlie",
		"age":  float64(35),
		"city": "Seattle",
	}

	// Serialize and store
	data1, _ := json.Marshal(doc1)
	data2, _ := json.Marshal(doc2)
	data3, _ := json.Marshal(doc3)

	if err := db.Put(NamespaceDocs, EncodeID(1), data1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(NamespaceDocs, EncodeID(2), data2); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(NamespaceDocs, EncodeID(3), data3); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Print all
	if err := DebugPrintDB(db, NamespaceDocs); err != nil {
		t.Fatalf("DebugPrintDB failed: %v", err)
	}

	// Test GetValue
	retrieved, err := db.GetValue(NamespaceDocs, 1)
	if err != nil {
		t.Fatalf("GetValue failed: %v", err)
	}
	if retrieved["name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", retrieved["name"])
	}

	// Test MultiGetValue
	docs, err := db.MultiGetValue(NamespaceDocs, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("MultiGetValue failed: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("Expected 3 documents, got %d", len(docs))
	}
	if docs[0]["name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", docs[0]["name"])
	}
	if docs[1]["name"] != "Bob" {
		t.Errorf("Expected Bob, got %v", docs[1]["name"])
	}
	if docs[2]["name"] != "Charlie" {
		t.Errorf("Expected Charlie, got %v", docs[2]["name"])
	}

	// Test MultiGetValue with non-existent IDs
	docs, err = db.MultiGetValue(NamespaceDocs, []uint64{1, 999, 3})
	if err != nil {
		t.Fatalf("MultiGetValue with non-existent ID failed: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("Expected 3 documents (with empty map), got %d", len(docs))
	}
	if len(docs[1]) != 0 {
		t.Errorf("Expected empty map for non-existent ID, got %v", docs[1])
	}
}

func TestGenIncrIDs(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	// Test generating IDs from scratch
	ids1, err := db.GenIncrIDs(NamespaceDocs, 5)
	if err != nil {
		t.Fatalf("GenIncrIDs failed: %v", err)
	}
	if len(ids1) != 5 {
		t.Errorf("Expected 5 IDs, got %d", len(ids1))
	}

	// IDs should be sequential starting from 1
	for i, id := range ids1 {
		expected := uint64(i + 1)
		if id != expected {
			t.Errorf("Expected ID %d, got %d", expected, id)
		}
	}

	// Generate more IDs - should continue from where we left off
	ids2, err := db.GenIncrIDs(NamespaceDocs, 3)
	if err != nil {
		t.Fatalf("Second GenIncrIDs failed: %v", err)
	}
	if len(ids2) != 3 {
		t.Errorf("Expected 3 IDs, got %d", len(ids2))
	}

	// Should start from 6
	for i, id := range ids2 {
		expected := uint64(6 + i)
		if id != expected {
			t.Errorf("Expected ID %d, got %d", expected, id)
		}
	}
}

func TestGenIncrIDsConcurrency(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	// Test concurrent ID generation to ensure thread safety
	const numGoroutines = 10
	const idsPerGoroutine = 10

	results := make(chan []uint64, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			ids, err := db.GenIncrIDs(NamespaceDocs, idsPerGoroutine)
			if err != nil {
				errors <- err
				return
			}
			results <- ids
		}()
	}

	// Collect all IDs
	allIDs := make(map[uint64]bool)
	for i := 0; i < numGoroutines; i++ {
		select {
		case err := <-errors:
			t.Fatalf("Concurrent GenIncrIDs failed: %v", err)
		case ids := <-results:
			for _, id := range ids {
				if allIDs[id] {
					t.Errorf("Duplicate ID generated: %d", id)
				}
				allIDs[id] = true
			}
		}
	}

	// Should have exactly numGoroutines * idsPerGoroutine unique IDs
	expectedCount := numGoroutines * idsPerGoroutine
	if len(allIDs) != expectedCount {
		t.Errorf("Expected %d unique IDs, got %d", expectedCount, len(allIDs))
	}
}

func TestIterator(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	// Put some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := db.Put(NamespaceDocs, []byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Iterate and verify
	iter, err := db.Iterator(NamespaceDocs)
	if err != nil {
		t.Fatalf("Iterator creation failed: %v", err)
	}

	found := make(map[string]string)
	for pair := range iter {
		key := string(pair.Key)
		value := string(pair.Value)
		found[key] = value
	}

	// Verify all items were found
	for k, expectedV := range testData {
		if foundV, exists := found[k]; !exists {
			t.Errorf("Key %s not found in iterator", k)
		} else if foundV != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, foundV)
		}
	}
}

func TestUpdateExistingKey(t *testing.T) {
	db, tmpDir := setupTestDB(t)
	defer teardownTestDB(db, tmpDir)

	key := []byte("test_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	err := db.Put(NamespaceDocs, key, value1)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Update with new value
	err = db.Put(NamespaceDocs, key, value2)
	if err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	// Verify updated value
	retrieved, err := db.Get(NamespaceDocs, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value2) {
		t.Errorf("Expected %s, got %s", value2, retrieved)
	}
}
