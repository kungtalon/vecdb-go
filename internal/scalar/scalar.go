package scalar

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"vecdb-go/internal/common"

	"github.com/nutsdb/nutsdb"
)

const (
	NamespaceDocs = "docs"
	NamespaceWals = "wals"
)

var (
	keyIDMax = []byte("__id_max__")
)

type KVPair[T any] struct {
	Key   []byte
	Value T
}

type KVIterator[T any] iter.Seq[KVPair[T]]

// ScalarStorage defines the interface for scalar database operations
// This matches the ScalarStorage trait in Rust
type ScalarStorage interface {
	// Put stores a key-value pair in the specified namespace
	Put(namespace string, key []byte, value []byte) error

	// Get retrieves a value by key from the specified namespace
	Get(namespace string, key []byte) ([]byte, error)

	// GetValue retrieves a document by ID from the specified namespace
	GetValue(namespace string, id uint64) (common.DocMap, error)

	// MultiGetValue retrieves multiple documents by IDs from the specified namespace
	MultiGetValue(namespace string, ids []uint64) ([]common.DocMap, error)

	// GenIncrIDs generates a sequence of unique IDs for a namespace
	GenIncrIDs(namespace string, count int) ([]uint64, error)

	// Iterator returns an iterator for all key-value pairs in the specified namespace
	Iterator(namespace string) (ScalarIterator, error)

	// Close closes the database
	Close() error
}

type ScalarOption struct {
	DIR     string   `toml:"dir"`
	Buckets []string `toml:"buckets"`
}

type ScalarIterator KVIterator[[]byte]

// nutsDBStorage implements ScalarStorage using NutsDB
type nutsDBStorage struct {
	db *nutsdb.DB
}

var _ ScalarStorage = (*nutsDBStorage)(nil)

// NewScalarStorage creates a new NutsDB-based scalar storage
// This matches the new_scalar_storage function in Rust
func NewScalarStorage(opts *ScalarOption) (ScalarStorage, error) {
	nutsdbOpts := nutsdb.DefaultOptions
	nutsdbOpts.Dir = opts.DIR
	nutsdbOpts.EntryIdxMode = nutsdb.HintKeyValAndRAMIdxMode // Better performance for key-value operations
	nutsdbOpts.SegmentSize = 64 * 1024 * 1024                // 64MB segments

	db, err := nutsdb.Open(nutsdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open nutsdb: %w. Config: %+v", err, nutsdbOpts)
	}

	for _, bucket := range opts.Buckets {
		if err = db.Update(func(tx *nutsdb.Tx) error {
			exists := tx.ExistBucket(nutsdb.DataStructureBTree, bucket)
			if exists {
				return nil
			}

			err := tx.NewBucket(nutsdb.DataStructureBTree, bucket)
			return err
		}); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create bucket %s: %w", bucket, err)
		}
	}

	storage := &nutsDBStorage{
		db: db,
	}

	return storage, nil
}

// Put stores a key-value pair in the specified namespace
func (s *nutsDBStorage) Put(namespace string, key []byte, value []byte) error {
	err := s.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(namespace, key, value, 0) // 0 means no TTL
	})
	if err != nil {
		return fmt.Errorf("failed to put key-value: %w", err)
	}

	return nil
}

// Get retrieves a value by key from the specified namespace
func (s *nutsDBStorage) Get(namespace string, key []byte) ([]byte, error) {
	var value []byte

	err := s.db.View(func(tx *nutsdb.Tx) error {
		entry, err := tx.Get(namespace, key)
		if err != nil {
			return err
		}

		value = entry

		return nil
	})
	if err != nil {
		if err == nutsdb.ErrKeyNotFound {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	return value, nil
}

// GetValue retrieves a document by ID from the specified namespace
func (s *nutsDBStorage) GetValue(namespace string, id uint64) (common.DocMap, error) {
	key := EncodeID(id)
	data, err := s.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	return common.JSONUnmarshal[common.DocMap](data)
}

// MultiGetValue retrieves multiple documents by IDs from the specified namespace
func (s *nutsDBStorage) MultiGetValue(namespace string, ids []uint64) ([]common.DocMap, error) {
	results := make([]common.DocMap, 0, len(ids))

	// NutsDB doesn't have native multi-get, so we do multiple single gets in one transaction
	err := s.db.View(func(tx *nutsdb.Tx) error {
		for _, id := range ids {
			key := EncodeID(id)
			entry, err := tx.Get(namespace, key)
			if err != nil {
				if err == nutsdb.ErrKeyNotFound {
					// Return empty map for missing entries
					results = append(results, common.DocMap{})
					continue
				}
				return err
			}

			doc, err := common.JSONUnmarshal[common.DocMap](entry)
			if err != nil {
				return fmt.Errorf("failed to deserialize doc for id %d: %w", id, err)
			}
			results = append(results, doc)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to multi-get values: %w", err)
	}

	return results, nil
}

// GenIncrIDs generates a sequence of unique IDs for a namespace
func (s *nutsDBStorage) GenIncrIDs(namespace string, count int) ([]uint64, error) {
	var ids []uint64

	err := s.db.Update(func(tx *nutsdb.Tx) error {
		maxIDKey := []byte(keyIDMax)

		// Get current max ID
		var maxID uint64
		entry, err := tx.Get(namespace, maxIDKey)
		if err != nil {
			if err == nutsdb.ErrKeyNotFound {
				maxID = 0
			} else {
				return fmt.Errorf("failed to get max id: %w", err)
			}
		} else {
			maxID = binary.BigEndian.Uint64(entry)
		}

		// Generate new IDs
		newMaxID := maxID + uint64(count)
		ids = make([]uint64, count)
		for i := 0; i < count; i++ {
			ids[i] = maxID + uint64(i) + 1
		}

		// Update max ID
		newMaxIDBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(newMaxIDBytes, newMaxID)
		err = tx.Put(namespace, maxIDKey, newMaxIDBytes, 0)
		if err != nil {
			return fmt.Errorf("failed to update max id: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ids, nil
}

// Close closes the database
func (s *nutsDBStorage) Close() error {
	return s.db.Close()
}

// Iterator returns an iterator for all key-value pairs in the specified namespace
func (s *nutsDBStorage) Iterator(namespace string) (ScalarIterator, error) {
	// Create a snapshot of all entries
	var keys [][]byte
	var values [][]byte

	err := s.db.View(func(tx *nutsdb.Tx) error {
		var err error

		keys, values, err = tx.GetAll(namespace)

		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get all entries: %w", err)
	}

	return func(yield func(KVPair[[]byte]) bool) {
		for i, k := range keys {
			if !yield(KVPair[[]byte]{Key: k, Value: values[i]}) {
				return
			}
		}
	}, nil
}

// EncodeID converts a uint64 ID to a byte slice key
func EncodeID(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

// DecodeID converts a byte slice key to uint64 ID
func DecodeID(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key)
}

// DebugPrintDB prints all entries in the database for debugging
func DebugPrintDB(s ScalarStorage, namespace string) error {
	iter, err := s.Iterator(namespace)
	if err != nil {
		return err
	}

	for pair := range iter {
		key := pair.Key
		value := pair.Value

		// Check if this is the special max ID key
		if string(key) == string(keyIDMax) {
			maxID := binary.BigEndian.Uint64(value)
			slog.Debug("[SPECIAL]", "key", string(keyIDMax), "value", maxID)
			continue
		}

		// Try to decode as ID
		if len(key) == 8 {
			id := DecodeID(key)
			var doc common.DocMap
			err := json.Unmarshal(value, &doc)
			if err == nil {
				slog.Debug("[DOC]", "id", id, "value", doc)
				continue
			}
		}

		// Otherwise print as raw bytes
		slog.Debug("[RAW]", "key", string(key), "value", string(value))
	}

	return nil
}
