package persistence

import (
	"os"
	"sync"
	"sync/atomic"
)

type Persistence struct {
	filePath  string
	walWriter *os.File
	mu        sync.Mutex
	version   string
	counter   atomic.Uint64
}

type WALOperation int

const (
	Insert WALOperation = iota
	Delete
)

type WALRecord struct {
	logID     uint64
	version   string
	operation WALOperation
	data      []byte
}
