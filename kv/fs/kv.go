package kv

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"time"

	. "github.com/stevegt/goadapt"
)

var StatsLength = 10 // Minimum samples before analysis

// KV provides pure key-value storage with fixed hierarchical nesting
type KV struct {
	Dir           string
	SplitSize     int // Characters per nesting level (default 3)
	NestingLevels int // Number of nesting levels (default 2)

	// Background scanner for metrics
	scanTrigger chan string
	scanCtx     context.Context
	scanCancel  context.CancelFunc
	scanStats   map[string][]scanResult
}

type scanResult struct {
	entryCount int
	scanTime   time.Duration
	timestamp  time.Time
}

// NewKV creates a new KV store with default options (3-char split, 2 levels)
func NewKV(dir string) *KV {
	return NewKVWithOptions(dir, 3, 2)
}

// NewKVWithOptions creates a KV store with custom split size and nesting levels
func NewKVWithOptions(dir string, splitSize int, nestingLevels int) *KV {
	if splitSize < 1 {
		splitSize = 3
	}
	if nestingLevels < 0 {
		nestingLevels = 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	kv := &KV{
		Dir:           dir,
		SplitSize:     splitSize,
		NestingLevels: nestingLevels,
		scanTrigger:   make(chan string, 100),
		scanCtx:       ctx,
		scanCancel:    cancel,
		scanStats:     make(map[string][]scanResult),
	}

	// Start background scanner for metrics collection
	go kv.scanWorker()

	return kv
}

// Close stops the background scanner
func (kv *KV) Close() {
	kv.scanCancel()
}

// keyPath generates deterministic nested path for key
// Example: key="abcdefghij" with splitSize=3, levels=2 → kv.Dir/abc/def/abcdefghij
func (kv *KV) keyPath(key string) string {
	path := kv.Dir

	// Add nesting levels based on key characters
	for level := 0; level < kv.NestingLevels; level++ {
		startIdx := level * kv.SplitSize
		endIdx := startIdx + kv.SplitSize

		if endIdx <= len(key) {
			path = filepath.Join(path, key[startIdx:endIdx])
		} else if startIdx < len(key) {
			// Partial segment if key is shorter than expected
			path = filepath.Join(path, key[startIdx:])
			break
		} else {
			// Key exhausted, stop nesting
			break
		}
	}

	return filepath.Join(path, key)
}

// Get retrieves data for the given key
func (kv *KV) Get(key string) (data []byte, err error) {
	defer Return(&err)

	path := kv.keyPath(key)
	ErrnoIf(!exists(path), syscall.ENOENT, "not found: %s", key)

	data, err = ioutil.ReadFile(path)
	Ck(err)
	return data, nil
}

// Put stores data for the given key
func (kv *KV) Put(key string, data []byte) (err error) {
	defer Return(&err)

	path := kv.keyPath(key)
	dir := filepath.Dir(path)

	err = os.MkdirAll(dir, 0755)
	Ck(err)

	err = ioutil.WriteFile(path, data, 0644)
	Ck(err)

	// Trigger background scan of parent directory
	select {
	case kv.scanTrigger <- dir:
	default:
		// Channel full, skip trigger
	}

	return nil
}

// Delete removes the key and its data
func (kv *KV) Delete(key string) (err error) {
	defer Return(&err)

	path := kv.keyPath(key)
	ErrnoIf(!exists(path), syscall.ENOENT, "not found: %s", key)

	err = os.Remove(path)
	Ck(err)
	return nil
}

// scanWorker runs in background, collecting performance metrics
func (kv *KV) scanWorker() {
	for {
		select {
		case <-kv.scanCtx.Done():
			return
		case dir := <-kv.scanTrigger:
			kv.scanDirectory(dir)
		}
	}
}

// scanDirectory measures directory performance (metrics only, no adaptive splitting)
func (kv *KV) scanDirectory(dir string) {
	// Rate limit: don't scan same directory more than once per 11 seconds
	if stats, exists := kv.scanStats[dir]; exists && len(stats) > 0 {
		lastScan := stats[len(stats)-1].timestamp
		if time.Since(lastScan) < 11*time.Second {
			return
		}
	}

	start := time.Now()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return // Directory might not exist yet
	}
	scanTime := time.Since(start)

	// Store scan result for metrics
	result := scanResult{
		entryCount: len(entries),
		scanTime:   scanTime,
		timestamp:  time.Now(),
	}
	kv.scanStats[dir] = append(kv.scanStats[dir], result)

	// Keep only recent results
	if len(kv.scanStats[dir]) > StatsLength {
		kv.scanStats[dir] = kv.scanStats[dir][1:]
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

