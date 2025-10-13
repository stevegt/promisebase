package kv

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	. "github.com/stevegt/goadapt"
)

// KV provides pure key-value storage with automatic subdirectory creation
type KV struct {
	Dir string

	// Background scanner
	scanTrigger chan string
	scanCtx     context.Context
	scanCancel  context.CancelFunc
	scanning    map[string]bool
	scanMutex   sync.Mutex
	scanStats   map[string][]scanResult
}

type scanResult struct {
	entryCount int
	scanTime   time.Duration
	timestamp  time.Time
}

// NewKV creates a new KV store with background scanner
func NewKV(dir string) *KV {
	ctx, cancel := context.WithCancel(context.Background())
	kv := &KV{
		Dir:         dir,
		scanTrigger: make(chan string, 100),
		scanCtx:     ctx,
		scanCancel:  cancel,
		scanning:    make(map[string]bool),
		scanStats:   make(map[string][]scanResult),
	}

	// Start background scanner
	go kv.scanWorker()

	return kv
}

// Close stops the background scanner
func (kv *KV) Close() {
	kv.scanCancel()
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

	// Trigger background scan of this directory
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

// scanWorker runs in background, processing scan triggers
func (kv *KV) scanWorker() {
	for {
		select {
		case <-kv.scanCtx.Done():
			return
		case dir := <-kv.scanTrigger:
			// Skip if already scanning this directory
			kv.scanMutex.Lock()
			if kv.scanning[dir] {
				kv.scanMutex.Unlock()
				continue
			}
			kv.scanning[dir] = true
			kv.scanMutex.Unlock()

			// Perform scan and collect stats
			kv.scanDirectory(dir)

			kv.scanMutex.Lock()
			delete(kv.scanning, dir)
			kv.scanMutex.Unlock()
		}
	}
}

// scanDirectory measures directory performance
func (kv *KV) scanDirectory(dir string) {
	start := time.Now()

	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return // Directory might not exist yet
	}

	scanTime := time.Since(start)

	// Store scan result
	kv.scanMutex.Lock()
	result := scanResult{
		entryCount: len(entries),
		scanTime:   scanTime,
		timestamp:  time.Now(),
	}
	kv.scanStats[dir] = append(kv.scanStats[dir], result)

	// Keep only recent results (last 10)
	if len(kv.scanStats[dir]) > 10 {
		kv.scanStats[dir] = kv.scanStats[dir][1:]
	}
	kv.scanMutex.Unlock()

	// TODO: Analyze performance curve and trigger split if needed
}

// keyPath converts key to filesystem path with subdirectories
func (kv *KV) keyPath(key string) string {
	// Use first 6 characters for two-level subdirectory nesting
	if len(key) >= 6 {
		return filepath.Join(kv.Dir, key[:3], key[3:6], key)
	}
	return filepath.Join(kv.Dir, key)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
