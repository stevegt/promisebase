package kv

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	. "github.com/stevegt/goadapt"
)

var StatsLength = 10 // Minimum samples before analysis

// KV provides pure key-value storage with automatic subdirectory creation
type KV struct {
	Dir string

	// Background scanner
	scanTrigger chan string
	scanCtx     context.Context
	scanCancel  context.CancelFunc
	scanStats   map[string][]scanResult
	scanTimeWma float64 // Weighted moving average of all scan times
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

// findKeyPath searches for key from shallowest to deepest nesting
func (kv *KV) findKeyPath(key string) (string, bool) {
	maxDepth := len(key) / 2

	for depth := 0; depth <= maxDepth; depth++ {
		path := kv.Dir
		for i := 0; i < depth; i++ {
			if i*2+2 <= len(key) {
				path = filepath.Join(path, key[i*2:i*2+2])
			}
		}
		path = filepath.Join(path, key)

		if exists(path) {
			return path, true
		}
	}
	return "", false
}

// defaultKeyPath generates deepest path for new keys
func (kv *KV) defaultKeyPath(key string) string {
	path := kv.Dir
	maxDepth := len(key) / 2

	bestPath := path
	for i := 0; i < maxDepth; i++ {
		if i*2+2 <= len(key) {
			path = filepath.Join(path, key[i*2:i*2+2])
			// check if path exists and is a directory
			fileInfo, err := os.Stat(path)
			if err == nil && fileInfo.IsDir() {
				bestPath = path
			} else {
				break
			}
		}
	}
	return filepath.Join(bestPath, key)
}

func (kv *KV) Get(key string) (data []byte, err error) {
	defer Return(&err)

	path, found := kv.findKeyPath(key)
	ErrnoIf(!found, syscall.ENOENT, "not found: %s", key)

	data, err = ioutil.ReadFile(path)
	Ck(err)
	return data, nil
}

// Put stores data for the given key
func (kv *KV) Put(key string, data []byte) (err error) {
	defer Return(&err)

	path := kv.defaultKeyPath(key)
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

	path, found := kv.findKeyPath(key)
	ErrnoIf(!found, syscall.ENOENT, "not found: %s", key)

	err = os.Remove(path)
	Ck(err)
	return nil
}

// scanWorker runs in background, processing scan triggers serially
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

// scanDirectory measures directory performance with rate limiting
func (kv *KV) scanDirectory(dir string) {
	// Check if we scanned recently
	if stats, exists := kv.scanStats[dir]; exists && len(stats) > 0 {
		lastScan := stats[len(stats)-1].timestamp
		if time.Since(lastScan) < 1*time.Second {
			return // Skip scan, too soon
		}
	}

	start := time.Now()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return // Directory might not exist yet
	}
	scanTime := time.Since(start)

	// Store scan result
	result := scanResult{
		entryCount: len(entries),
		scanTime:   scanTime,
		timestamp:  time.Now(),
	}
	kv.scanStats[dir] = append(kv.scanStats[dir], result)

	kv.scanTimeWma = 0.9*kv.scanTimeWma + 0.1*float64(scanTime.Milliseconds())

	// Keep only recent results
	if len(kv.scanStats[dir]) > StatsLength {
		kv.scanStats[dir] = kv.scanStats[dir][1:]
	}

	// Analyze performance curve if we have enough data
	if len(kv.scanStats[dir]) >= StatsLength {
		kv.analyzePerformance(dir)
	}
}

// analyzePerformance examines scan time curve and triggers splitting if degraded
func (kv *KV) analyzePerformance(dir string) {
	stats := kv.scanStats[dir]

	/*
		// simpler approach: if last scan time > 100ms, split
		last := stats[len(stats)-1]
		if last.scanTime > 100*time.Millisecond {
			fmt.Printf("Splitting directory %s due to scan time %v\n", dir, last.scanTime)
			kv.splitDirectory(dir)
		}
	*/

	// statistical approach: split if last scan time > 2*scanTimeWma
	last := stats[len(stats)-1]
	if float64(last.scanTime.Milliseconds()) > 2*kv.scanTimeWma {
		fmt.Printf("Splitting directory %s due to scan time %v (WMA %.2fms)\n", dir, last.scanTime, kv.scanTimeWma)
		kv.splitDirectory(dir)
	}

	/*
		if len(stats) < 3 {
			return
		}

			// Compare earliest and latest samples
			first := stats[0]
			last := stats[len(stats)-1]

			// Expected: scan time scales linearly with entry count
			// If actual time >> expected, directory needs splitting
			expectedRatio := float64(last.entryCount) / float64(first.entryCount)
			actualRatio := float64(last.scanTime) / float64(first.scanTime)
			fmt.Printf("Dir: %s, Entries: %d -> %d, Time: %v -> %v, ExpectedRatio: %.2f, ActualRatio: %.2f\n",
				dir, first.entryCount, last.entryCount, first.scanTime, last.scanTime, expectedRatio, actualRatio)

			// Trigger split if actual > 2x expected (quadratic behavior)
			if actualRatio > 2*expectedRatio {
				kv.splitDirectory(dir)
			}
	*/

}

func (kv *KV) splitDirectory(dir string) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		oldPath := filepath.Join(dir, filename)
		relPath, err := filepath.Rel(kv.Dir, oldPath)
		if err != nil {
			continue
		}

		depth := strings.Count(relPath, string(os.PathSeparator))
		startIdx := depth * 2

		if len(filename) < startIdx+2 {
			continue
		}

		subdir := filename[startIdx : startIdx+2]
		newDir := filepath.Join(dir, subdir)
		newPath := filepath.Join(newDir, filename)

		os.MkdirAll(newDir, 0755)
		os.Rename(oldPath, newPath)
	}

	delete(kv.scanStats, dir)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
