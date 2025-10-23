package kv

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	. "github.com/stevegt/goadapt"
)

// Options configures KV store behavior
type Options struct {
	SplitSize     int // Characters per nesting level (default 3)
	NestingLevels int // Number of nesting levels (default 2)
}

// KV provides pure key-value storage with fixed hierarchical nesting
type KV struct {
	Dir           string
	SplitSize     int
	NestingLevels int
}

// NewKV creates a new KV store with default options
func NewKV(dir string) *KV {
	return NewKVWithOptions(dir, Options{SplitSize: 3, NestingLevels: 3})
}

// NewKVWithOptions creates a KV store with custom options
func NewKVWithOptions(dir string, opts Options) *KV {
	if opts.SplitSize < 1 {
		opts.SplitSize = 3
	}
	if opts.NestingLevels < 0 {
		opts.NestingLevels = 3
	}

	return &KV{
		Dir:           dir,
		SplitSize:     opts.SplitSize,
		NestingLevels: opts.NestingLevels,
	}
}

// keyPath generates deterministic nested path for key
// Example: key="abcdefghij" with SplitSize=3, NestingLevels=2 → dir/abc/def/abcdefghij
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

func exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
