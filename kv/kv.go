package kv

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	. "github.com/stevegt/goadapt"
)

// KV provides pure key-value storage with automatic subdirectory creation
type KV struct {
	Dir string
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
