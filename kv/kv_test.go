package kv

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func setup(t *testing.T) *KV {
	dir, err := ioutil.TempDir("", "kv-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return &KV{Dir: dir}
}

func TestPutGet(t *testing.T) {
	kv := setup(t)
	
	key := "testkey123"
	data := []byte("test data")
	
	err := kv.Put(key, data)
	if err != nil {
		t.Fatal(err)
	}
	
	got, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	
	if !bytes.Equal(data, got) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestSubdirectories(t *testing.T) {
	kv := setup(t)
	
	key := "abcdef1234567890"
	data := []byte("subdirectory test")
	
	err := kv.Put(key, data)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify subdirectory structure
	expected := filepath.Join(kv.Dir, "abc", "def", key)
	if !exists(expected) {
		t.Fatalf("expected file at %s", expected)
	}
}

func TestDelete(t *testing.T) {
	kv := setup(t)
	
	key := "deletekey"
	data := []byte("to be deleted")
	
	err := kv.Put(key, data)
	if err != nil {
		t.Fatal(err)
	}
	
	err = kv.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	
	_, err = kv.Get(key)
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestGetNonExistent(t *testing.T) {
	kv := setup(t)
	
	_, err := kv.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}
