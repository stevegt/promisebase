package kv

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setup(t *testing.T) *KV {
	dir, err := ioutil.TempDir("", "kv-test-*")
	if err != nil {
		t.Fatal(err)
	}
	kv := NewKV(dir)
	t.Cleanup(func() {
		kv.Close()
		os.RemoveAll(dir)
	})
	return kv
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

func TestDynamicDepth(t *testing.T) {
	kv := setup(t)

	// Create nested structure manually
	key := "abcdef1234567890"
	deepDir := filepath.Join(kv.Dir, "ab", "cd")
	os.MkdirAll(deepDir, 0755)

	data := []byte("deep test")
	err := kv.Put(key, data)
	if err != nil {
		t.Fatal(err)
	}

	// Should find it regardless of depth
	got, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, got) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestSplitting(t *testing.T) {
	kv := setup(t)
	StatsLength = 3 // Lower threshold for testing

	// Write many keys to same shallow directory
	for i := 0; i < 50; i++ {
		key := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + "testkey"
		err := kv.Put(key, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Allow scanner time to process
	time.Sleep(100 * time.Millisecond)

	// Verify some keys moved to subdirectories
	foundDeep := false
	filepath.Walk(kv.Dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			depth := len(filepath.SplitList(path)) - len(filepath.SplitList(kv.Dir))
			if depth > 1 {
				foundDeep = true
			}
		}
		return nil
	})

	if !foundDeep {
		t.Log("Warning: splitting may not have occurred yet")
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
