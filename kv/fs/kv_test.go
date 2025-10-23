package kv

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func setup(t *testing.T) (*KV, string) {
	dir, err := ioutil.TempDir("", "kv-test-*")
	if err != nil {
		t.Fatal(err)
	}
	kv := NewKV(dir)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return kv, dir
}

func TestPutGet(t *testing.T) {
	kv, _ := setup(t)

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

func TestDelete(t *testing.T) {
	kv, _ := setup(t)

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
	kv, _ := setup(t)

	_, err := kv.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	kv, _ := setup(t)

	err := kv.Delete("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}
}

func TestCustomOptions(t *testing.T) {
	dir, err := ioutil.TempDir("", "kv-opts-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	kv := NewKVWithOptions(dir, Options{SplitSize: 2, NestingLevels: 3})
	if kv.SplitSize != 2 {
		t.Fatalf("expected SplitSize 2, got %d", kv.SplitSize)
	}
	if kv.NestingLevels != 3 {
		t.Fatalf("expected NestingLevels 3, got %d", kv.NestingLevels)
	}

	key := "abcdef123456"
	data := []byte("custom opts test")

	err = kv.Put(key, data)
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

	// XXX look in the filesystem to verify correct path structure
}

func TestDefaultOptions(t *testing.T) {
	kv, _ := setup(t)

	if kv.SplitSize != 3 {
		t.Fatalf("expected default SplitSize 3, got %d", kv.SplitSize)
	}
	if kv.NestingLevels != 3 {
		t.Fatalf("expected default NestingLevels 3, got %d", kv.NestingLevels)
	}

	// XXX create a key and verify path structure
}

func TestLongKey(t *testing.T) {
	kv, _ := setup(t)

	key := "verylongkeyabcdefghijklmnopqrstuvwxyz"
	data := []byte("long key data")

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

func TestShortKey(t *testing.T) {
	kv, _ := setup(t)

	key := "ab"
	data := []byte("short key")

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

func TestMultiplePuts(t *testing.T) {
	kv, _ := setup(t)

	// Write 10 keys
	for i := 0; i < 10; i++ {
		key := string(rune('a'+(i%26))) + string(rune('a'+(i/26)%26)) + "key"
		data := []byte{byte(i)}
		err := kv.Put(key, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify all keys exist and have correct data
	for i := 0; i < 10; i++ {
		key := string(rune('a'+(i%26))) + string(rune('a'+(i/26)%26)) + "key"
		expected := []byte{byte(i)}
		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("failed to get key %s: %v", key, err)
		}
		if !bytes.Equal(expected, got) {
			t.Fatalf("key %s: expected %q, got %q", key, expected, got)
		}
	}
}
