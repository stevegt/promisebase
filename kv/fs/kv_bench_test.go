package kv

import (
	"fmt"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	kv := NewKV(b.TempDir())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
	}
}

func BenchmarkGet(b *testing.B) {
	kv := NewKV(b.TempDir())

	// Pre-populate with 1000 keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%1000)
		kv.Get(key)
	}
}

func BenchmarkPutDelete(b *testing.B) {
	kv := NewKV(b.TempDir())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
		kv.Delete(key)
	}
}

func BenchmarkManyPuts(b *testing.B) {
	kv := NewKV(b.TempDir())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Keys with varied prefixes test full directory depth
		key := fmt.Sprintf("%06d-data-%08d", i%1000, i)
		kv.Put(key, []byte(key))
	}
}

func BenchmarkCustomOptions(b *testing.B) {
	kv := NewKVWithOptions(b.TempDir(), Options{SplitSize: 2, NestingLevels: 4})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
	}
}
