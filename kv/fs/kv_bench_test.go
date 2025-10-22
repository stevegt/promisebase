package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func BenchmarkPut(b *testing.B) {
	kv := NewKV("/tmp/kv-bench")
	defer kv.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
	}
}

func BenchmarkGet(b *testing.B) {
	kv := NewKV("/tmp/kv-bench")
	defer kv.Close()

	// Pre-populate 10000 keys to trigger splitting
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%08d", i)
		kv.Put(key, []byte(key))
	}
	time.Sleep(2 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i%10000)
		kv.Get(key)
	}
}

func BenchmarkDynamicSplitVerified(b *testing.B) {
	kv := NewKV("/tmp/kv-bench-verified")
	defer kv.Close()
	StatsLength = 3

	// maxDepth := 0
	// checkInterval := 50

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Keys with similar prefixes force same directory
		key := fmt.Sprintf("aa%06d", i)
		kv.Put(key, []byte{byte(i)})

		/*
			// Periodically check directory depth
			if i%checkInterval == 0 {
				// time.Sleep(50 * time.Millisecond)
				depth := measureMaxDepth(kv.Dir)
				if depth > maxDepth {
					b.Logf("Split detected at iteration %d: depth increased to %d", i, depth)
					maxDepth = depth
				}
			}
		*/
	}

	finalDepth := measureMaxDepth(kv.Dir)
	b.Logf("Final max depth: %d, keys written: %d", finalDepth, b.N)

	if finalDepth <= 1 {
		b.Logf("Warning: No splits detected - try longer benchmark with -benchtime=10s")
	}
}

func measureMaxDepth(root string) int {
	maxDepth := 0
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			rel, _ := filepath.Rel(root, path)
			// split by filepath separator and count depth
			parts := strings.Split(rel, string(os.PathSeparator))
			depth := len(parts) - 1 // exclude the file itself
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return nil
	})
	return maxDepth
}
