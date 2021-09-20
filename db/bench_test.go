package db

import (
	"fmt"
	"testing"
)

var benchSize int

func Benchmark0PutBlock(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkbuf(asString(n))
		_, err = db.PutBlock("sha256", val)
		if err != nil {
			b.Fatal(err)
		}
		benchSize = n
	}
}

func Benchmark1Sync(b *testing.B) {
	shell("/bin/bash", "-c", "echo 3 | sudo tee /proc/sys/vm/drop_caches")
	// os.Stat("/tmp/bench")
	// time.Sleep(10 * time.Second)
}

func Benchmark2GetBlock(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	// fmt.Println("bench size:", benchSize)
	for n := 0; n <= benchSize; n++ {
		path, err := pathFromString(db, "blob", "sha256", asString(n))
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.GetBlock(path)
		if err != nil {
			fmt.Printf("n: %d\n", n)
			b.Fatal(err)
		}
	}
}

func XXXBenchmarkPutBlockSame(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	val := mkbuf("foo")
	for n := 0; n < b.N; n++ {
		gotpath, err := db.PutBlock("sha256", val)
		_ = gotpath
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutGetBlock(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkbuf(asString(n))
		block, err := db.PutBlock("sha256", val)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.GetBlock(block.Path)
		if err != nil {
			//	fmt.Printf("n: %d\n", n)
			b.Fatal(err)
		}
	}
}
