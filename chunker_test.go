package pitbase

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
)

func TestChunker(t *testing.T) {
	// setup
	// polynomial was randomly generated from a call to chunker.Init()
	chunker, err := Rabin{Poly: 0x25d92e975e1aa3}.Init()
	tassert(t, err == nil, "%v", err)
	// fmt.Printf("%T %#v\n", chunker.Poly, chunker.Poly)
	tassert(t, chunker.Poly > 0, "polynomial is %v", chunker.Poly)

	// create some data
	size := 10 * miB
	var stream []byte
	stream = make([]byte, size)
	rand.Seed(42)
	// write random data into stream buf
	n, err := rand.Read(stream)
	tassert(t, err == nil, "rand.Read(): %v", err)
	tassert(t, size == n, "size: expected %d got %d", size, n)
	tassert(t, size == len(stream), "size: expected %d got %d", size, n)

	// chunk it
	rd := bytes.NewReader(stream)
	chunker.Start(rd)

	buf := make([]byte, 1*miB)
	var gotstream []byte
	for {
		chunk, err := chunker.Next(buf)
		if errors.Cause(err) == io.EOF {
			fmt.Println("EOF")
			break
		}
		fmt.Printf(".")
		expect := stream[chunk.Start : chunk.Start+chunk.Length]
		tassert(t, bytes.Compare(expect, chunk.Data) == 0, "chunk: expected %v got %v", expect, chunk.Data)
		gotstream = append(gotstream, chunk.Data...)
	}
	gotsize := len(gotstream)
	tassert(t, size == int(gotsize), "size: expected %d got %d", size, gotsize)
	tassert(t, bytes.Compare(stream, gotstream) == 0, "chunk: stream vs. gotstream mismatch")
}

func TestPutStream(t *testing.T) {
	// db := newdb()
	/*
		db, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}



		stream := ioutil.
		// chunker =
	*/
}
