package pitbase

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
)

func TestChunker(t *testing.T) {
	// setup
	chunker, err := Chunker{Poly: 0x25d92e975e1aa3}.Init()
	tassert(t, err == nil, "%v", err)
	// fmt.Printf("%T %#v\n", chunker.Poly, chunker.Poly)
	tassert(t, chunker.Poly > 0, "polynomial is %v", chunker.Poly)

	// create some data
	size := 1 * miB
	var stream []byte
	stream = make([]byte, size)
	rand.Seed(42)
	n, err := rand.Read(stream)
	tassert(t, err == nil, "rand.Read(): %v", err)
	tassert(t, size == n, "size: expected %d got %d", size, n)
	tassert(t, size == len(stream), "size: expected %d got %d", size, n)

	// chunk it
	rd := bytes.NewReader(stream)
	chunker.Start(rd)

	buf := make([]byte, 10*miB)
	for {
		chunk, err := chunker.Next(buf)
		_ = chunk
		if errors.Cause(err) == io.EOF {
		}

	}
}

func TestPutStream(t *testing.T) {
	/*
		db, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}



		stream := ioutil.
		// chunker =
	*/
}
