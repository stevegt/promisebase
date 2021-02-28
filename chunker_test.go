package pitbase

import (
	"bytes"
	"fmt"
	"io"
	"math"
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
	size := 100 * miB
	stream := genstream(t, size)

	// chunk it
	chunker.Start(stream)

	buf := make([]byte, 1*miB)
	var gotstream []byte
	for {
		chunk, err := chunker.Next(buf)
		if errors.Cause(err) == io.EOF {
			fmt.Println("EOF")
			break
		}
		fmt.Printf(".")
		expect := stream.Data[chunk.Start : chunk.Start+chunk.Length]
		tassert(t, bytes.Compare(expect, chunk.Data) == 0, "chunk: expected %v got %v", expect, chunk.Data)
		gotstream = append(gotstream, chunk.Data...)
	}
	gotsize := len(gotstream)
	tassert(t, size == int(gotsize), "size: expected %d got %d", size, gotsize)
	tassert(t, bytes.Compare(stream.Data, gotstream) == 0, "chunk: stream vs. gotstream mismatch")
}

type Stream struct {
	Data    []byte
	nextPos int64
}

func (s *Stream) Read(p []byte) (n int, err error) {
	bs := 4096 // XXX try different sizes
	start := s.nextPos
	if start >= int64(len(s.Data)) {
		err = io.EOF
		return
	}
	end := int64(math.Min(float64(start)+float64(bs), float64(len(s.Data))))
	n = copy(p, s.Data[start:end])
	s.nextPos = end
	return
}

func genstream(t *testing.T, size int) (stream *Stream) {
	stream = &Stream{}
	stream.Data = make([]byte, size)
	rand.Seed(42)
	// write random data into stream.Data
	n, err := rand.Read(stream.Data)
	tassert(t, err == nil, "rand.Read(): %v", err)
	tassert(t, size == n, "size: expected %d got %d", size, n)
	tassert(t, size == len(stream.Data), "size: expected %d got %d", size, n)
	// t.Fatal("sadf")
	return
}

func TestPutStream(t *testing.T) {
	stream := genstream(t, 100*miB)
	db := newdb(t)

	node, err := db.PutStream(stream)
	tassert(t, err == nil, "PutStream(): %v", err)
	tassert(t, node != nil, "PutStream() node is nil")

	gotbuf, err := node.Cat()
	tassert(t, err == nil, "node.Cat(): %v", err)

	tassert(t, bytes.Compare(stream.Data, *gotbuf) == 0, "chunk: stream vs. gotbuf mismatch")

}
