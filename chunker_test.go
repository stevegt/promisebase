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

// XXX replace testStream with randStream
type testStream struct {
	Data    []byte
	nextPos int64
}

func (s *testStream) Read(p []byte) (n int, err error) {
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

func genstream(t *testing.T, size int) (stream *testStream) {
	stream = &testStream{}
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

	buf := make([]byte, 9*miB) // XXX we need to understand buffer size
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

func TestPutStreamBig(t *testing.T) {
	stream := genstream(t, 100*miB)
	db := setup(t, nil)
	testPutStream(t, db, stream)
}

func TestPutStreamSmall(t *testing.T) {
	stream := &testStream{Data: mkbuf("apple bob carol dave echo foxtrot golf hotel india juliet kilo lima mike november oscar pear something ")}
	db := setup(t, &Db{MinSize: 10, MaxSize: 20})
	testPutStream(t, db, stream)
}

func testPutStream(t *testing.T, db *Db, stream *testStream) {

	tree, err := db.PutStream("sha256", stream)
	tassert(t, err == nil, "PutStream(): %v", err)
	tassert(t, tree != nil, "PutStream() tree is nil")

	// fmt.Printf("root %s\n", tree.Path.Abs)

	gotbuf, err := tree.Cat()
	tassert(t, err == nil, "tree.Cat(): %v", err)

	if len(stream.Data) < 200 && len(gotbuf) < 200 {
		tassert(t, bytes.Compare(stream.Data, gotbuf) == 0, "expected %v\n=================\ngot %v", string(stream.Data), string(gotbuf))
	}
	tassert(t, len(stream.Data) == len(gotbuf), "size: expected %d got %d", len(stream.Data), len(gotbuf))
	tassert(t, bytes.Compare(stream.Data, gotbuf) == 0, "stream vs. gotbuf mismatch")

}

/*
func TestChunkerWrite(t *testing.T) {
	// setup
	// polynomial was randomly generated from a call to chunker.Init()
	chunker, err := Rabin{Poly: 0x25d92e975e1aa3}.Init()
	tassert(t, err == nil, "%v", err)
	// fmt.Printf("%T %#v\n", chunker.Poly, chunker.Poly)
	tassert(t, chunker.Poly > 0, "polynomial is %v", chunker.Poly)

	// create some data
	size := int64(100 * miB)
	src := RandStream(size)
	cmp := RandStream(size)

	var gotstream []byte

	// start the chunker goroutine
	buf := make([]byte, 9*miB) // XXX try other buffer sizes
	// XXX the following is better, but probably needs to be combined
	// with Init()
	writer, chunkchan := chunker.Open(buf)

	// read chunks from channel
	for chunk := range chunkchan {
		if errors.Cause(chunk.Err) == io.EOF {
			fmt.Println("EOF")
			break
		}
		fmt.Printf(".")
		gotstream = append(gotstream, chunk.Data...)
	}

	// compare src with cmp
	ok, err := readercomp.Equal(src, cmp, 4096)
	tassert(t, err == nil, "readercomp.Equal: %v", err)
	tassert(t, ok, "stream mismatch")

}
*/
