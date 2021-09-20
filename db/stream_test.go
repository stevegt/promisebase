package db

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/hlubek/readercomp"
)

// randStream supports the io.Reader interface -- see the RandStream
// function for usage.
type randStream struct {
	Size    int64
	nextPos int64
}

func (s *randStream) Read(p []byte) (n int, err error) {
	start := s.nextPos
	if start >= s.Size {
		err = io.EOF
		return
	}
	end := start + int64(len(p))
	if end > s.Size {
		// We need to limit the total bytes read from the stream so
		// that we don't return more than Size.  There may be a better
		// way of doing this, but in the meantime, on the last Read(),
		// we'll create a smaller buffer than p, write into that, and
		// then copy to p.
		buf := make([]byte, s.Size-start)
		_, err = rand.Read(buf)
		if err != nil {
			return
		}
		n = copy(p, buf)
	} else {
		n, err = rand.Read(p)
	}
	s.nextPos += int64(n)
	return
}

func (rs *randStream) Rewind() error {
	*rs = randStream{Size: rs.Size}
	rand.Seed(42)
	return nil
}

// RandStream supports the io.Reader interface.  It returns a stream
// that will produce `size` bytes of random data before EOF.
func RandStream(size int64) (stream *randStream) {
	stream = &randStream{Size: size}
	rand.Seed(42)
	return
}

func TestRandStream(t *testing.T) {
	size := int64(10 * miB)
	stream := RandStream(size)
	buf, err := ioutil.ReadAll(stream)
	tassert(t, err == nil, "ReadAll: %v", err)
	tassert(t, size == int64(len(buf)), "size: expected %d got %d", size, len(buf))
}

func TestTreeStream(t *testing.T) {
	db := setup(t, nil)

	// setup
	buf1 := mkbuf("blob1value")
	block1, err := db.PutBlock("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	block2, err := db.PutBlock("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}
	buf3 := mkbuf("blob3value")
	block3, err := db.PutBlock("sha256", buf3)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree1, err := db.PutTree("sha256", block1, block2)
	if err != nil {
		t.Fatal(err)
	}
	if tree1 == nil {
		t.Fatal("tree1 is nil")
	}
	tree2, err := db.PutTree("sha256", tree1, block3)
	if err != nil {
		t.Fatal(err)
	}
	if tree2 == nil {
		t.Fatal("tree2 is nil")
	}

	stream1, err := tree2.LinkStream("stream1")
	if err != nil {
		t.Fatal(err)
	}

	gotstream, err := db.OpenStream("stream1")
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, stream1.RootNode.Path.Abs == gotstream.RootNode.Path.Abs, "stream mismatch: expect %v got %v", pretty(stream1), pretty(gotstream))
	entries, err := stream1.RootNode.Entries()
	tassert(t, err == nil, "%#v", err)
	tassert(t, len(entries) > 0, "stream root tree has no entries: %#v", stream1.RootNode)

	// list leaf objs
	objects, err := stream1.Ls(false)
	if err != nil {
		t.Fatal(err)
	}
	expect := "block/sha256/c0fbf60ef9d67478b8d7ba518f911032716f019e5feaba2aac0f899e88dd99fe\nblock/sha256/bb842be9895fe25f7fb23abf74d7df9647fa8cd1123f6d36300fe1e0e1350056\nblock/sha256/118177df5d4ed72e06e29ac78d0a177df7217483420f2d5c2e6cf75a29eb9f00\n"
	gotobjs := objs2str(objects)
	tassert(t, expect == gotobjs, "expected %v got %v", expect, gotobjs)

	// list all objs
	objects, err = stream1.Ls(true)
	if err != nil {
		t.Fatal(err)
	}
	expect = "tree/sha256/c048444880a1f0f99d846551532de669d3682c2bb9fbee0c91e6851ff609601f\ntree/sha256/c89a57f991a863f3dfe665a0305c432e1c13c19df7803bc8cbb5eb09822ce55c\nblock/sha256/c0fbf60ef9d67478b8d7ba518f911032716f019e5feaba2aac0f899e88dd99fe\nblock/sha256/bb842be9895fe25f7fb23abf74d7df9647fa8cd1123f6d36300fe1e0e1350056\nblock/sha256/118177df5d4ed72e06e29ac78d0a177df7217483420f2d5c2e6cf75a29eb9f00\n"

	gotobjs = objs2str(objects)
	tassert(t, expect == gotobjs, "expected %v got %v", expect, gotobjs)

	// catstream
	/*
		gotbuf, err := stream1.Cat()
		if err != nil {
			t.Fatal(err)
		}
		expectbuf := mkbuf("blob1valueblob2valueblob3value")
		tassert(t, bytes.Compare(expectbuf, gotbuf) == 0, "expected %v got %v", string(expectbuf), string(gotbuf))
	*/
	// append
	block4 := mkbuf("blob4value")
	stream1, err = stream1.AppendBlock("sha256", block4)
	if err != nil {
		t.Fatal(err)
	}
	/*
		gotbuf, err = stream1.Cat()
		if err != nil {
			t.Fatal(err)
		}
		expectbuf := mkbuf("blob1valueblob2valueblob3valueblob4value")
		tassert(t, bytes.Compare(expectbuf, gotbuf) == 0, "expected %v got %v", string(expectbuf), string(gotbuf))
	*/
	expectbuf := mkbuf("blob1valueblob2valueblob3valueblob4value")
	expectrd := bytes.NewReader(expectbuf)
	stream1.Rewind()
	tassert(t, err == nil, "rewind: %v", err)
	ok, err := readercomp.Equal(expectrd, stream1, 4096) // XXX try different sizes
	tassert(t, err == nil, "readercomp.Equal: %v", err)
	tassert(t, ok, "stream mismatch")

}

/*
func TestStream(t *testing.T) {
	db := setup(t)

	// open a stream
	stream := Stream{Db: db, Algo: "sha256"}.Init()
	_ = stream

	// get random data
	randstream := RandStream(10 * miB)

	// copy random data into db
	n, err := io.Copy(stream, randstream)
	tassert(t, err == nil, "io.Copy: %v", err)
	tassert(t, n == 10*miB, "n: expected %v got %v", 10*miB, n)

	// rewind db stream
	n, err = stream.Seek(0, 0)
	tassert(t, err == nil, "stream.Seek: %v", err)
	tassert(t, n == 0, "n: expected 0 got %v", n)

	// rewind random stream
	// (RandStream always produces the same data)
	randstream = RandStream(10 * miB)

	// compare the two
	ok, err := readercomp.Equal(stream, randstream, 4096)
	tassert(t, err == nil, "readercomp.Equal: %v", err)
	tassert(t, ok, "stream mismatch")

	// stream.Close() ?

}
*/
