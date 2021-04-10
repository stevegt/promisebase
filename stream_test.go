package pitbase

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
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
	db := setup(t)

	// setup
	buf1 := mkbuf("blob1value")
	blob1, err := db.PutBlob("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	blob2, err := db.PutBlob("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}
	buf3 := mkbuf("blob3value")
	blob3, err := db.PutBlob("sha256", buf3)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree1, err := db.PutTree("sha256", blob1, blob2)
	if err != nil {
		t.Fatal(err)
	}
	if tree1 == nil {
		t.Fatal("tree1 is nil")
	}
	tree2, err := db.PutTree("sha256", tree1, blob3)
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
	tassert(t, len(*stream1.RootNode.entries) > 0, "stream root tree has no entries: %#v", stream1.RootNode)

	// list leaf objs
	objects, err := stream1.Ls(false)
	if err != nil {
		t.Fatal(err)
	}
	expect := "blob/sha256/a13d00682410383f1003d6428d1028d6feb88f166e1266949bc4cd91725d532a\nblob/sha256/fc0d850d5930109e3eb3b799f067da93483fb80407e5d9dac56e17455be1dbaa\nblob/sha256/b4c9630d4f6928c0fb77a01984e5920a0a2be28382812c7ba31d60aa0abe652f\n"
	gotobjs := objs2str(objects)
	tassert(t, expect == gotobjs, "expected %v got %v", expect, gotobjs)

	// list all objs
	objects, err = stream1.Ls(true)
	if err != nil {
		t.Fatal(err)
	}
	expect = "tree/sha256/da0e74aa2d64168df0321877dd98a0e0c1f8b8f02a6f54211995623f518dd7f4\ntree/sha256/78e986b6bf7f04ec9fa1e14fb506f0cba967898183a1db602348ee65234c2c06\nblob/sha256/a13d00682410383f1003d6428d1028d6feb88f166e1266949bc4cd91725d532a\nblob/sha256/fc0d850d5930109e3eb3b799f067da93483fb80407e5d9dac56e17455be1dbaa\nblob/sha256/b4c9630d4f6928c0fb77a01984e5920a0a2be28382812c7ba31d60aa0abe652f\n"

	gotobjs = objs2str(objects)
	tassert(t, expect == gotobjs, "expected %v got %v", expect, gotobjs)

	// catstream
	gotbuf, err := stream1.Cat()
	if err != nil {
		t.Fatal(err)
	}
	expectbuf := mkbuf("blob1valueblob2valueblob3value")
	tassert(t, bytes.Compare(expectbuf, gotbuf) == 0, "expected %v got %v", string(expectbuf), string(gotbuf))

	// append
	blob4 := mkbuf("blob4value")
	stream1, err = stream1.AppendBlob("sha256", blob4)
	if err != nil {
		t.Fatal(err)
	}
	gotbuf, err = stream1.Cat()
	if err != nil {
		t.Fatal(err)
	}
	expectbuf = mkbuf("blob1valueblob2valueblob3valueblob4value")
	tassert(t, bytes.Compare(expectbuf, gotbuf) == 0, "expected %v got %v", string(expectbuf), string(gotbuf))

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
