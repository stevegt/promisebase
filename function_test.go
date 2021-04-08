package pitbase

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/stevegt/goadapt"
)

// test boolean condition
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
	}
}

var testDbDir string

func newdb(db *Db) *Db {

	if db == nil {
		db = &Db{}
	}

	// create Dir if needed
	// (if db.Dir is already set, then assume the caller has done mkdir)
	var err error
	if db.Dir == "" {
		db.Dir, err = ioutil.TempDir("", "pitbase")
		Ck(err)
	}
	db, err = db.Create()
	Ck(err)

	// XXX test other depths
	// db, err = Db{Depth: 4}.Create(dir)

	fmt.Println(db.Dir)
	testDbDir = db.Dir
	return db
}

func setup(t *testing.T) (db *Db) {
	db, err := Open(testDbDir)
	if err != nil {
		log.Printf("db err: %v", err)
		t.Fatal(err)
	}
	tassert(t, db != nil, "db is nil")
	return
}

func TestMain(m *testing.M) {

	newdb(nil)
	rc := m.Run()
	if rc == 0 {
		// XXX rmdb()
	}
	os.Exit(rc)
}

func nonMissingErr(err error) error {
	switch err.(type) {
	case *os.PathError:
		return nil
	case nil:
		return nil
	}
	return err
}

func mkbuf(s string) []byte {
	tmp := []byte(s)
	return tmp
}

func mkpath(t *testing.T, db *Db, class, s string) (path *Path) {
	path, err := pathFromString(db, class, "sha256", s)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestHash(t *testing.T) {
	val := mkbuf("somevalue")
	binhash, err := Hash("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	hexhash := bin2hex(binhash)
	expect := "70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342"
	tassert(t, expect == hexhash, "expected %q got %q", expect, hexhash)

	binhash, err = Hash("sha512", val)
	if err != nil {
		t.Fatal(err)
	}
	hexhash = bin2hex(binhash)
	expect = "8e77e71abe427ced1c93d883aeeddfa57ce39b787f229caaf176fdd71353f3466d340a2cdb5a219c429c53ad37f2f144c7ce01b985b6b33e397c4b8fd1433cc3"
	tassert(t, expect == hexhash, "expected %q got %q", expect, hexhash)

	binhash, err = Hash("foobar", val)
	if err == nil {
		t.Fatal("expected error, received none")
	}

	//expecterr := fmt.Errorf("not implemented: %s", "foobar")
	//binhash, err = Hash("foobar", val)
	//tassert(t, err == expecterr, "expected %q got %q", err, expecterr)
}

func TestBlob(t *testing.T) {
	db := setup(t)

	hash := "d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	canpath := "blob/sha256/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	relpath := "blob/sha256/d2c/71a/d2c71afc5848aa2a33ff08621217f24dab485077d95d788c5170995285a5d65d"
	path := Path{}.New(db, canpath)
	file, err := File{}.New(db, path)
	tassert(t, err == nil, "File.New err %v", err)
	b := Blob{}.New(db, file)

	// put something in the blob
	data := mkbuf("somedata")
	nwrite, err := b.Write(data)
	tassert(t, err == nil, "b.Write err %v", err)
	tassert(t, nwrite == len(data), "b.Write len expected %v, got %v", len(data), nwrite)

	// close writeable
	err = b.Close()
	tassert(t, err == nil, "b.Close() err %v", err)

	// re-open readable
	file, err = File{}.New(db, path)
	tassert(t, err == nil, "File.New err %v", err)
	b = Blob{}.New(db, file)
	tassert(t, err == nil, "OpenBlob err %v", err)

	// check size
	size, err := b.Size()
	tassert(t, err == nil, "Blob.Size() size %d err %v", size, err)
	// fmt.Printf("object %s is %d bytes\n", b.Path.Canon, size)

	// seek to a location
	nseek, err := b.Seek(2, 0)
	tassert(t, err == nil, "b.Seek err %v", err)
	tassert(t, nseek == int64(2), "b.Seek expected %v, got %v", 2, nseek)

	// check our current location
	ntell, err := b.Tell()
	tassert(t, err == nil, "b.Tell err %v", err)
	tassert(t, ntell == 2, "b.Tell expected %v, got %v", 2, ntell)

	// read from that location
	buf := make([]byte, 100)
	nread, err := b.Read(buf)
	// fmt.Printf("dsaf nread %#v buf %#v", nread, buf)
	tassert(t, err == nil, "b.Read err %v", err)
	tassert(t, nread == 6, "b.Read len expected %v, got %v", 6, nread)
	expect := mkbuf("medata")
	got := buf[:nread]
	tassert(t, bytes.Compare(expect, got) == 0, "b.Read expected %v, got %v", expect, got)

	// ensure we can't write to a read-only blob
	_, err = b.Write(data)
	tassert(t, err != nil, "b.Write to a read-only file should throw error")

	// test Object methods
	objectExample(t, b)

	abspath := b.Path.Abs
	tassert(t, len(abspath) > 11, "path len %v", len(abspath))
	// fmt.Printf("object path %s\n", abspath)

	gotrelpath := b.Path.Rel
	tassert(t, relpath == gotrelpath, "relpath '%v'", gotrelpath)

	class := b.Path.Class
	tassert(t, class == "blob", "class '%v'", class)

	algo := b.Path.Algo
	tassert(t, algo == "sha256", "algo '%v'", algo)

	gothash := b.Path.Hash
	tassert(t, gothash == hash, "hash '%v'", gothash)

	gotcanpath := b.Path.Canon
	tassert(t, canpath == gotcanpath, "canpath '%v'", gotcanpath)

}

// an example of how an Object might be used
func objectExample(t *testing.T, o Object) {

	abspath := o.GetPath().Abs
	tassert(t, len(abspath) > 0, "path len %v", len(abspath))
	// fmt.Printf("object path %s\n", abspath)

	size, err := o.Size()
	tassert(t, err == nil, "Blob.Size() size %d err %v", size, err)
	// fmt.Printf("object %s is %d bytes\n", o.GetPath().Canon, size)
}

func TestRm(t *testing.T) {
	db := setup(t)
	buf := mkbuf("somevalue")
	blob, err := db.PutBlob("sha256", buf)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(blob.Path)
	if err != nil {
		t.Fatal(err)
	}
	gotblob, err := db.GetBlob(blob.Path)
	if err == nil {
		t.Fatalf("blob not deleted: %#v", gotblob)
	}
}

func TestGetBlob(t *testing.T) {
	db := setup(t)
	val := mkbuf("somevalue")
	path, err := pathFromBuf(db, "blob", "sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotblob, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if path.Canon != gotblob.Path.Canon {
		t.Fatalf("expected path %s, got %s", path.Canon, gotblob.Path.Canon)
	}
	got, err := db.GetBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %q, got %q", string(val), string(got))
	}
}

func pathEqual(a, b *Path) bool {
	return a.Rel == b.Rel && a.Canon == b.Canon
}

func deepEqual(a, b interface{}) bool {
	// fmt.Printf("a:\n%s\nb:\n%s\n", pretty(a), pretty(b))
	return pretty(a) == pretty(b)
}

/*
func TestPath(t *testing.T) {
	db := setup(t)
	val := mkbuf("somevalue")
	path, err := pathFromBuf(db, "blob", "sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	expectabs := filepath.Join(db.Dir, "blob/sha256/70a/524/70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342")
	gotabs := path.Abs
	if expectabs != gotabs {
		t.Fatalf("expected %s, got %s", path.Abs, gotabs)
	}
}
*/

func TestGetGID(t *testing.T) {
	n := GetGID()
	if n == 0 {
		t.Fatalf("oh no n is 0")
	}
}

func TestVerify(t *testing.T) {
	db, err := Open("testdata")
	if err != nil {
		t.Fatal(err)
	}
	path := Path{}.New(db, "tree/sha256/22695d451d4f8383546f8cc3d3c93b78c4827f508ad682c620d02a78e58a3ab3")
	tree, err := db.GetTree(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, child := range *tree.entries {
		switch i {
		case 0:
			expect := "tree/sha256/606/1c8/6061c8eb4f00c1039c0922f1cfb73233b7353b371227fd0a5cd380104ba58a7b"
			tassert(t, expect == child.GetPath().Rel, "expected %v got %v", expect, child.GetPath().Rel)
		case 1:
			expect := "blob/sha256/32b/cc6/32bcc691cfa205d4a4be7f47cfca49253fd76cbdfd93124388b1824499cdb36b"
			tassert(t, expect == child.GetPath().Rel, "expected %q got %q", expect, child.GetPath().Rel)
		}
	}
	ok, err := tree.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "tree verify failed: %v", pretty(tree))
}

func TestTree(t *testing.T) {
	db := setup(t)
	// setup
	buf1 := mkbuf("blob1value")
	child1, err := db.PutBlob("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	child2, err := db.PutBlob("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree, err := db.PutTree("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if tree == nil {
		t.Fatal("tree is nil")
	}

	/*
		nodekey := db.KeyFromPath("node/sha256/cb4/678/cb46789e72baabd2f1b1bc7dc03f9588f2a36c1d38224f3a11fad7386cb9cbcf")
		if nodekey == nil {
			t.Fatal("nodekey is nil")
		}
		// t.Log(fmt.Sprintf("nodekey %#v node %#v", nodekey, node))
		tassert(t, keyEqual(nodekey, node.Key), "node key mismatch: expect %s got %s", nodekey, node.Key)
	*/

	ok, err := tree.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "tree verify failed: %v", tree)

	// get
	gottree, err := db.GetTree(tree.Path)
	if err != nil {
		t.Fatal(err)
	}
	// t.Log(fmt.Sprintf("node\n%q\ngotnode\n%q\n", node, gotnode))
	tassert(t, tree.Txt() == gottree.Txt(), "tree %v mismatch: expect %v got %v", tree.Path.Abs, tree.Txt(), gottree.Txt())
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

// XXX add chattr for failure test
func TestMkdir(t *testing.T) {
	err := mkdir("/etc/foobar")
	if err == nil {
		t.Fatal("expected error, got none")
	}
}

func pathFromString(db *Db, class, algo, s string) (path *Path, err error) {
	buf := []byte(s)
	return pathFromBuf(db, class, algo, buf)
}

func pathFromBuf(db *Db, class string, algo string, buf []byte) (path *Path, err error) {
	b := append([]byte(class+"\n"), buf...)
	binhash, err := Hash(algo, b)
	if err != nil {
		return
	}
	hash := bin2hex(binhash)
	path = Path{}.New(db, filepath.Join(class, algo, hash))
	return
}

var benchSize int

func Benchmark0PutBlob(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkbuf(asString(n))
		_, err = db.PutBlob("sha256", val)
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

func Benchmark2GetBlob(b *testing.B) {
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
		_, err = db.GetBlob(path)
		if err != nil {
			fmt.Printf("n: %d\n", n)
			b.Fatal(err)
		}
	}
}

func XXXBenchmarkPutBlobSame(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	val := mkbuf("foo")
	for n := 0; n < b.N; n++ {
		gotpath, err := db.PutBlob("sha256", val)
		_ = gotpath
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutGetBlob(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkbuf(asString(n))
		blob, err := db.PutBlob("sha256", val)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.GetBlob(blob.Path)
		if err != nil {
			//	fmt.Printf("n: %d\n", n)
			b.Fatal(err)
		}
	}
}

func objs2str(objects []Object) (out string) {
	for _, obj := range objects {
		line := string(obj.GetPath().Canon)
		line = strings.TrimSpace(line) + "\n"
		out += line
	}
	return
}

func asString(input interface{}) (out string) {
	out = fmt.Sprintf("%v", input)
	return
}

func shell(path string, args ...string) (out []byte, err error) {
	cmd := exec.Command(path, args...)
	out, err = cmd.CombinedOutput()
	return
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
