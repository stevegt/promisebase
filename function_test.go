package pitbase

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hlubek/readercomp"
)

// test boolean condition
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
		/*
			XXX The following isn't needed with t.Helper(), and doesn't
			work anyway because t.Logf always prepends the wrong file:line
			anyway, but keeping here for a while in case it's useful
			elsewhere.

			// prepend caller's file:line info
			msg := fmt.Sprintf(txt, args...)
			_, file, line, ok := runtime.Caller(1)
			if ok {
				dir, _ := os.Getwd()
				file = strings.TrimPrefix(file, dir+"/")
				// chunker_test.go:74: sadf
				t.Fatalf("%s:%d: %s", file, line, msg)
			} else {
				debug.PrintStack()
				t.Fatalf("======> %s", msg)
			}
		*/
	}
}

var testDbDir string

func newdb(t *testing.T, db *Db) *Db {
	var err error

	if db == nil {
		db = &Db{}
	}

	// create Dir if needed
	// (if Dir is passed in, then assume the caller has done mkdir)
	if db.Dir == "" {
		db.Dir, err = ioutil.TempDir("", "pitbase")
		if err != nil {
			t.Fatal(err)
		}
	}

	db, err = (*db).Create()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, db != nil, "db is nil")

	fmt.Println(db.Dir)
	testDbDir = db.Dir // XXX hackkk
	return db
}

func setup(t *testing.T) (db *Db) {
	db, err := Open(testDbDir)
	if err != nil {
		log.Printf("db err: %v", err)
		t.Fatal(err)
	}
	tassert(t, db != nil, "db is nil")
	// XXX test other depths
	// db, err = Db{Dir: dir, Depth: 4}.Create()
	// fmt.Println(dir)
	return
}

func TestExist(t *testing.T) {
	db := newdb(t, nil)
	// log.Printf("db: %v", db)
	db, err := Open(db.Dir)
	if err != nil {
		t.Fatal(err)
	}
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

func mkblob(s string) *[]byte {
	tmp := []byte(s)
	return &tmp
}

func mkkey(t *testing.T, db *Db, s string) (key *Key) {
	key, err := db.KeyFromString("sha256", s)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestHash(t *testing.T) {
	val := mkblob("somevalue")
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

func TestPut(t *testing.T) {
	db := setup(t)
	key := mkkey(t, db, "somekey")
	val := mkblob("somevalue")
	err := db.put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ioutil.ReadFile(db.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(*val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(*val), string(got))
	}
}

func TestGet(t *testing.T) {
	db := setup(t)
	key := mkkey(t, db, "somekey")
	val := mkblob("somevalue")
	err := db.put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.GetBlob(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(*val, *got) != 0 {
		t.Fatalf("expected %s, got %s", string(*val), string(*got))
	}
}

func TestRm(t *testing.T) {
	db := setup(t)
	key := mkkey(t, db, "somekey")
	val := mkblob("somevalue")
	err := db.put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.GetBlob(key)
	if err == nil {
		t.Fatalf("key not deleted: %s", key.Path())
	}
}

func TestPutBlob(t *testing.T) {
	db := setup(t)
	val := mkblob("somevalue")
	key, err := db.KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if key.Canon() != gotkey.Canon() {
		t.Fatalf("expected key %s, got %s", key.Canon(), gotkey.Canon())
	}
	got, err := ioutil.ReadFile(db.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(*val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(*val), string(got))
	}
}

func TestGetBlob(t *testing.T) {
	db := setup(t)
	val := mkblob("somevalue")
	key, err := db.KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if key.Canon() != gotkey.Canon() {
		t.Fatalf("expected key %s, got %s", key.Canon(), gotkey.Canon())
	}
	got, err := db.GetBlob(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(*val, *got) != 0 {
		t.Fatalf("expected %s, got %s", string(*val), string(*got))
	}
}

func keyEqual(a, b *Key) bool {
	return a.Path() == b.Path() && a.Canon() == b.Canon()
}

// XXX should use reflect.DeepEqual()
func deepEqual(a, b interface{}) bool {
	// fmt.Printf("a:\n%s\nb:\n%s\n", pretty(a), pretty(b))
	return pretty(a) == pretty(b)
}

func TestPath(t *testing.T) {
	db := setup(t)
	val := mkblob("somevalue")
	key, err := db.KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(db.Dir, "blob/sha256/70a/524/70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342")
	gotpath := db.Path(key)
	if path != gotpath {
		t.Fatalf("expected %s, got %s", path, gotpath)
	}
}

// XXX test db.GetRef

// XXX redefine "key" to mean the path to a blob, tree, or ref
// XXX change ref format accordingly
// XXX change key struct accordingly

// TestKey makes sure we have a Key struct and that the KeyFromPath
// function works.
func TestKey(t *testing.T) {
	db := setup(t)
	var key *Key
	val := mkblob("somevalue")
	algo := "sha256"
	d := sha256.Sum256(*val)
	bin := make([]byte, len(d))
	copy(bin[:], d[0:len(d)])
	hex := fmt.Sprintf("%x", bin)
	key, err := db.KeyFromBlob(algo, val)
	if err != nil {
		t.Fatal(err)
	}
	if algo != key.Algo {
		t.Fatalf("expected %s, got %s", algo, key.Algo)
	}
	expect := filepath.Join("blob/sha256", hex[0:3], hex[3:6], hex)
	if expect != key.Path() {
		t.Fatalf("expected %s, got %s", expect, key.Path())
	}
}

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
	node, err := db.GetNode(db.KeyFromPath("node/sha256/4caca571948628fa4badbe6c42790446affe3a9b13d9a92fee4862255b34afe2"))
	if err != nil {
		t.Fatal(err)
	}
	children, err := node.ChildNodes()
	if err != nil {
		return
	}
	for i, child := range children {
		switch i {
		case 0:
			expect := "node/sha256/1e0/9f2/1e09f25b6b42842798bc74ee930d7d0e6b712512087e6b3b39f15cc10a82ba18"
			tassert(t, expect == child.Key.Path(), "expected %v got %v", expect, child.Key.Path())
		case 1:
			expect := "blob/sha256/534/d05/534d059533cc6a29b0e8747334c6af08619b1b59e6727f50a8094c90f6393282"
			tassert(t, expect == child.Key.Path(), "expected %q got %q", expect, child.Key.Path())
		}
	}
	ok, err := node.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "node verify failed: %v", pretty(node))
}

func TestNode(t *testing.T) {
	db := setup(t)
	// setup
	blob1 := mkblob("blob1value")
	key1, err := db.PutBlob("sha256", blob1)
	if err != nil {
		t.Fatal(err)
	}
	child1 := &Node{Db: db, Key: key1, Label: ""}
	blob2 := mkblob("blob2value")
	key2, err := db.PutBlob("sha256", blob2)
	if err != nil {
		t.Fatal(err)
	}
	child2 := &Node{Db: db, Key: key2, Label: ""}
	// fmt.Println(child1.Key.String(), child2.Key.String())

	// put
	node, err := db.PutNode("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatal("node is nil")
	}
	nodekey := db.KeyFromPath("node/sha256/cb4/678/cb46789e72baabd2f1b1bc7dc03f9588f2a36c1d38224f3a11fad7386cb9cbcf")
	if nodekey == nil {
		t.Fatal("nodekey is nil")
	}
	// t.Log(fmt.Sprintf("nodekey %#v node %#v", nodekey, node))
	tassert(t, keyEqual(nodekey, node.Key), "node key mismatch: expect %s got %s", nodekey, node.Key)
	ok, err := node.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "node verify failed: %v", node)

	// get
	gotnode, err := db.GetNode(node.Key)
	if err != nil {
		t.Fatal(err)
	}
	// t.Log(fmt.Sprintf("node\n%q\ngotnode\n%q\n", node, gotnode))
	tassert(t, reflect.DeepEqual(node, gotnode), "node mismatch: expect %v got %v", node, gotnode)
}

func TestWorld(t *testing.T) {
	db := setup(t)

	// setup
	blob1 := mkblob("blob1value")
	key1, err := db.PutBlob("sha256", blob1)
	if err != nil {
		t.Fatal(err)
	}
	child1 := &Node{Db: db, Key: key1, Label: "blob1label"}
	blob2 := mkblob("blob2value")
	key2, err := db.PutBlob("sha256", blob2)
	if err != nil {
		t.Fatal(err)
	}
	child2 := &Node{Db: db, Key: key2, Label: "blob2label"}
	blob3 := mkblob("blob3value")
	key3, err := db.PutBlob("sha256", blob3)
	if err != nil {
		t.Fatal(err)
	}
	child3 := &Node{Db: db, Key: key3, Label: "blob3label"}

	// put
	node1, err := db.PutNode("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if node1 == nil {
		t.Fatal("node1 is nil")
	}
	node1.Label = "node1label"
	node2, err := db.PutNode("sha256", node1, child3)
	if err != nil {
		t.Fatal(err)
	}
	if node2 == nil {
		t.Fatal("node2 is nil")
	}

	world1, err := db.PutWorld(node2.Key, "world1")
	if err != nil {
		t.Fatal(err)
	}

	gotworld, err := db.GetWorld("world1")
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, reflect.DeepEqual(world1, gotworld), "world mismatch: expect %v got %v", pretty(world1), pretty(gotworld))

	// list leaf nodes
	nodes, err := world1.Ls(false)
	if err != nil {
		t.Fatal(err)
	}
	expect := "blob/sha256/1499559e764b35ac77e76e8886ef237b3649d12014566034198661dc7db77379 blob1label\nblob/sha256/48618376a9fcd7ec1147a90520a003d72ffa169b855f0877fd42b722538867f0 blob2label\nblob/sha256/ea5a02427e3ca466defa703ed3055a86cd3ae9ee6598fd1bf7e0219a6c490a7f blob3label\n"
	gotnodes := nodes2str(nodes)
	tassert(t, expect == gotnodes, "expected %v got %v", expect, gotnodes)

	// list all nodes
	nodes, err = world1.Ls(true)
	if err != nil {
		t.Fatal(err)
	}
	expect = "node/sha256/fc489024469b5e9acfa85e4c117e9bef69552720ef5154edaaa6123bad98ec56 world1\nnode/sha256/9ae11d65603f394a9dcb6a54166dde24ebdd9479c480ad8b8e5b700f3a1cde4b node1label\nblob/sha256/1499559e764b35ac77e76e8886ef237b3649d12014566034198661dc7db77379 blob1label\nblob/sha256/48618376a9fcd7ec1147a90520a003d72ffa169b855f0877fd42b722538867f0 blob2label\nblob/sha256/ea5a02427e3ca466defa703ed3055a86cd3ae9ee6598fd1bf7e0219a6c490a7f blob3label\n"

	gotnodes = nodes2str(nodes)
	tassert(t, expect == gotnodes, "expected %v got %v", expect, gotnodes)

	// catworld
	gotbuf, err := world1.Cat()
	if err != nil {
		t.Fatal(err)
	}
	expectbuf := mkblob("blob1valueblob2valueblob3value")
	tassert(t, bytes.Compare(*expectbuf, *gotbuf) == 0, "expected %v got %v", string(*expectbuf), string(*gotbuf))

	// append
	blob4 := mkblob("blob4value")
	world1, err = world1.AppendBlob("sha256", blob4)
	if err != nil {
		t.Fatal(err)
	}
	gotbuf, err = world1.Cat()
	if err != nil {
		t.Fatal(err)
	}
	expectbuf = mkblob("blob1valueblob2valueblob3valueblob4value")
	tassert(t, bytes.Compare(*expectbuf, *gotbuf) == 0, "expected %v got %v", string(*expectbuf), string(*gotbuf))

}

// XXX add chattr for failure test
func TestMkdir(t *testing.T) {
	err := mkdir("/etc/foobar")
	if err == nil {
		t.Fatal("expected error, got none")
	}
}

var benchSize int

func Benchmark0PutBlob(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkblob(asString(n))
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
		key, err := db.KeyFromString("sha256", asString(n))
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.GetBlob(key)
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
	val := mkblob("foo")
	for n := 0; n < b.N; n++ {
		gotkey, err := db.PutBlob("sha256", val)
		_ = gotkey
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
		val := mkblob(asString(n))
		key, err := db.PutBlob("sha256", val)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.GetBlob(key)
		if err != nil {
			//	fmt.Printf("n: %d\n", n)
			b.Fatal(err)
		}
	}
}

func nodes2str(nodes []*Node) (out string) {
	for _, node := range nodes {
		line := strings.Join([]string{node.Key.Canon(), node.Label}, " ")
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
	size := 10 * miB
	stream := RandStream(10 * miB)
	buf, err := ioutil.ReadAll(stream)
	tassert(t, err == nil, "ReadAll: %v", err)
	tassert(t, size == len(buf), "size: expected %d got %d", size, len(buf))
}
