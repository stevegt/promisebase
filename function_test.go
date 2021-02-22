package pitbase

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
)

const dir = "var"

// test boolean condition
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Errorf(txt, args...)
	}
}

func TestNotExist(t *testing.T) {
	os.RemoveAll(dir)
	_, err := Open(dir)
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

func mkkey(t *testing.T, s string) (key *Key) {
	key, err := KeyFromString("sha256", s)
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
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := mkkey(t, "somekey")
	val := mkblob("somevalue")
	err = db.put(key, val)
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
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := mkkey(t, "somekey")
	val := mkblob("somevalue")
	err = db.put(key, val)
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
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := mkkey(t, "somekey")
	val := mkblob("somevalue")
	err = db.put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.GetBlob(key)
	if err == nil {
		t.Fatalf("key not deleted: %s", key)
	}
}

func TestPutBlob(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	val := mkblob("somevalue")
	key, err := KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if key.String() != gotkey.String() {
		t.Fatalf("expected key %s, got %s", key, gotkey)
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
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	val := mkblob("somevalue")
	key, err := KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if key.String() != gotkey.String() {
		t.Fatalf("expected key %s, got %s", key, gotkey)
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
	return a.String() == b.String()
}

// XXX should use reflect.DeepEqual()
func deepEqual(a, b interface{}) bool {
	// fmt.Printf("a:\n%s\nb:\n%s\n", pretty(a), pretty(b))
	return pretty(a) == pretty(b)
}

func TestPath(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	val := mkblob("somevalue")
	key, err := KeyFromBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	path := "var/blob/sha256/70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342"
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
	var key *Key
	val := mkblob("somevalue")
	algo := "sha256"
	d := sha256.Sum256(*val)
	bin := make([]byte, len(d))
	copy(bin[:], d[0:len(d)])
	hex := fmt.Sprintf("%x", bin)
	key, err := KeyFromBlob(algo, val)
	if err != nil {
		t.Fatal(err)
	}
	if algo != key.Algo {
		t.Fatalf("expected %s, got %s", algo, key.Algo)
	}
	expect := fmt.Sprintf("blob/sha256/%s", hex)
	if expect != key.String() {
		t.Fatalf("expected %s, got %s", expect, key.String())
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
	node, err := db.GetNode(KeyFromPath("node/sha256/14fe3864a6848b8b4b61e6b2c39fae59491c6e017e268f21ce23f1f8b07f736d"))
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
			expect := "node/sha256/563dcb27d5d8ae1c579ea8b2af89db2d125ade16d95efde13952821230d28e46"
			tassert(t, expect == child.Key.String(), "expected %v got %v", expect, child.Key.String())
		case 1:
			expect := "blob/sha256/534d059533cc6a29b0e8747334c6af08619b1b59e6727f50a8094c90f6393282"
			tassert(t, expect == child.Key.String(), "expected %q got %q", expect, child.Key.String())
		}
	}
	// sha256sum testdata/node/sha256/00e2a12b4ae802c79344fa05fd49ff63c1335fdd5bc308dab69a6d6b5b5884b2
	//expect := "00e2a12b4ae802c79344fa05fd49ff63c1335fdd5bc308dab69a6d6b5b5884b2"
	ok, err := node.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "node verify failed: %v", pretty(node))
}

func TestNode(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

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
	nodekey := KeyFromPath("node/sha256/cb46789e72baabd2f1b1bc7dc03f9588f2a36c1d38224f3a11fad7386cb9cbcf")
	if nodekey == nil {
		t.Fatal("nodekey is nil")
	}

	// put
	node, err := db.PutNode("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatal("node is nil")
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
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

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
	_, err = world1.AppendBlock("sha256", blob4)
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

func BenchmarkPutBlob(b *testing.B) {
	db, err := Open("/tmp/bench/")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		val := mkblob(string(n))
		gotkey, err := db.PutBlob("sha256", val)
		_ = gotkey
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutBlobSame(b *testing.B) {
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
func nodes2str(nodes []*Node) (out string) {
	for _, node := range nodes {
		line := strings.Join([]string{node.Key.String(), node.Label}, " ")
		line = strings.TrimSpace(line) + "\n"
		out += line
	}
	return
}

// XXX todo items moved to ./ROADMAP.md
