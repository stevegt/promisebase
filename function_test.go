package pitbase

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime/debug"
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

/*
func TestPut(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ioutil.ReadFile(db.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

func TestGet(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

func TestRm(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.Put(key, val)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Get(key)
	if err == nil {
		t.Fatalf("key not deleted: %s", key)
	}
}
func TestOpenKey(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key, err := KeyFromString("sha256", "somekey")
	if err != nil {
		t.Fatal(err)
	}
	inode, err := db.openKey(key, os.O_WRONLY|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}
	err = inode.ExLock()
	if err != nil {
		t.Fatal(err)
	}
	err = inode.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}
*/

/*
func TestConcurrent(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// key := []byte("somekey")
	valA := mkblob("valueA")
	valB := mkblob("valueB")
	worldA := &World{Db: db, Name: "worldA"}
	worldB := &World{Db: db, Name: "worldB"}
	doneA := make(chan bool)
	doneB := make(chan bool)

	// have both A and B do concurrent reads and writes -- this is in
	// different worlds, so there should be no collisions
	iterations := 2000
	go iterate(t, worldA, iterations, doneA, valA, valB)
	go iterate(t, worldB, iterations, doneB, valB, valA)

	<-doneA
	<-doneB
}

func iterate(t *testing.T, world *World, iterations int, done chan bool, myblob, otherblob *[]byte) {
	for i := 0; i < iterations; i++ {
		// store a blob
		key, err := db.putBlob("sha256", myblob)
		if err != nil {
			t.Fatal(err)
		}
		// start a transaction so we're isolated
		tx, err := db.()
		if err != nil {
			t.Fatal(err)
		}
		// inside transaction 1
		// try to create a conflict by putting the blob's key in the
		// same ref that the other goroutine is using
		err = tx.PutRef(key, "iterate")
		if err != nil {
			t.Fatal(err)
		}
		// get the ref
		gotkey, err := tx.GetRef("iterate")
		if err != nil {
			t.Fatal(err)
		}
		if key.Algo != "sha256" {
			t.Fatalf("expected 'sha256', got '%s'", key.Algo)
		}
		// get the blob
		gotblob, err := db.GetBlob(gotkey)
		// compare the blob we put with the one we got
		if bytes.Compare(*myblob, *gotblob) != 0 {
			t.Fatalf("expected %s, got %s", string(*myblob), string(*gotblob))
		}
		// XXX deal with the case of removing a blob inside a
		// transaction -- do we use gc, or do we replay a log?
		// XXX if we support delete, then how do we ensure WORM?
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		// end transaction 1

		// start transaction 2
		// new transaction should pick up whatever is in db now
		tx, err = db.StartTransaction()
		if err != nil {
			// XXX  A  call  to  flock()  may block if an incompatible lock is held by another process.
			// To make a nonblocking request, include LOCK_NB (by ORing) with any of the above operations.
			// syscall.Flock(int(db.locknode.fd), syscall.LOCK_EX | syscall.LOCK_NB)
			t.Fatal(err)
		}
		// get the ref
		gotkey, err = tx.GetRef("iterate")
		if err != nil {
			t.Fatal(err)
		}
		if key.Algo != "sha256" {
			t.Fatalf("expected 'sha256', got '%s'", key.Algo)
		}
		// get the blob
		gotblob, err = db.GetBlob(gotkey)
		// compare the blob we put with the one we got
		// the result must be either A or B, otherwise it's
		// corrupt due to something wrong in Commit()
		if bytes.Compare(*myblob, *gotblob) != 0 && bytes.Compare(*otherblob, *gotblob) != 0 {
			t.Fatalf("expected %s or %s, got %s", string(*myblob), string(*otherblob), string(*gotblob))
		}
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		// end transaction 2

	}
	done <- true
}
*/

func nonMissingErr(err error) error {
	switch err.(type) {
	case *os.PathError:
		return nil
	case nil:
		return nil
	}
	return err
}

func TestDbLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	locknode, err := db.ExLock()
	if err != nil {
		t.Fatal(err)
	}
	err = locknode.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	locknode, err = db.ShLock()
	if err != nil {
		t.Fatal(err)
	}
	err = locknode.Unlock()
	if err != nil {
		t.Fatal(err)
	}

}

// XXX once all of the following tests are working, delete all of the
// locking code and rename the *NoLock functions

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

/*
func TestPutRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	world := &World{Db: db, Name: "world1"}

	ref := "someref"
	key, err := KeyFromBlob("sha256", mkblob("somevalue"))
	if err != nil {
		t.Fatal(err)
	}
	err = world.PutRef(key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/world/world1/someref")
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf)
	expect := key.String()
	if expect != got {
		t.Fatalf("expected %s, got %s", expect, got)
	}
	gotkey, err := world.GetRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	if gotkey.Algo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", gotkey.Algo)
	}
	if !keyEqual(key, gotkey) {
		t.Fatalf("expected '%s', got '%s'", key, gotkey)
	}
}
*/

func keyEqual(a, b *Key) bool {
	return a.String() == b.String()
}

// XXX should use reflect.DeepEqual()
func deepEqual(a, b interface{}) bool {
	// fmt.Printf("a:\n%s\nb:\n%s\n", pretty(a), pretty(b))
	return pretty(a) == pretty(b)
}

/*
func TestSubRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	world := &World{Db: db, Name: "world1"}
	ref := "somedir/someref"
	key, err := KeyFromBlob("sha256", mkblob("somevalue"))
	if err != nil {
		t.Fatal(err)
	}
	err = world.PutRef(key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/world/world1/somedir/someref")
	if err != nil {
		t.Fatal(err)
	}
	expect := key.String()
	got := string(buf)
	if expect != got {
		t.Fatalf("expected %s, got %s", expect, got)
	}
	gotkey, err := world.GetRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	if gotkey.Algo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", gotkey.Algo)
	}
	if !keyEqual(key, gotkey) {
		t.Fatalf("expected '%s', got '%s'", key.String(), gotkey)
	}
}
*/

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

/*
func XXXTestRefPath(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ref := "someref"
	path := "var/refs/someref"
	gotpath := db.RefPath(ref)
	if path != gotpath {
		t.Fatalf("expected %s, got %s", path, gotpath)
	}
}
*/
/*
func TestCloneWorld(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	oldworld := &World{Db: db, Name: "world0"}

	// create blob and ref in oldworld
	blob := mkblob(fmt.Sprintf("value.outside"))
	blobkey, err := db.PutBlob("sha256", blob)
	if err != nil {
		t.Fatal(err)
	}
	outref := "ref.outside"
	err = oldworld.PutRef(blobkey, outref)
	if err != nil {
		t.Fatal(err)
	}

	newworld, err := db.CloneWorld(oldworld, "world1")
	if err != nil {
		t.Fatal(err)
	}

	// verify old ref is hardlinked into newworld
	if !exists(newworld.Db.Dir, outref) {
		t.Fatalf("missing %s/%s", newworld.Db.Dir, outref)
	}

	// create ref in our world
	inref := "ref.inside"
	err = newworld.PutRef(blobkey, inref)
	if err != nil {
		t.Fatal(err)
	}

	// verify new ref is in our world
	if !exists(newworld.Db.Dir, inref) {
		t.Fatalf("missing %s/%s", newworld.Dir(), inref)
	}
	// verify new ref is not in oldworld
	if exists(oldworld.Dir(), inref) {
		t.Fatalf("found %s/%s", oldworld.Dir(), inref)
	}

	// check blob content
	gotkey, err := newworld.GetRef(inref)
	if gotkey.Algo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", gotkey.Algo)
	}
	if !keyEqual(blobkey, gotkey) {
		t.Fatalf("expected key %s, got %s", blobkey, gotkey)
	}

	// XXX test db.GetRef
}
*/

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
	n := getGID()
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
	for i, child := range node.Children {
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

func pretty(x interface{}) string {
	b, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
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
	child1 := &Node{Db: db, Key: key1, Label: ""}
	blob2 := mkblob("blob2value")
	key2, err := db.PutBlob("sha256", blob2)
	if err != nil {
		t.Fatal(err)
	}
	child2 := &Node{Db: db, Key: key2, Label: ""}
	blob3 := mkblob("blob3value")
	key3, err := db.PutBlob("sha256", blob3)
	if err != nil {
		t.Fatal(err)
	}
	child3 := &Node{Db: db, Key: key3, Label: ""}

	// put
	node1, err := db.PutNode("sha256", child1, child2)
	if err != nil {
		t.Fatal(err)
	}
	if node1 == nil {
		t.Fatal("node1 is nil")
	}
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
	_ = world1
	// XXX check it
}

// XXX test chunking order

/*
// Experiment with a merkle tree implementation.
//
// XXX ensure we're not vulnerable to https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
func TestTree(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// ref does not contain the algo name
	ref := "someref"
	key := Hash("sha256", []byte("somevalue"))
	// fullref contains the algo name
	fullref := fmt.Sprintf("sha256:%x", key)
	err = db.PutRef("sha256", key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/refs/someref")
	if err != nil {
		t.Fatal(err)
	}
	gotfullref := string(buf)
	if fullref != gotfullref {
		t.Fatalf("expected %s, got %s", fullref, gotfullref)
	}
}
*/

// XXX if merkle tree works, then refactor refs to just be hard links
// to merkle tree nodes?
