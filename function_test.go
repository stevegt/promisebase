package pitbase

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
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

func iterate(t *testing.T, db *Db, iterations int, done chan bool, myblob, otherblob *[]byte) {
	for i := 0; i < iterations; i++ {
		// store a blob
		key, err := db.PutBlob("sha256", myblob)
		if err != nil {
			t.Fatal(err)
		}
		// start a transaction so we're isolated
		tx, err := db.StartTransaction()
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

func nonMissingErr(err error) error {
	switch err.(type) {
	case *os.PathError:
		return nil
	case nil:
		return nil
	}
	return err
}

func XXXiterate(t *testing.T, db *Db, iterations int, done chan bool, myVal []byte) {
	i := 0
	for ; i < iterations; i++ {
		// create tmpVal by appending some random characters to myVal
		tmpVal := []byte(fmt.Sprintf("%s.%d", string(myVal), rand.Uint64()))
		// put tmpVal into a blob
		key, err := db.PutBlob("sha256", &tmpVal)
		if err != nil {
			t.Fatal(err)
		}
		// store the blob's key in a unique ref
		ref := fmt.Sprintf("ref.%s.%d", string(myVal), rand.Uint64())
		err = db.PutRef(key, ref)
		if err != nil {
			t.Fatal(err)
		}
		// get the ref
		gotkey, err := db.GetRef(ref)
		if err != nil {
			t.Fatal(err)
		}
		if key.Algo != "sha256" {
			t.Fatalf("expected 'sha256', got '%s'", key.Algo)
		}
		// get the blob
		gotblob, err := db.GetBlob(gotkey)
		// compare the blob we got with tmpVal
		if bytes.Compare(tmpVal, *gotblob) != 0 {
			t.Fatalf("expected %s, got %s", string(tmpVal), string(*gotblob))
		}
		// XXX delete the ref and the blob

	}
	if i != iterations {
		t.Fatal("omg no it didnt work there's not enough iterations :(", iterations)
	}
	done <- true
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

func TestConcurrent(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// key := []byte("somekey")
	valA := mkblob("valueA")
	valB := mkblob("valueB")
	doneA := make(chan bool)
	doneB := make(chan bool)

	// attempt to cause collisions by having both A and B do concurrent reads and writes
	iterations := 2000
	go iterate(t, db, iterations, doneA, valA, valB)
	go iterate(t, db, iterations, doneB, valB, valA)

	<-doneA
	<-doneB
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

func TestPutRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ref := "someref"
	key, err := KeyFromBlob("sha256", mkblob("somevalue"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutRef(key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/refs/someref")
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf)
	expect := key.String()
	if expect != got {
		t.Fatalf("expected %s, got %s", expect, got)
	}
}

func keyEqual(a, b *Key) bool {
	return a.String() == b.String()
}

func TestGetRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref := "someref"
	key, err := KeyFromBlob("sha256", mkblob("somevalue"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutRef(key, ref)
	if err != nil {
		t.Fatal(err)
	}
	gotkey, err := db.GetRef(ref)
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
func TestSubRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref := "somedir/someref"
	key, err := KeyFromBlob("sha256", mkblob("somevalue"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutRef(key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/refs/somedir/someref")
	if err != nil {
		t.Fatal(err)
	}
	expect := key.String()
	got := string(buf)
	if expect != got {
		t.Fatalf("expected %s, got %s", expect, got)
	}
	gotkey, err := db.GetRef(ref)
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

func TestRefPath(t *testing.T) {
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

func TestTransaction(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	refdir := filepath.Join(db.Dir, "refs")

	// create blob and ref not in our transaction
	outval := mkblob(fmt.Sprintf("value.outside"))
	outkey, err := db.PutBlob("sha256", outval)
	if err != nil {
		t.Fatal(err)
	}
	outref := "ref.outside"
	err = db.PutRef(outkey, outref)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Index(tx.dir, "var/tx/") != 0 {
		t.Fatalf("tx.dir should not be %s", tx.dir)
	}

	// verify old ref is hardlinked into tx.Dir
	if !exists(tx.dir, outref) {
		t.Fatalf("missing %s/%s", tx.dir, outref)
	}

	// create ref in our transaction
	inref := "ref.inside"
	err = tx.PutRef(outkey, inref)
	if err != nil {
		t.Fatal(err)
	}

	// verify new ref is in tx.Dir
	if !exists(tx.dir, inref) {
		t.Fatalf("missing %s/%s", tx.dir, inref)
	}
	// verify new ref is not in db.Dir
	if exists(refdir, inref) {
		t.Fatalf("found %s/%s", refdir, inref)
	}

	gotkey, err := tx.GetRef(inref)
	if gotkey.Algo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", gotkey.Algo)
	}
	if !keyEqual(outkey, gotkey) {
		t.Fatalf("expected key %s, got %s", outkey, gotkey)
	}

	// XXX test db.GetRef

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// XXX ensure tx.Dir is gone
	// XXX ensure call to tx.* fails gracefully

	// verify old ref is in db.Dir
	if !exists(refdir, outref) {
		t.Fatalf("missing %s/%s", refdir, outref)
	}
	// verify new ref is in db.Dir
	if !exists(refdir, inref) {
		t.Fatalf("missing %s/%s", refdir, inref)
	}
	// XXX verify blob content

}

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
	node, err := db.ReadNode("node/sha256/00e2a12b4ae802c79344fa05fd49ff63c1335fdd5bc308dab69a6d6b5b5884b2")
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range node.ChildKeys {
		switch i {
		case 0:
			expect := "node/sha256/563dcb27d5d8ae1c579ea8b2af89db2d125ade16d95efde13952821230d28e46"
			tassert(t, expect == key.String(), "expected %v got %v", expect, key.String())
		case 1:
			expect := "blob/sha256/534d059533cc6a29b0e8747334c6af08619b1b59e6727f50a8094c90f6393282"
			tassert(t, expect == key.String(), "expected %q got %q", expect, key.String())
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

func TestPutNode(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	blob1 := mkblob("blob1value")
	key1, err := db.PutBlob("sha256", blob1)
	if err != nil {
		t.Fatal(err)
	}

	blob2 := mkblob("blob2value")
	key2, err := db.PutBlob("sha256", blob2)
	if err != nil {
		t.Fatal(err)
	}

	node, err := db.PutNode("sha256", key1, key2)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := node.Verify()
	if err != nil {
		t.Fatal(err)
	}
	tassert(t, ok, "node verify failed: %v", node)
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

// XXX rollback()

// XXX deprecate db methods that are in tx
