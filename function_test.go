package pitbase

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

const dir = "var"

func TestNotExist(t *testing.T) {
	os.RemoveAll(dir)
	_, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
}

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
	key := []byte("somekey")

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

func XXXiterateLock(t *testing.T, db *Db, iterations int, done chan bool, key, myVal, otherVal []byte) {
	for i := 0; i < iterations; i++ {
		err := db.Put(key, myVal)
		if err != nil {
			t.Fatal(err)
		}
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		err = db.Rm(key)
		if err != nil {
			t.Fatal(err)
		}
		// the result must be either A or B, otherwise it's
		// corrupt due to a lack of locking in Put() or Get()
		if bytes.Compare(myVal, got) != 0 && bytes.Compare(otherVal, got) != 0 {
			t.Fatalf("expected %s or %s, got %s", string(myVal), string(otherVal), string(got))
		}
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

func iterate(t *testing.T, db *Db, iterations int, done chan bool, myVal []byte) {
	i := 0
	for ; i < iterations; i++ {
		// create tmpVal by appending some random characters to myVal
		tmpVal := []byte(fmt.Sprintf("%s.%d", string(myVal), rand.Uint64()))
		// put tmpVal into a blob
		key, err := db.PutBlob("sha256", tmpVal)
		if err != nil {
			t.Fatal(err)
		}
		// store the blob's key in a unique ref
		ref := fmt.Sprintf("ref.%s.%d", string(myVal), rand.Uint64())
		err = db.PutRef("sha256", key, ref)
		if err != nil {
			t.Fatal(err)
		}
		// get the ref
		gotalgo, gotkey, err := db.GetRef(ref)
		if err != nil {
			t.Fatal(err)
		}
		if gotalgo != "sha256" {
			t.Fatalf("expected 'sha256', got '%s'", string(gotalgo))
		}
		// get the blob
		gotblob, err := db.GetBlob("sha256", gotkey)
		// compare the blob we got with tmpVal
		if bytes.Compare(tmpVal, gotblob) != 0 {
			t.Fatalf("expected %s, got %s", string(tmpVal), string(gotblob))
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

	err = db.ExLock()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	err = db.ShLock()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Unlock()
	if err != nil {
		t.Fatal(err)
	}

}

// XXX once all of the following tests are working, delete all of the
// locking code and rename the *NoLock functions

func TestConcurrent(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// key := []byte("somekey")
	valA := []byte("valueA")
	valB := []byte("valueB")
	doneA := make(chan bool)
	doneB := make(chan bool)

	// attempt to cause collisions by having both A and B do concurrent reads and writes
	iterations := 2000
	go iterate(t, db, iterations, doneA, valA)
	go iterate(t, db, iterations, doneB, valB)

	<-doneA
	<-doneB
}

func TestPutNoLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.PutNoLock(key, val)
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

func TestGetNoLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.PutNoLock(key, val)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.GetNoLock(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

func TestRmNoLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")
	err = db.PutNoLock(key, val)
	if err != nil {
		t.Fatal(err)
	}
	err = db.RmNoLock(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.GetNoLock(key)
	if err == nil {
		t.Fatalf("key not deleted: %s", key)
	}
}

func TestPutBlob(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	val := []byte("somevalue")
	k := sha256.Sum256(val)
	key := make([]byte, len(k))
	copy(key[:], k[0:len(k)])
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(gotkey, key) != 0 {
		t.Fatalf("expected key %q, got %q", key, gotkey)
	}
	got, err := ioutil.ReadFile(db.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

func TestHash(t *testing.T) {
	algo := "sha256"
	val := []byte("somevalue")
	k := sha256.Sum256(val)
	key := make([]byte, len(k))
	copy(key[:], k[0:len(k)])
	gotkey := Hash(algo, val)
	if bytes.Compare(gotkey, key) != 0 {
		t.Fatalf("expected key %q, got %q", key, gotkey)
	}
}

func TestGetBlob(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	val := []byte("somevalue")
	key := Hash("sha256", val)
	gotkey, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(gotkey, key) != 0 {
		t.Fatalf("expected key %q, got %q", key, gotkey)
	}
	got, err := db.GetBlob("sha256", key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

func TestPutRef(t *testing.T) {
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

func TestGetRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref := "someref"
	key := Hash("sha256", []byte("somevalue"))
	err = db.PutRef("sha256", key, ref)
	if err != nil {
		t.Fatal(err)
	}
	gotalgo, gotkey, err := db.GetRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	if gotalgo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", string(gotalgo))
	}
	if bytes.Compare(key, gotkey) != 0 {
		t.Fatalf("expected '%x', got '%q'", key, string(gotkey))
	}
}
func TestSubRef(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ref := "somedir/someref"
	key := Hash("sha256", []byte("somevalue"))
	err = db.PutRef("sha256", key, ref)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadFile("var/refs/somedir/someref")
	if err != nil {
		t.Fatal(err)
	}
	fullref := fmt.Sprintf("sha256:%x", key)
	gotfullref := string(buf)
	if fullref != gotfullref {
		t.Fatalf("expected %s, got %s", fullref, gotfullref)
	}
	gotalgo, gotkey, err := db.GetRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	if gotalgo != "sha256" {
		t.Fatalf("expected 'sha256', got '%s'", string(gotalgo))
	}
	if bytes.Compare(key, gotkey) != 0 {
		t.Fatalf("expected '%x', got '%q'", key, string(gotkey))
	}
}

func TestPath(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	val := []byte("somevalue")
	key := Hash("sha256", val)
	path := "var/objects/70a524688ced8e45d26776fd4dc56410725b566cd840c044546ab30c4b499342"
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

	// create blob and ref not in our transaction
	outval := []byte(fmt.Sprintf("value.outside"))
	outkey, err := db.PutBlob("sha256", outval)
	if err != nil {
		t.Fatal(err)
	}
	outref := fmt.Sprintf("ref.outside")
	err = db.PutRef("sha256", outkey, outref)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.StartTransaction()
	if err != nil {
		t.Fatal(err)
	}

	// create ref in our transaction
	inref := fmt.Sprintf("ref.inside")
	err = db.PutRef("sha256", outkey, inref)
	if err != nil {
		t.Fatal(err)
	}

	// XXX verify old ref is in tx.Dir
	// XXX verify new ref is in tx.Dir
	// XXX verify new ref is not in db.Dir

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// XXX verify old ref is in db.Dir
	// XXX verify new ref is in db.Dir
	// XXX verify blob content

}

func X01TestKey(t *testing.T) {
	var key *Key

	val := []byte("somevalue")
	algo := "sha256"
	bin := Hash(algo, val)
	hex := fmt.Sprintf("%x", bin)
	key = KeyFromBlob(algo, val)
	if algo != key.Algo {
		t.Fatalf("expected %s, got %s", algo, key.Algo)
	}
	if bytes.Compare(bin, key.Bin) != 0 {
		t.Fatalf("expected %s, got %s", string(bin), string(key.Bin))
	}
	if hex != key.Hex {
		t.Fatalf("expected %s, got %s", hex, key.Hex)
	}
}

// XXX change all functions to use Key struct

// XXX find all the places where we're passing blobs by value and
// change them so we pass by reference
