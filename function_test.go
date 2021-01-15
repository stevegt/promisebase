package pitbase

import (
	"bytes"
	"io/ioutil"
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
	got, err := ioutil.ReadFile("var/somekey")
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

func iterate(t *testing.T, db *Db, iterations int, done chan bool, key, myVal, otherVal []byte) {
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

func XXXTestConcurrent(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	valA := []byte("valueA")
	valB := []byte("valueB")
	doneA := make(chan bool)
	doneB := make(chan bool)

	// attempt to cause collisions by having both A and B do concurrent reads and writes
	iterations := 2000
	go iterate(t, db, iterations, doneA, key, valA, valB)
	go iterate(t, db, iterations, doneB, key, valB, valA)

	<-doneA
	<-doneB
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
