package pitbase

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

const dir = "var"

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

func TestExclusiveLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")

	// test timing:
	// - :00 start both goroutines
	// - :00 A pause 1 sec
	// - :00 B pause 2 sec
	// - :01 A lock
	// - :01 A write
	// - :01 A pause 2 sec
	// - :02 B try to lock but block
	// - :03 A confirm own value
	// - :03 A unlock
	// - :03 A pause 1 sec
	// - :03 B write
	// - :03 B unlock
	// - :03 B pause 1 sec
	// - :04 A confirm B's value
	// - :04 B confirm own value

	valA := []byte("valueA")
	valB := []byte("valueA")

	// goroutine A
	go func() {
		// - :00 A pause 1 sec
		time.Sleep(1 * time.Second)
		// - :01 A lock
		err := db.ExLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - :01 A write
		err = db.Put(key, valA)
		if err != nil {
			t.Fatal(err)
		}
		// - :01 A pause 2 sec
		time.Sleep(2 * time.Second)
		// - :03 A confirm own value
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valA, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valA), string(got))
		}
		// - :03 A unlock
		err = db.Unlock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - :03 A pause 1 sec
		time.Sleep(1 * time.Second)
		// - :04 A confirm B's value
		got, err = db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valB, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}
	}()

	go func() {
		// - :00 B pause 2 sec
		time.Sleep(2 * time.Second)
		// - :02 B try to lock but block
		err := db.ExLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - :03 B write
		err = db.Put(key, valB)
		if err != nil {
			t.Fatal(err)
		}
		// - :03 B unlock
		err = db.Unlock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - :03 B pause 1 sec
		time.Sleep(1 * time.Second)
		// - :04 B confirm own value
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valB, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}
	}()

}
