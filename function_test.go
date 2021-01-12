package pitbase

import (
	"bytes"
	"io/ioutil"
	"testing"
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

func TestLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")

	// these channels are used for barrier rendevous
	goA := make(chan bool)
	goB := make(chan bool)
	doneA := make(chan bool)
	doneB := make(chan bool)
	// test sequence:
	// exclusive lock:
	// - B wait
	// - A exlock
	// - A signal B
	// - A write
	// - B try to exlock but block
	// - A confirm own value
	// - A unlock
	// - A wait
	// - B write
	// - B unlock
	// - B signal A
	// - A confirm B's value
	// - B confirm own value
	// shared lock:
	// - B wait
	// - A exlock
	// - A signal B
	// - B try to shlock but block
	// - A write
	// - A unlock
	// - A shlock
	// - A confirm own value
	// - B confirm A's value
	// - return

	valA := []byte("valueA")
	valB := []byte("valueB")

	finishedA := false
	finishedB := false

	// goroutine A
	go func() {
		// - A exlock
		fd, err := db.ExLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - A signal B
		goB <- true
		// - A write
		err = db.Put(key, valA)
		if err != nil {
			t.Fatal(err)
		}
		// - A confirm own value
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valA, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valA), string(got))
		}
		// - A unlock
		err = db.Unlock(fd)
		if err != nil {
			t.Fatal(err)
		}
		// - A wait
		<-goA
		// - A confirm B's value
		got, err = db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valB, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}

		// - A exlock
		fd, err = db.ExLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - A signal B
		goB <- true
		// - A write
		err = db.Put(key, valA)
		if err != nil {
			t.Fatal(err)
		}
		// - A unlock
		err = db.Unlock(fd)
		if err != nil {
			t.Fatal(err)
		}
		print("lksadjf\n")
		// - A shlock
		fd, err = db.ShLock(key)
		if err != nil {
			t.Fatal(err)
		}
		print("iuwoe\n")
		// - A confirm own value
		got, err = db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valA, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}
		finishedA = true
		doneA <- true
	}()

	go func() {
		// - B wait
		<-goB
		// - B try to exlock but block
		fd, err := db.ExLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - B write
		err = db.Put(key, valB)
		if err != nil {
			t.Fatal(err)
		}
		// - B unlock
		err = db.Unlock(fd)
		if err != nil {
			t.Fatal(err)
		}
		// - B signal A
		goA <- true
		// - B confirm own value
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valB, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}

		// - B wait
		<-goB
		// - B try to shlock but block
		fd, err = db.ShLock(key)
		if err != nil {
			t.Fatal(err)
		}
		// - B confirm A's value
		got, err = db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(valA, got) != 0 {
			t.Fatalf("expected %s, got %s", string(valB), string(got))
		}
		finishedB = true
		doneB <- true
	}()

	<-doneA
	<-doneB
	if finishedA == false || finishedB == false {
		t.Fatalf("finishedA: %t, finishedB: %t", finishedA, finishedB)
	}
}
