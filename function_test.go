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
	buf, err := ioutil.ReadFile("var/somekey")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(buf, val) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(buf))
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
	if bytes.Compare(got, val) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}

/*
func TestLock(t *testing.T) {
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("somekey")
	val := []byte("somevalue")

	go func() {

	}



	db.Put(key, val)
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(got, val) != 0 {
		t.Fatalf("expected %s, got %s", string(val), string(got))
	}
}
*/
