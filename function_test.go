package pitbase

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestPut(t *testing.T) {
	db := Open("var")
	val := []byte("somevalue")
	db.Put([]byte("somekey"), val)
	buf, err := ioutil.ReadFile("var/somekey")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(buf, val) != 0 {
		t.Fatal("expected somevalue, got", buf)
	}
}
