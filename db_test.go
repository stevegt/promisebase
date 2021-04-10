package pitbase

import (
	"bytes"
	"testing"
)

func TestGetBlob(t *testing.T) {
	db := setup(t)
	val := mkbuf("somevalue")
	path, err := pathFromBuf(db, "blob", "sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotblob, err := db.PutBlob("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if path.Canon != gotblob.Path.Canon {
		t.Fatalf("expected path %s, got %s", path.Canon, gotblob.Path.Canon)
	}
	got, err := db.GetBlob(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %q, got %q", string(val), string(got))
	}
}

func TestRm(t *testing.T) {
	db := setup(t)
	buf := mkbuf("somevalue")
	blob, err := db.PutBlob("sha256", buf)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(blob.Path)
	if err != nil {
		t.Fatal(err)
	}
	gotblob, err := db.GetBlob(blob.Path)
	if err == nil {
		t.Fatalf("blob not deleted: %#v", gotblob)
	}
}
