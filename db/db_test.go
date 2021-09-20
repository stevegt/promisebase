package db

import (
	"bytes"
	"testing"
)

func TestGetBlock(t *testing.T) {
	db := setup(t, nil)
	val := mkbuf("somevalue")
	path, err := pathFromBuf(db, "block", "sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	gotblock, err := db.PutBlock("sha256", val)
	if err != nil {
		t.Fatal(err)
	}
	if path.Canon != gotblock.Path.Canon {
		t.Fatalf("expected path %s, got %s", path.Canon, gotblock.Path.Canon)
	}
	got, err := db.GetBlock(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(val, got) != 0 {
		t.Fatalf("expected %q, got %q", string(val), string(got))
	}
}

func TestRm(t *testing.T) {
	db := setup(t, nil)
	buf := mkbuf("somevalue")
	block, err := db.PutBlock("sha256", buf)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Rm(block.Path)
	if err != nil {
		t.Fatal(err)
	}
	gotblock, err := db.GetBlock(block.Path)
	if err == nil {
		t.Fatalf("block not deleted: %#v", gotblock)
	}
}
