package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase/db"
)

const testDbDirPrefix = "pitbase_db"
const testMountPrefix = "pitbase_mnt"

func TestHello(t *testing.T) {
	// t.Fatal("not implemented")
	db, mnt := setup(t, nil)
	_ = db

	server, err := hello(mnt)
	tassert(t, err == nil, "%#v", err)

	fn := filepath.Join(mnt, "hello.txt")
	expect := []byte("hello")
	got, err := ioutil.ReadFile(fn)
	tassert(t, err == nil, "%#v", err)
	tassert(t, bytes.Compare(expect, got) == 0, "expect %s, got %v", string(expect), string(got))

	err = server.Unmount()
	tassert(t, err == nil, "%#v", err)
}

// test boolean condition
// XXX consolidate into a util or testutil package
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
	}
}

func setup(t *testing.T, dbin *pb.Db) (db *pb.Db, mnt string) {
	// XXX test other depths
	// db, err = Db{Depth: 4}.Create(dir)

	var err error
	var dir string

	if dbin == nil {
		db = &pb.Db{}
	} else {
		db = dbin
	}
	Assert(db.Dir == "")

	debug := os.Getenv("DEBUG")
	if debug == "1" {
		dir, err = ioutil.TempDir("", testDbDirPrefix)
		Ck(err)
		fmt.Println(dir)
		// no cleanup
	} else {
		dir = t.TempDir()
		// automatically cleaned up
	}
	db.Dir = dir

	db, err = db.Create()
	Ck(err)
	db, err = pb.Open(dir)
	Ck(err)
	tassert(t, db != nil, "db is nil")

	mnt = t.TempDir()

	return
}
