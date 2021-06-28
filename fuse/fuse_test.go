package fuse

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

// test boolean condition
// XXX consolidate into a util or testutil package
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
	}
}

func mkbuf(s string) []byte {
	tmp := []byte(s)
	return tmp
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
		// no cleanup
	} else {
		dir = t.TempDir()
		// automatically cleaned up
	}
	db.Dir = dir
	fmt.Println("db ", dir)

	db, err = db.Create()
	Ck(err)
	db, err = pb.Open(dir)
	Ck(err)
	tassert(t, db != nil, "db is nil")

	// mnt = t.TempDir()
	// Ck(err)
	mnt = "/tmp/FuseTest81027" // XXX
	fmt.Println("mnt", mnt)

	return
}

func TestHello(t *testing.T) {
	// t.Fatal("not implemented")
	db, mnt := setup(t, nil)
	_ = db

	server, err := hello(mnt)
	tassert(t, err == nil, "%#v", err)
	defer server.Unmount()

	fn := filepath.Join(mnt, "hello.txt")
	expect := []byte("hello")
	got, err := ioutil.ReadFile(fn)
	tassert(t, err == nil, "%#v", err)
	tassert(t, bytes.Compare(expect, got) == 0, "expect %s, got %v", string(expect), string(got))

}

func TestTreeFuse(t *testing.T) {
	db, mnt := setup(t, nil)

	buf1 := mkbuf("blob1value")
	blob1, err := db.PutBlob("sha256", buf1)
	if err != nil {
		t.Fatal(err)
	}
	buf2 := mkbuf("blob2value")
	blob2, err := db.PutBlob("sha256", buf2)
	if err != nil {
		t.Fatal(err)
	}
	buf3 := mkbuf("blob3value")
	blob3, err := db.PutBlob("sha256", buf3)
	if err != nil {
		t.Fatal(err)
	}

	// put
	tree1, err := db.PutTree("sha256", blob1, blob2)
	if err != nil {
		t.Fatal(err)
	}
	if tree1 == nil {
		t.Fatal("tree1 is nil")
	}
	tree2, err := db.PutTree("sha256", tree1, blob3)
	if err != nil {
		t.Fatal(err)
	}
	if tree2 == nil {
		t.Fatal("tree2 is nil")
	}

	server, err := Serve(db, mnt)
	tassert(t, err == nil, "%#v", err)
	defer server.Unmount()

	expect := []byte("blob1valueblob2valueblob3value")

	// debug
	if false {
		info := `
		While developing pitbase/fuse, we're pausing here so you can play around 
		in the filesystem.  Run 'fusermount -u %s' to exit.


		Mount point: %s
		Test data: %s/%s
		
		`
		fmt.Printf(info, mnt, mnt, mnt, tree2.Addr)
		// Wait until unmount before exiting
		server.Wait()
		return
	}

	fn := filepath.Join(mnt, tree2.Addr, "content")
	got, err := ioutil.ReadFile(fn)
	tassert(t, err == nil, "%#v", err)
	tassert(t, bytes.Compare(expect, got) == 0, "expect %s, got %v", string(expect), string(got))

}

func TestWrite(t *testing.T) {
	db, mnt := setup(t, nil)

	server, err := Serve(db, mnt)
	tassert(t, err == nil, "%#v", err)
	defer server.Unmount()

	expect := []byte("blob1valueblob2valueblob3value")
	addr := "sha256/da0e74aa2d64168df0321877dd98a0e0c1f8b8f02a6f54211995623f518dd7f4"

	// when we write the contents of `expect` to ./tag/test1/sha256,
	// that creates a tree, writes the data to it, and makes a symlink
	// at ./tag/test1 pointing at the path in `addr`/content

	newfn := filepath.Join(mnt, "tag", "test1", "sha256")
	err = ioutil.WriteFile(newfn, expect, 0644)
	tassert(t, err == nil, "%#v", err)

	fn := filepath.Join(mnt, addr, "content")
	got, err := ioutil.ReadFile(fn)
	tassert(t, err == nil, "%#v", err)
	tassert(t, bytes.Compare(expect, got) == 0, "expect %s, got %v", string(expect), string(got))

}
