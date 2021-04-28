package pit

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase"
)

const testPitDirPrefix = "pit"

// test boolean condition
func tassert(t *testing.T, cond bool, txt string, args ...interface{}) {
	t.Helper() // cause file:line info to show caller
	if !cond {
		t.Fatalf(txt, args...)
	}
}

func TestPitDir(t *testing.T) {
	err := os.Setenv("PITDIR", "/dev/null")
	tassert(t, err == nil, "%#v", err)
	got := dbdir()
	tassert(t, got == "/dev/null", "got %q", got)

	dir, err := os.Getwd()
	tassert(t, err == nil, "%#v", err)
	err = os.Unsetenv("PITDIR")
	tassert(t, err == nil, "%#v", err)
	got = dbdir()
	tassert(t, dir == got, "expected %q got %q", dir, got)
}

func TestParser(t *testing.T) {
	txt := "sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b hello world"
	msg, err := Parse(txt)
	tassert(t, err == nil, "%#v", err)
	tassert(t, msg.Addr == "sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b", "%#v", msg)
	tassert(t, len(msg.Args) == 2, "%#v", msg)
	tassert(t, msg.Args[0] == "hello", "%#v", msg)
	tassert(t, msg.Args[1] == "world", "%#v", msg)
}

// client-facing API
func TestDispatcher(t *testing.T) {
	dp := NewDispatcher()

	ok := false
	// register a callback for an addr
	addr := "sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b"
	txt := addr + " hello world"
	cb := func(msg string) {
		ok = true
	}
	dp.Register(cb, addr)

	// send that address in a message to the dispatcher
	msg, err := Parse(txt)
	tassert(t, err == nil, "%#v", err)
	err = dp.Dispatch(msg)

	// confirm the callback worked
	tassert(t, ok, "nok")
}

func setupDb(t *testing.T, db *pb.Db) *pb.Db {
	var err error
	var dir string

	if db == nil {
		db = &pb.Db{}
	}
	Assert(db.Dir == "")

	debug := os.Getenv("DEBUG")
	if debug == "1" {
		dir, err = ioutil.TempDir("", testPitDirPrefix)
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

	return db
}

// execute(scriptPath string, args ...string) (stdout, stderr io.Reader, rc int, err error)
// xeq(interpreterPath *pb.Path, args ...string) (stdout, stderr io.Reader, rc int, err error)
// runContainer(img string, cmd ...string) (stdout, stderr io.Reader, rc int, err error)

// putBlob(algo string, rd io.Reader) (blob *pb.Blob, err error)
// getBlob(canpath string, wr io.Writer) (err error)
// putTree(algo string, canpaths []string) (tree *pb.Tree, err error)
// getTree(canpath string) (tree *pb.Tree, err error)
// linkStream(canpath, name string) (stream *pb.Stream, err error)
// getStream(name string) (stream *pb.Stream, err error)
// lsStream(name string, all bool) (canpaths []string, err error)
// catStream(name string) (stream *pb.Stream, err error)
// catTree(canpath string) (tree *pb.Tree, err error)
// putStream(algo string, name string, rd io.Reader) (stream *pb.Stream, err error)

// create() (msg string, err error)
// opendb() (db *pb.Db, err error)
