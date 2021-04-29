package pit

import (
	"bytes"
	"os"
	"testing"
	// . "github.com/stevegt/goadapt"
)

const tmpPitPrefix = "pit"

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
	got, err := dbdir()
	tassert(t, err == nil, "%#v", err)
	tassert(t, got == "/dev/null", "got %q", got)

	dir, err := os.Getwd()
	tassert(t, err == nil, "%#v", err)
	err = os.Unsetenv("PITDIR")
	tassert(t, err == nil, "%#v", err)
	got, err = dbdir()
	tassert(t, err == nil, "%#v", err)
	tassert(t, dir == got, "expected %q got %q", dir, got)
}

func TestParser(t *testing.T) {
	txt := Addr("sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b hello world")
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

	// create some simple callbacks
	ok1 := false
	cb1 := func(msg Msg) error {
		ok1 = true
		return nil
	}

	ok1b := false
	cb1b := func(msg Msg) error {
		ok1b = true
		return nil
	}

	// create some simple callbacks
	ok2 := false
	cb2 := func(msg Msg) error {
		ok2 = true
		return nil
	}

	// register some callbacks
	addr1 := Addr("sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b")
	txt1 := addr1 + " hello world"
	dp.Register(cb1, addr1)
	dp.Register(cb1b, addr1)
	addr2 := Addr("sha256/4f52047d917c0082d7eaafa55f97afe2b84c306ce2c4e46b0ed1ff238d8d3af0")
	txt2 := addr2 + " hello again world"
	dp.Register(cb2, addr2)

	// send that address in a message to the dispatcher
	msg, err := Parse(txt1)
	tassert(t, err == nil, "%#v", err)
	err = dp.Dispatch(msg)

	// confirm the callback worked
	tassert(t, ok1, "nok")
	tassert(t, ok1b, "nok")
	tassert(t, !ok2, "nok")

	// send another address in a message to the dispatcher
	msg, err = Parse(txt2)
	tassert(t, err == nil, "%#v", err)
	err = dp.Dispatch(msg)

	// confirm the callback worked
	tassert(t, ok1, "nok")
	tassert(t, ok1b, "nok")
	tassert(t, ok2, "nok")
}

func TestPipeFd(t *testing.T) {
	// create an io.Reader
	expect := "somedata"
	rd := bytes.NewReader([]byte(expect))

	// convert it to a file descriptor
	fd := PipeFd(rd)

	// convert it to an os.File
	file := os.NewFile(fd, "foo")

	// check the results
	buf := make([]byte, 32768)
	n, err := file.Read(buf)
	tassert(t, err == nil, "%#v", err)
	tassert(t, n == len(expect), "%#v", err)
	tassert(t, string(buf[:n]) == expect, "got %v", buf[:n])
}

/*
func TestCreatePit(t *testing.T) {
	setup(t)
}

func TestRunC(t *testing.T) {

	runContainer(img string, cmd ...string) (stdout, stderr io.Reader, rc int, err error)

func setup(t *testing.T, pit *Pit) *Pit {
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

	err := os.Setenv("PITDIR", "/dev/null")
	pit := CreatePit()

	db, err = db.Create()
	Ck(err)
	db, err = pb.Open(dir)
	Ck(err)
	tassert(t, db != nil, "db is nil")

	return db
}
*/

// execute(scriptPath string, args ...string) (stdout, stderr io.Reader, rc int, err error)
// xeq(interpreterPath *pb.Path, args ...string) (stdout, stderr io.Reader, rc int, err error)

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
