package pit

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/fsnotify/fsnotify"
	"github.com/stevegt/readercomp"
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

func setup(t *testing.T) *Pit {
	var err error
	var dir string

	debug := os.Getenv("DEBUG")
	if debug == "1" {
		dir, err = ioutil.TempDir("", tmpPitPrefix)
		tassert(t, err == nil, "%#v", err)
		fmt.Println(dir)
		// manual cleanup
	} else {
		dir = t.TempDir()
		// automatic cleanup
	}
	pit, err := Create(dir)
	tassert(t, err == nil, "%#v", err)

	return pit
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
	fd, status, err := PipeFd(rd)
	tassert(t, err == nil, "%#v", err)

	// convert it to an os.File
	file := os.NewFile(fd, "foo")

	// check the results
	buf := make([]byte, 32768)
	n, err := file.Read(buf)
	tassert(t, err == nil, "%#v", err)
	tassert(t, n == len(expect), "%#v", err)
	tassert(t, string(buf[:n]) == expect, "got %v", buf[:n])

	copyerr := <-status
	tassert(t, copyerr == nil, "%#v", copyerr)
}

func TestSocket(t *testing.T) {
	pit := setup(t)
	id := "appid"

	listener, err := pit.Listen(id)
	tassert(t, err == nil, "%#v", err)

	go func() {
		// sleep to ensure server's Accept() has a chance to start
		time.Sleep(time.Second)
		conn, err := pit.Connect(id)
		tassert(t, err == nil, "%#v", err)
		n, err := conn.Write([]byte("hi"))
		tassert(t, err == nil, "%#v", err)
		tassert(t, n == 2, "got %d", n)
		conn.Close()
	}()

	// we block on Accept() while waiting for client goroutine to connect
	conn, err := listener.Accept()
	tassert(t, err == nil, "%#v", err)
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	tassert(t, err == nil, "%#v", err)
	tassert(t, n == 2, "got %d", n)
	got := string(buf[:n])
	tassert(t, got == "hi", "got %s", got)
}

func TestInotify(t *testing.T) {
	// https://pkg.go.dev/github.com/fsnotify/fsnotify#readme-usage
	pit := setup(t)

	// create a file in the pit dir
	fn := filepath.Join(pit.Dir, "foo")
	err := ioutil.WriteFile(fn, []byte(""), 0644)
	tassert(t, err == nil, "%#v", err)

	// check for CREATE event
	event, ok := <-pit.Events
	tassert(t, ok, "%#v", "nok")
	tassert(t, event.Op&fsnotify.Create > 0, "event %#v", event)
}

func TestCreatePit(t *testing.T) {
	setup(t)
	// XXX
}

func TestRunHub(t *testing.T) {
	// pit := setup(t)

	expect := "hello"
	expectrd := bytes.NewReader([]byte(expect))
	emptyrd := bytes.NewReader([]byte(""))

	// get the image from docker hub
	stdoutr, stdout := io.Pipe()
	stderrr, stderr := io.Pipe()
	out, rc, err := runContainer("docker.io/library/alpine:3.12.0", "echo", "-n", expect)
	tassert(t, err == nil, "%#v", err)
	tassert(t, rc == 0, "%#v", rc)

	go func() {
		stdcopy.StdCopy(stdout, stderr, out)
		stdout.Close()
		stderr.Close()
	}()

	ok, err := readercomp.Equal(expectrd, stdoutr, 4096)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

	ok, err = readercomp.Equal(emptyrd, stderrr, 4096)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

}

func TestImageSave(t *testing.T) {
	pit := setup(t)

	src := "docker.io/library/alpine:3.12.0"
	addr := "tree/sha256/658ab2dbc592a6e37da9623bc416bfdb5d311e846da7c349eb79a6ed640e08cd"

	// pull container image and save it as a stream
	tree, err := pit.imageSave("sha256", src)
	tassert(t, err == nil, "%v", err)
	tassert(t, tree != nil, "%v", tree)

	// make sure it's at least a tarball
	out, err := shellin(tree, "file", "-")
	tassert(t, err == nil, "%v", err)
	outstr := string(out)
	tassert(t, strings.Index(outstr, "POSIX tar archive") >= 0, outstr)

	expect := "hello"
	expectrd := bytes.NewReader([]byte(expect))
	emptyrd := bytes.NewReader([]byte(""))

	// get the image from the pitbase stream we saved above
	stdoutr, stdout := io.Pipe()
	stderrr, stderr := io.Pipe()
	outrd, rc, err := runContainer(addr, "echo", "-n", expect)
	tassert(t, err == nil, "%#v", err)
	tassert(t, rc == 0, "%#v", rc)

	go func() {
		stdcopy.StdCopy(stdout, stderr, outrd)
		stdout.Close()
		stderr.Close()
	}()

	ok, err := readercomp.Equal(expectrd, stdoutr, 4096)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

	ok, err = readercomp.Equal(emptyrd, stderrr, 4096)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

}

func shellin(stdin io.Reader, path string, args ...string) (out []byte, err error) {
	cmd := exec.Command(path, args...)
	cmd.Stdin = stdin
	out, err = cmd.CombinedOutput()
	return
}

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
