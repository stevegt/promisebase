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

	"github.com/fsnotify/fsnotify"
	"github.com/stevegt/readercomp"
	"github.com/vmihailenco/msgpack"

	// . "github.com/stevegt/goadapt"
	"github.com/alessio/shellescape"
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
		tassert(t, err == nil, "%v", err)
		fmt.Println(dir)
		// manual cleanup
	} else {
		dir = t.TempDir()
		// automatic cleanup
	}
	pit, err := Create(dir)
	tassert(t, err == nil, "%v", err)

	return pit
}

func TestMsgPack(t *testing.T) {
	txt := "sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b hello world"
	req, err := Parse(txt)
	tassert(t, err == nil, "%v", err)

	buf, err := msgpack.Marshal(req)
	tassert(t, err == nil, "%v", err)

	var got Msg
	err = msgpack.Unmarshal(buf, &got)
	tassert(t, err == nil, "%v", err)
	tassert(t, req.Compare(&got), "got %#v", got)

}

func TestPitDir(t *testing.T) {
	err := os.Setenv("PITDIR", "/dev/null")
	tassert(t, err == nil, "%v", err)
	got, err := dbdir()
	tassert(t, err == nil, "%v", err)
	tassert(t, got == "/dev/null", "got %q", got)

	dir, err := os.Getwd()
	tassert(t, err == nil, "%v", err)
	err = os.Unsetenv("PITDIR")
	tassert(t, err == nil, "%v", err)
	got, err = dbdir()
	tassert(t, err == nil, "%v", err)
	tassert(t, dir == got, "expected %q got %q", dir, got)
}

func TestParser(t *testing.T) {
	txt := "sha256/1adab0720df1e5e62a8d2e7866a4a84dafcdfb71dde10443fdac950d8066623b hello world"
	msg, err := Parse(txt)
	tassert(t, err == nil, "%v", err)
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
	msg, err := Parse(string(txt1))
	tassert(t, err == nil, "%v", err)
	err = dp.Dispatch(msg)

	// confirm the callback worked
	tassert(t, ok1, "nok")
	tassert(t, ok1b, "nok")
	tassert(t, !ok2, "nok")

	// send another address in a message to the dispatcher
	msg, err = Parse(string(txt2))
	tassert(t, err == nil, "%v", err)
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
	tassert(t, err == nil, "%v", err)

	// convert it to an os.File
	file := os.NewFile(fd, "foo")

	// check the results
	buf := make([]byte, 32768)
	n, err := file.Read(buf)
	tassert(t, err == nil, "%v", err)
	tassert(t, n == len(expect), "%v", err)
	tassert(t, string(buf[:n]) == expect, "got %v", buf[:n])

	copyerr := <-status
	tassert(t, copyerr == nil, "%#v", copyerr)
}

// XXX use this as a starter for pitd
func TestServe(t *testing.T) {
	pit := setup(t)
	fn := "pit.sock"

	errc := pit.Serve(fn)
	// XXX check messages on errc
	_ = errc

	// XXX try some client-side stuff here
	// grab some code from TestSocket and feed in
	// a message that causes a container to
	// be run, check output

	// simulate a client
	// sleep to ensure server's Accept() has a chance to start
	time.Sleep(time.Second)
	conn, err := pit.Connect(fn)
	tassert(t, err == nil, "%v", err)
	err = echoTestSocket(t, conn, "docker.io/library/alpine:3.12.0", "hello")
	tassert(t, err == nil, "%v", err)
	conn.Close()

}

func TestSocket(t *testing.T) {
	pit := setup(t)
	fn := "pit.sock"

	listener, err := pit.Listen(fn)
	tassert(t, err == nil, "%v", err)

	msg, err := Parse("some/hash/path echo hello")
	tassert(t, err == nil, "%v", err)

	// simulate a client
	go func() {
		// sleep to ensure server's Accept() has a chance to start
		time.Sleep(time.Second)
		conn, err := pit.Connect(fn)
		tassert(t, err == nil, "%v", err)
		// the Encode() method takes the msg struct, marshals it into
		// a msgpack message, and writes it to the conn that we passed
		// into NewEncoder.
		encoder := msgpack.NewEncoder(conn)
		err = encoder.Encode(msg)
		tassert(t, err == nil, "%v", err)
		conn.Close() // XXX test should still work without this close
	}()

	// server side
	// we block on Accept() while waiting for client goroutine to connect
	conn, err := listener.Accept()
	tassert(t, err == nil, "%v", err)
	var got Msg
	// the Decode() method reads from conn and unmarshals the
	// msgpack message into msg.
	decoder := msgpack.NewDecoder(conn)
	err = decoder.Decode(&got)
	tassert(t, err == nil, "%v", err)
	tassert(t, msg.Compare(&got), "got %#v", got)
}

func TestInotify(t *testing.T) {
	// https://pkg.go.dev/github.com/fsnotify/fsnotify#readme-usage
	pit := setup(t)

	// create a file in the pit dir
	fn := filepath.Join(pit.Dir, "foo")
	err := ioutil.WriteFile(fn, []byte(""), 0644)
	tassert(t, err == nil, "%v", err)

	// check for CREATE event
	event, ok := <-pit.Events
	tassert(t, ok, "%#v", "nok")
	tassert(t, event.Op&fsnotify.Create > 0, "event %#v", event)
}

func TestRunHub(t *testing.T) {
	pit := setup(t)

	// get the image from docker hub
	err := echoTest(t, pit, "docker.io/library/alpine:3.12.0", "hello")
	tassert(t, err == nil, "%v", err)
}

func echoTest(t *testing.T, pit *Pit, img, expect string) (err error) {

	fn := "/run/containerd/containerd.sock"
	err = pit.connectRuntime(fn)
	tassert(t, err == nil, "%v", err)
	client := pit.runtime.client

	fmt.Println("echoTest starting")
	expectrd := bytes.NewReader([]byte(expect))
	emptyrd := bytes.NewReader([]byte(""))

	stdoutr, stdout := io.Pipe()
	stderrr, stderr := io.Pipe()

	cntr := &Container{
		Image:  img,
		Args:   []string{"echo", "-n", expect},
		Stdin:  nil,
		Stdout: stdout,
		Stderr: stderr,
	}

	fmt.Println("container starting")
	err = pit.startContainer(cntr)

	tassert(t, err == nil, "%v", err)

	fmt.Println("container started")

	// make sure we wait before calling start
	// XXX why?
	exitStatusC, err := cntr.task.Wait(cntr.ctx)
	_ = exitStatusC
	if err != nil {
		// XXX why not abend?
		fmt.Println(err)
	}
	fmt.Println("wait done")

	/*
		// sleep for a lil bit to see the logs
		// XXX get rid of sleep
		time.Sleep(1 * time.Second)

		// kill the process and get the exit status
		// XXX no
		err = task.Kill(ctx, syscall.SIGTERM)
		Ck(err)

		fmt.Println("container task killed")
		// wait for the process to fully exit and print out the exit status

	*/
	status := <-exitStatusC
	fmt.Println("got status")
	code, _, err := status.Result()
	tassert(t, err == nil, "%v", err)
	fmt.Printf("exited with status: %d\n", code)
	tassert(t, code == 0, "%v", code)
	// fmt.Println("exiting with no status")

	stdout.Close()
	stderr.Close()
	fmt.Println("starting readercomp stdout")
	ok, err := readercomp.Equal(expectrd, stdoutr, 1024)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

	fmt.Println("starting readercomp stderr")
	ok, err = readercomp.Equal(emptyrd, stderrr, 1024)
	tassert(t, err == nil, "%v", err)
	tassert(t, ok, "stream mismatch")

	cntr.task.Delete(cntr.ctx)
	client.Close()

	return
}

// XXX use this as a starter for `client`
func echoTestSocket(t *testing.T, conn io.ReadWriteCloser, img, expect string) (err error) {

	txt := fmt.Sprintf("%s echo -n %s\n", img, shellescape.Quote(expect))
	msg, err := Parse(txt)
	tassert(t, err == nil, "%v", err)

	// the Encode() method takes the msg struct, marshals it into
	// a msgpack message, and writes it to the conn that we passed
	// into NewEncoder
	encoder := msgpack.NewEncoder(conn)
	err = encoder.Encode(msg)
	tassert(t, err == nil, "%v", err)

	// the Decode() method reads from conn and unmarshals the
	// msgpack message into msg.
	decoder := msgpack.NewDecoder(conn)

	// We expect two response messages; the first containing
	// io.Writers for the container's stdout and stderr, and the
	// second containing those as well as the container's exit code.
	var res Response

	// get stdio descriptors
	err = decoder.Decode(&res)
	tassert(t, err == nil, "%v", err)
	// because we get io.Writers from Response, we need to convert
	// those to io.Readers so we can read from them and check the
	// output.  We do this using a Pipe() and Copy() pattern as in
	// https://gist.github.com/stevegt/6d14dc97731b10b46bd79771d336a390
	stdout, stdoutw := io.Pipe()
	stderr, stderrw := io.Pipe()
	go func() {
		_, err = io.Copy(stdoutw, stdout)
		_, err = io.Copy(stderrw, stderr)
		stdoutw.Close()
		stderrw.Close()
	}()

	// XXX read stdout and stderr into buffers
	// var outbuf, errbuf []byte

	// XXX get rc Response

	// XXX ensure rc is zero

	// XXX compare stdout buf with expect

	// XXX ensure stderr buf is empty

	/*

		XXX old code from the pre-msgpack version of this function --
		mine this for e.g. outbuf and errbuf

		outbuf := make([]byte, len(expect))
		readn, err := stdoutr.Read(outbuf)
		tassert(t, err == nil, "%v", err)
		tassert(t, readn == len(expect), "expect %v bytes read, got %v", len(expect), readn)
		tassert(t, bytes.Compare([]byte(expect), outbuf) == 0, "expect %s, got %v", expect, string(outbuf))

		// XXX check stderr
		_ = stderrr
		// errbuf, err := ioutil.ReadAll(stderrr)
		// tassert(t, err == nil, "%v", err)
		// tassert(t, len(errbuf) == 0, "%v", string(errbuf))
	*/

	return
}

func TestImageSave(t *testing.T) {
	pit := setup(t)

	src := "docker.io/library/alpine:3.12.0"
	// pull container image and save it as a stream
	tree, err := pit.imageSave("sha256", src)
	tassert(t, err == nil, "%v", err)
	tassert(t, tree != nil, "%v", tree)

	// should be a tarball
	out, err := shellin(tree, "file", "-")
	tassert(t, err == nil, "%v", err)
	outstr := string(out)
	tassert(t, strings.Index(outstr, "POSIX tar archive") >= 0, outstr)

	// get the image from the pitbase stream we saved above
	addr := "tree/" + tree.Path.Addr
	err = echoTest(t, pit, addr, "hello")
	tassert(t, err == nil, "%v", err)

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
