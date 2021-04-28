package pit

import (
	"io"
	"os"
	"strings"
	"syscall"

	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase"
)

type Addr string
type Callback func(Msg) error

type Dispatcher struct {
	callbacks map[Addr][]Callback
}

func NewDispatcher() *Dispatcher {
	m := make(map[Addr]Callback)
	return &Dispatcher{callbacks: m}
}

// Register records callback as a function which Dispatch() will later
// call.
func (dp *Dispatcher) Register(callback Callback, addr Addr) {
	dp.callbacks[addr] = callback
	return
}

// Dispatch calls any functions that were previously registered with
// msg.Addr, passing msg as an argument to each function.
func (dp *Dispatcher) Dispatch(msg *Msg) (err error) {
	callback := dp.callbacks[msg.Addr]
	err = callback(*msg)
	return
}

type Msg struct {
	Addr Addr
	Args []string
}

// Parse splits txt returns the parts in a Msg struct.
func Parse(txt Addr) (msg *Msg, err error) {
	parts := strings.Fields(string(txt))
	ErrnoIf(len(parts) < 3, syscall.EINVAL, txt)
	msg = &Msg{}
	msg.Addr = Addr(parts[0])
	msg.Args = parts[1:]
	return
}

// XXX copy most of the following functions from pb/main.go

func dbdir() (dir string, err error) {
	dir, ok := os.LookupEnv("PITDIR")
	if !ok {
		dir, err = os.Getwd()
	}
	return
}

func create() (msg string, err error) {
	return
}

func opendb() (db *pb.Db, err error) {
	return
}

func putBlob(algo string, rd io.Reader) (blob *pb.Blob, err error) {
	return
}

func getBlob(canpath string, wr io.Writer) (err error) {
	return
}

func putTree(algo string, canpaths []string) (tree *pb.Tree, err error) {
	return
}

func getTree(canpath string) (tree *pb.Tree, err error) {
	return
}

func linkStream(canpath, name string) (stream *pb.Stream, err error) {
	return
}

func getStream(name string) (stream *pb.Stream, err error) {
	return
}

func lsStream(name string, all bool) (canpaths []string, err error) {
	return
}

func catStream(name string) (stream *pb.Stream, err error) {
	return
}

func catTree(canpath string) (tree *pb.Tree, err error) {
	return
}

func putStream(algo string, name string, rd io.Reader) (stream *pb.Stream, err error) {
	return
}

func canon2abs(canpath string) (abspath string, err error) {
	return
}

func abs2canon(abspath string) (canpath string, err error) {
	return
}

func execute(scriptPath string, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	return
}

func xeq(interpreterPath *pb.Path, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	return
}

func runContainer(img string, cmd ...string) (stdout, stderr io.Reader, rc int, err error) {
	return
}
