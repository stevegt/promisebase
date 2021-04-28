package pit

import (
	"io"
	"os"
	"strings"

	pb "github.com/t7a/pitbase"
)

type Msg struct {
	Addr string
	Args []string
}

// Parse splits txt returns the parts in a Msg struct.
func Parse(txt string) (msg Msg, err error) {
	parts := strings.Split(txt, " ")
	msg.Addr = parts[0]
	msg.Args = parts[1:]
	return
}

// XXX copy most of the following functions from pb/main.go

func dbdir() (dir string) {
	dir, _ = os.LookupEnv("PITDIR")
	if dir == "" {
		dir, _ = os.Getwd()
	}
	// XXX cannot not handle errors in this design. consider revising
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
