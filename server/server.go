package pit

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/fsnotify/fsnotify"
	"github.com/google/shlex"
	"github.com/stevegt/debugpipe"
	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase/db"
)

type ExistsError struct {
	Dir string
}

func (e *ExistsError) Error() string {
	return fmt.Sprintf("directory not empty: %s", e.Dir)
}

type Pit struct {
	Dir     string
	Db      *pb.Db
	watcher *fsnotify.Watcher
	Events  chan fsnotify.Event
}

func Create(dir string) (pit *Pit, err error) {
	defer Return(&err)

	// if directory exists, make sure it's empty
	if canstat(dir) {
		var files []os.FileInfo
		files, err = ioutil.ReadDir(dir)
		if len(files) > 0 {
			return nil, &ExistsError{Dir: dir}
		}
		Ck(err)
	}

	// create pit dir
	err = mkdir(dir, 1777)
	Ck(err)

	/*
		XXX turn this on and use it
		XXX this should be in /var/run
		// the ipc dir is where processes create sockets
		// - needs to be world writeable with sticky bit on
		err = mkdir(filepath.Join(dir, "ipc"), 1777)
		Ck(err)
	*/

	// create db dir tree
	_, err = pb.Db{Dir: dir}.Create()
	Ck(err)

	return Open(dir)
}

func Open(dir string) (pit *Pit, err error) {
	defer Return(&err)

	pit = &Pit{Dir: dir}

	db, err := pb.Open(dir)
	Ck(err)
	pit.Db = db

	// create a watcher
	pit.watcher, err = fsnotify.NewWatcher()
	Ck(err)

	pit.Events = pit.watcher.Events

	// watch the pit dir // XXX ipc
	err = pit.watcher.Add(pit.Dir)
	Ck(err)

	return pit, nil
}

// Listen on a new UNIX domain socket
// https://eli.thegreenplace.net/2019/unix-domain-sockets-in-go/
func (pit *Pit) Listen(id string) (listener net.Listener, err error) {
	fn := filepath.Join(pit.Dir, id)
	listener, err = net.Listen("unix", fn)
	Ck(err)
	return
}

// Connect to an existing UNIX domain socket
func (pit *Pit) Connect(id string) (conn io.ReadWriteCloser, err error) {
	fn := filepath.Join(pit.Dir, id)
	conn, err = net.Dial("unix", fn)
	return
}

// handle a single connection from a client
// XXX rehack to use msgpack
func (pit *Pit) handle(conn net.Conn) {
	rd := bufio.NewReader(conn)
	for {
		// read message from conn
		txt, err := rd.ReadString('\n')

		if err == io.EOF {
			break
		}
		Ck(err)

		// parse message
		msg, err := Parse(txt)
		Ck(err)

		// pass msg to runContainer
		out, rc, err := pit.runContainer(string(msg.Addr), []string(msg.Args)...)
		Ck(err)

		// return results to client
		// XXX demultiplex `out` on server side and repack in msgpack frames
		_, err = io.Copy(conn, out)
		Ck(err)

		// XXX send rc in msgpack frame
		// _, err = fmt.Fprint(conn, rc)
		// Ck(err)
		_ = rc

	}
}

// Serve requests on a UNIX domain socket
func (pit *Pit) Serve(fn string) (err error) {
	defer Return(&err)

	// listen on socket at fn
	listener, err := pit.Listen(fn)
	Ck(err)

	go func() {

		for {
			// accept connection from client
			conn, err := listener.Accept()
			Ck(err)

			// pass conn to handle()
			go pit.handle(conn)
		}

	}()
	return
}

type Addr string
type Callback func(Msg) error

type Dispatcher struct {
	callbacks map[Addr][]Callback
}

func NewDispatcher() *Dispatcher {

	m := make(map[Addr][]Callback)
	return &Dispatcher{callbacks: m}
}

// Register records callback as a function which Dispatch() will later
// call.
func (dp *Dispatcher) Register(callback Callback, addr Addr) {
	// fmt.Printf("before append ADDR: %v\n", dp.callbacks[addr])
	dp.callbacks[addr] = append(dp.callbacks[addr], callback)
	return
}

// Dispatch calls any functions that were previously registered with
// msg.Addr, passing msg as an argument to each function.
func (dp *Dispatcher) Dispatch(msg *Msg) (err error) {
	for _, callback := range dp.callbacks[msg.Addr] {
		err = callback(*msg)
	}
	return
}

type Msg struct {
	Addr Addr
	Args []string
}

// Parse splits txt and returns the parts in a Msg struct.
func Parse(txt string) (msg *Msg, err error) {
	defer Return(&err)
	// XXX rehack to use msgpack
	parts, err := shlex.Split(string(txt))
	Ck(err)
	// parts := strings.Fields(string(txt))
	ErrnoIf(len(parts) < 3, syscall.EINVAL, txt)
	msg = &Msg{}
	msg.Addr = Addr(parts[0])
	msg.Args = parts[1:]
	return
}

// PipeFd takes an io.Reader and returns the read end of a UNIX
// in-memory pipe -- see `man 2 pipe`.  We spawn a goroutine here to
// read from the io.Reader and write to the write end of the pipe.
func PipeFd(rd io.Reader) (fd uintptr, status chan error, err error) {
	defer Return(&err)
	rfile, wfile, err := os.Pipe()
	Ck(err)
	status = make(chan error)
	go func() {
		_, err := io.Copy(wfile, rd)
		status <- err
	}()
	fd = rfile.Fd()
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

func (pit *Pit) imageSave(algo, img string) (tree *pb.Tree, err error) {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	// pull container image
	pullrd, err := cli.ImagePull(ctx, img, types.ImagePullOptions{})
	if err != nil {
		panic(err)
	}
	io.Copy(os.Stdout, pullrd)

	// save image as a stream
	saverd, err := cli.ImageSave(ctx, []string{img})
	tree, err = pit.Db.PutStream(algo, saverd)

	return
}

func (pit *Pit) runContainer(img string, cmd ...string) (out io.ReadCloser, rc int, err error) {
	// XXX go get the runContainer() code from cmd/pb/main.go
	/// trace, debug := trace()

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	if strings.Index(img, "tree/") == 0 {
		path := pb.Path{}.New(pit.Db, img)
		tree, err := pit.Db.GetTree(path)
		Ck(err)
		defer tree.Close()

		var res types.ImageLoadResponse
		if true {
			res, err = cli.ImageLoad(ctx, tree, false)
			Ck(err)
		} else {
			pipeReader, pipeWriter := debugpipe.Pipe()
			go func() {
				_, err = io.Copy(pipeWriter, tree)
				Ck(err)
				err = pipeWriter.Close()
				Ck(err)
			}()
			res, err = cli.ImageLoad(ctx, pipeReader, false)
			Ck(err)
		}

		_, err = io.Copy(os.Stdout, res.Body)
		Ck(err)
		defer res.Body.Close()
	} else {
		reader, err := cli.ImagePull(ctx, img, types.ImagePullOptions{})
		if err != nil {
			panic(err)
		}
		io.Copy(os.Stdout, reader)
	}

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "alpine",
		Cmd:   cmd,
		Tty:   false,
	}, nil, nil, nil, "")
	if err != nil {
		panic(err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			panic(err)
		}
	case <-statusCh:
	}

	out, err = cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		panic(err)
	}

	return
}

func canstat(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func mkdir(dir string, mode os.FileMode) (err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, mode)
		if err != nil {
			return
		}
	}
	return
}
