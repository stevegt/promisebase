package pit

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/google/shlex"
	log "github.com/sirupsen/logrus"
	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase/db"
	"github.com/vmihailenco/msgpack"
)

// XXX init(), caller(), and GetGID() are copies of the same from
// pitbase.go and all should be moved to a common lib
func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
	log.SetReportCaller(true)
	formatter := &log.TextFormatter{
		CallerPrettyfier: caller(),
		FieldMap: log.FieldMap{
			log.FieldKeyFile: "caller",
		},
	}
	formatter.TimestampFormat = "15:04:05.999999999"
	log.SetFormatter(formatter)
}

// caller returns string presentation of log caller which is formatted as
// `/path/to/file.go:line_number`. e.g. `/internal/app/api.go:25`
// https://stackoverflow.com/questions/63658002/is-it-possible-to-wrap-logrus-logger-functions-without-losing-the-line-number-pr
func caller() func(*runtime.Frame) (function string, file string) {
	return func(f *runtime.Frame) (function string, file string) {
		p, _ := os.Getwd()
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, GetGID())
	}
}

// GetGID returns the goroutine ID of its calling function, for logging purposes.
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

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
	log.Debugf("client connecting %s", fn)
	conn, err = net.Dial("unix", fn)
	return
}

// handle a single connection from a client
func (pit *Pit) handle(conn net.Conn) {
	log.Debugf("handling conn")

	var req Request
	var res Response
	decoder := msgpack.NewDecoder(conn)
	encoder := msgpack.NewEncoder(conn)
	for {
		// read message from conn
		log.Debugf("reading request")
		err := decoder.Decode(&req)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("decode: %v", err)
		}
		log.Debugf("got request %#v", req)

		// pass req to runContainer
		cntr := &Container{
			Image: string(req.Addr),
			Args:  []string(req.Args),
			Cmd: &exec.Cmd{
				Stdin:  conn,
				Stdout: conn,
				Stderr: os.Stderr,
			},
		}

		err = pit.startContainer(cntr)
		if err != nil {
			log.Errorf("startContainer: %v", err)
		}

		// return results to client
		// status := <-statusChan
		// XXX populate res
		// XXX send rc in msgpack Response
		err = encoder.Encode(res)
		if err != nil {
			log.Errorf("encode: %v", err)
		}

	}
}

// Serve requests on a UNIX domain socket
func (pit *Pit) Serve(fn string) (err error) {
	defer Return(&err)

	// listen on socket at fn
	log.Debugf("listening on %s", fn)
	listener, err := pit.Listen(fn)
	Ck(err)

	go func() {

		for {
			// accept connection from client
			log.Debugf("accepting on %s", fn)
			conn, err := listener.Accept()
			if err != nil {
				log.Errorf("accept: %v", err)
			}

			// pass conn to handle()
			go pit.handle(conn)
		}

	}()
	return
}

type Addr string
type Callback func(Request) error

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
// req.Addr, passing req as an argument to each function.
func (dp *Dispatcher) Dispatch(req *Request) (err error) {
	for _, callback := range dp.callbacks[req.Addr] {
		err = callback(*req)
	}
	return
}

type Request struct {
	Addr Addr
	Args []string
}

// Parse splits txt and returns the parts in a Request struct.
func Parse(txt string) (req *Request, err error) {
	defer Return(&err)
	parts, err := shlex.Split(string(txt))
	Ck(err)
	// parts := strings.Fields(string(txt))
	ErrnoIf(len(parts) < 3, syscall.EINVAL, txt)
	req = &Request{}
	req.Addr = Addr(parts[0])
	req.Args = parts[1:]
	return
}

func (req *Request) Compare(b *Request) (ok bool) {
	if req.Addr != b.Addr {
		return false
	}
	if len(req.Args) != len(b.Args) {
		return false
	}
	for i, arg := range req.Args {
		if arg != b.Args[i] {
			return false
		}
	}
	return true
}

const (
	RUNNING = iota
	DONE
)

type Response struct {
	Stdout io.Writer
	Stderr io.Writer
	Rc     int
	Status int
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

// r2w converts an io.Reader to an io.Writer
func r2w(rd io.ReadCloser) (wr io.WriteCloser, errchan chan error) {
	errchan = make(chan error)
	go func() {
		_, err := io.Copy(wr, rd)
		if err != nil {
			errchan <- err
		}
		err = wr.Close()
		if err != nil {
			errchan <- err
		}
	}()
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
	tmpfile, err := ioutil.TempFile("", "*.oci")
	Ck(err)
	path := tmpfile.Name()
	defer os.Remove(path)
	cmd := exec.Command("skopeo", "copy", img, fmt.Sprintf("oci-archive:%s", path))
	fmt.Println(cmd.Args)
	// fmt.Println(tmpfile.Name(), dest)
	err = cmd.Run()
	Ck(err)
	imgrd, err := os.Open(path)
	Ck(err)
	tree, err = pit.Db.PutStream(algo, imgrd)
	Ck(err)

	/*
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
	*/
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
