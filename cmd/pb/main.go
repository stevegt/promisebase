package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docopt/docopt-go"
	log "github.com/sirupsen/logrus"
	"github.com/stevegt/debugpipe"
	. "github.com/stevegt/goadapt"
	pb "github.com/t7a/pitbase"
)

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
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, pb.GetGID())
	}
}

/*
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
*/

type Opts struct {
	Init       bool
	Putblob    bool
	Getblob    bool
	Puttree    bool
	Gettree    bool
	Linkstream bool
	Getstream  bool
	Lsstream   bool
	Cattree    bool
	Catstream  bool
	Putstream  bool
	Canon2abs  bool
	Abs2canon  bool
	Exec       bool
	Run        bool
	Algo       string
	Canpath    string
	Canpaths   []string
	Name       string
	All        bool `docopt:"-a"`
	Out        bool `docopt:"-o"`
	Filename   string
	Image      string
	Arg        []string
	Cmd        []string
	Quiet      bool `docopt:"-q"`
}

func main() {
	// see https://github.com/google/go-cmdtest
	os.Exit(run())
}

func run() (rc int) {
	rc, msg := _run()
	if len(msg) > 0 {
		fmt.Fprintf(os.Stderr, msg+"\n")
	}
	return rc
}

func _run() (rc int, msg string) {
	defer Halt(&rc, &msg)

	usage := `pitbase

Usage:
  pb init 
  pb putblob <algo>
  pb getblob <canpath>
  pb puttree <algo> <canpaths>... 
  pb gettree <canpath>
  pb linkstream <canpath> <name>
  pb getstream <name>
  pb lsstream [-a] <name>
  pb catstream <name> [-o <filename>] 
  pb cattree <canpath>
  pb putstream [-q] <algo> <name>
  pb canon2abs <filename>
  pb abs2canon <filename>
  pb exec <filename> [<arg>...]
  pb run <image> [<cmd>...]

Options:
  -h --help     Show this screen.
  --version     Show version.
`
	parser := &docopt.Parser{OptionsFirst: false}
	o, _ := parser.ParseArgs(usage, os.Args[1:], "0.0")
	var opts Opts
	err := o.Bind(&opts)
	Ck(err)
	log.Debugf("%#v", opts)
	// fmt.Printf("speed is a %T", arguments["--speed"])

	//putblob := optsBool("putblob")
	//getblob := optsBool("getblob")

	switch true {
	case opts.Init:
		msg, err := create()
		Ck(err)
		fmt.Println(msg)
	case opts.Putblob:
		blob, err := putBlob(opts.Algo, os.Stdin)
		ExitIf(err, syscall.ENOSYS)
		Ck(err)
		fmt.Println(blob.Path.Canon)
	case opts.Getblob:
		err := getBlob(opts.Canpath, os.Stdout)
		ExitIf(err, syscall.EINVAL)
		ExitIf(err, syscall.ENOENT)
		Ck(err)
	case opts.Puttree:
		tree, err := putTree(opts.Algo, opts.Canpaths)
		Ck(err)
		fmt.Println(tree.Path.Canon)
	case opts.Gettree:
		tree, err := getTree(opts.Canpath)
		Ck(err)
		fmt.Println(tree.Txt())
	case opts.Linkstream:
		stream, err := linkStream(opts.Canpath, opts.Name)
		Ck(err)
		gotstream, err := getStream(stream.Label)
		Ck(err)
		fmt.Printf("stream/%s -> %s\n", gotstream.Label, gotstream.RootNode.Path.Canon)
	case opts.Getstream:
		stream, err := getStream(opts.Name)
		ExitIf(err, syscall.ENOENT)
		Ck(err)
		fmt.Println(stream.RootNode.Path.Canon)
	case opts.Lsstream:
		canpaths, err := lsStream(opts.Name, opts.All)
		ExitIf(err, syscall.ENOENT)
		Ck(err)
		fmt.Println(strings.Join(canpaths, "\n"))
	case opts.Catstream:
		stream, err := catStream(opts.Name)
		Ck(err)
		if opts.Out {
			fh, err := os.OpenFile(opts.Filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644) // XXX perms
			_, err = io.Copy(fh, stream)
			Ck(err)
		} else {
			_, err = io.Copy(os.Stdout, stream)
			Ck(err)
		}
	case opts.Cattree:
		tree, err := catTree(opts.Canpath)
		Ck(err)
		_, err = io.Copy(os.Stdout, tree)
		Ck(err)
	case opts.Putstream:
		stream, err := putStream(opts.Algo, opts.Name, os.Stdin)
		Ck(err)
		gotstream, err := getStream(stream.Label)
		Ck(err)
		if !opts.Quiet {
			fmt.Printf("stream/%s -> %s\n", gotstream.Label, gotstream.RootNode.Path.Canon)
		}
	case opts.Canon2abs:
		path, err := canon2abs(opts.Filename)
		Ck(err)
		fmt.Println(path)
	case opts.Abs2canon:
		canon, err := abs2canon(opts.Filename)
		Ck(err)
		fmt.Println(canon)
	case opts.Exec:
		var stdout, stderr io.Reader
		stdout, stderr, rc, err = execute(opts.Filename, opts.Arg...)
		Ck(err)

		// show stdout, stderr, rc
		_, err = io.Copy(os.Stdout, stdout)
		Ck(err)

		_, err = io.Copy(os.Stderr, stderr)
		Ck(err)
	case opts.Run:
		var stdout, stderr io.Reader
		stdout, stderr, rc, err = runContainer(opts.Image, opts.Cmd...)
		Ck(err)

		// show stdout, stderr, rc
		/*
			_, err = io.Copy(os.Stdout, stdout)
			Ck(err)

			_, err = io.Copy(os.Stderr, stderr)
			Ck(err)
		*/
		_ = stdout
		_ = stderr
	}
	return 0, ""
}

func dbdir() (dir string) {
	dir = os.Getenv("DBDIR")
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			// XXX handling this better would mean that dbdir() needs
			// to return an err
			panic("can't get current directory")
		}
	}
	return
}

func create() (msg string, err error) {
	dir := dbdir()
	if err != nil {
		return
	}
	db, err := pb.Db{Dir: dir}.Create()
	if err != nil {
		return
	}
	return fmt.Sprintf("Initialized empty database in %s", db.Dir), nil
}

func opendb() (db *pb.Db, err error) {
	dir := dbdir()
	if err != nil {
		return
	}
	db, err = pb.Open(dir)
	if err != nil {
		return
	}
	return
}

func putBlob(algo string, rd io.Reader) (blob *pb.Blob, err error) {
	defer Return(&err)
	db, err := opendb()
	Ck(err)
	file, err := pb.CreateWORM(db, "blob", algo)
	ExitIf(err, syscall.ENOSYS)
	Ck(err)
	blob = pb.Blob{}.New(db, file)
	_, err = io.Copy(blob, rd)
	Ck(err)
	err = blob.Close()
	Ck(err)
	return
}

func getBlob(canpath string, wr io.Writer) (err error) {
	defer Return(&err)
	db, err := opendb()
	Ck(err)
	path := pb.Path{}.New(db, canpath)
	// XXX from here on down is the same as in putBlob and should be
	// moved to a common ioBlob(dst, src) (err error) {} function
	file, err := pb.OpenWORM(db, path)
	Ck(err)
	blob := pb.Blob{}.New(db, file)
	_, err = io.Copy(wr, blob)
	Ck(err)
	err = blob.Close()
	Ck(err)
	return
}

/*
func XXXgetBlob(canpath string) (buf []byte, err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	buf, err = db.GetBlob(path)
	if err != nil {
		return
	}
	return
}
*/

func putTree(algo string, canpaths []string) (tree *pb.Tree, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	var children []pb.Object
	for _, canpath := range canpaths {
		path := pb.Path{}.New(db, canpath)
		child, err := db.ObjectFromPath(path)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	tree, err = db.PutTree(algo, children...)
	if err != nil {
		return
	}
	return
}

func getTree(canpath string) (tree *pb.Tree, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	tree, err = db.GetTree(path)
	if err != nil {
		return
	}
	return
}

// $ pb linkstream tree/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2 stream1
// stream/stream1 -> tree/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2
func linkStream(canpath, name string) (stream *pb.Stream, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	tree, err := db.GetTree(path)
	if err != nil {
		return
	}
	stream, err = tree.LinkStream(name)
	if err != nil {
		return
	}
	return
}

func getStream(name string) (stream *pb.Stream, err error) {
	defer Return(&err)
	db, err := opendb()
	Ck(err)
	stream, err = db.OpenStream(name)
	Ck(err)
	return
}

func lsStream(name string, all bool) (canpaths []string, err error) {
	defer Return(&err)
	db, err := opendb()
	Ck(err)
	stream, err := db.OpenStream(name)
	Ck(err)
	objs, err := stream.Ls(all)
	for _, obj := range objs {
		// fmt.Printf("obj %#v\n", obj)
		canpath := obj.GetPath().Canon
		canpaths = append(canpaths, canpath)
	}
	return
}

func catStream(name string) (stream *pb.Stream, err error) {
	defer Return(&err)
	stream, err = getStream(name)
	ExitIf(err, syscall.ENOENT)
	Ck(err)
	return
}

func catTree(canpath string) (tree *pb.Tree, err error) {
	defer Return(&err)
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	tree, err = db.GetTree(path)
	Ck(err)
	return
}

func putStream(algo string, name string, rd io.Reader) (stream *pb.Stream, err error) {
	// XXX add -q flag to keep it from printing output

	db, err := opendb()
	if err != nil {
		return
	}
	tree, err := db.PutStream(algo, rd)
	if err != nil {
		return
	}
	if tree == nil {
		panic("tree is nil")
	}
	stream, err = tree.LinkStream(name)
	if err != nil {
		return
	}
	return
}

func canon2abs(canpath string) (abspath string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	return path.Abs, nil
}

func abs2canon(abspath string) (canpath string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, abspath)
	return path.Canon, nil
}

func execute(scriptPath string, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	// read first kilobyte of file at path
	buf := make([]byte, 1024)
	file, err := os.Open(scriptPath)
	if err != nil {
		return
	}
	defer file.Close()
	_, err = ReadAtMost(file, buf)
	// fmt.Println(n, string(buf))

	// extract interpreter addr from buf (must start at first byte in
	// stream, must be first word ending with whitepace)
	re := regexp.MustCompile(`^\S+`)
	interpreterAddr := string(re.Find(buf))

	// get interpreter hash algorithm
	algo := filepath.Dir(interpreterAddr)
	// fmt.Printf("algo!! %s\n", algo)

	// prepend "tree/" to interpreter addr
	interpreterPath := pb.Path{}.New(db, "tree/"+interpreterAddr)

	// rewind script file
	_, err = file.Seek(0, 0)
	if err != nil {
		return
	}

	// send script file to db.PutStream()
	roottree, err := db.PutStream(algo, file)
	if err != nil {
		return
	}

	// get scriptCanon from script stream's root tree path
	scriptCanon := roottree.Path.Canon

	// call xeq
	args = append([]string{scriptCanon}, args...)
	stdout, stderr, rc, err = xeq(interpreterPath, args...)
	return
}

func xeq(interpreterPath *pb.Path, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	defer Return(&err)
	db, err := opendb()
	Ck(err)

	// open a temporary file for the interpreter code
	fh, err := ioutil.TempFile("", "*")
	Ck(err)
	tempfn := fh.Name()

	// copy the interpreter into the temp file
	tree, err := db.GetTree(interpreterPath)
	Ck(err)
	_, err = io.Copy(fh, tree)
	Ck(err)
	err = fh.Close()
	Ck(err)
	err = os.Chmod(tempfn, 0700)
	Ck(err)

	// XXX do not uncomment the defer() below
	// XXX redo the xeq api to have more of a io.Reader and Writer interface
	// that we can close to let it know when it can delete the tempfile
	// defer os.Remove(tempfn) // clean up

	// pass the hash of the script and the remaining args to the
	//interpreter, and let the interpreter fetch the script from the db
	//
	// hash_of_interpreter hash_of_script arg1 arg2 arg3
	cmd := exec.Command(tempfn, args...)
	stdout, err = cmd.StdoutPipe()
	Ck(err)
	stderr, err = cmd.StderrPipe()
	Ck(err)

	err = cmd.Start()
	Ck(err)

	return
}

func ReadAtMost(r io.Reader, buf []byte) (n int, err error) {
	max := len(buf)
	for n < max && err == nil {
		var nread int
		nread, err = r.Read(buf[n:])
		n += nread
	}
	// XXX make sure we're handling EOF and other errors right
	if err == io.EOF {
		err = nil
	}
	return
}

func WriteTempFile(data []byte, mode os.FileMode) (filename string, err error) {
	tmpfile, err := ioutil.TempFile("", "pb")
	if err != nil {
		return
	}

	filename = tmpfile.Name()
	_, err = tmpfile.Write(data)
	if err != nil {
		return
	}

	err = tmpfile.Close()
	if err != nil {
		return
	}

	// set mode bits
	err = os.Chmod(filename, mode)
	if err != nil {
		return
	}

	return
}

func runContainer(img string, cmd ...string) (stdout, stderr io.Reader, rc int, err error) {

	/// trace, debug := trace()

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	if strings.Index(img, "tree/") == 0 {
		tree, err := catTree(img)
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

	out, err := cli.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		panic(err)
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	return
}
