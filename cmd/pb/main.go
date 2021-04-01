package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	pb "github.com/t7a/pitbase"

	"github.com/docopt/docopt-go"
)

func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
	logrus.SetReportCaller(true)
	formatter := &logrus.TextFormatter{
		CallerPrettyfier: caller(),
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyFile: "caller",
		},
	}
	formatter.TimestampFormat = "15:04:05.999999999"
	logrus.SetFormatter(formatter)
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
	Putnode    bool
	Getnode    bool
	Linkstream bool
	Getstream  bool
	Lsstream   bool
	Cattree    bool
	Catstream  bool
	Putstream  bool
	Canon2abs  bool
	Abs2canon  bool
	Exec       bool
	Algo       string
	Canpath    string
	Canpaths   []string
	Name       string
	All        bool `docopt:"-a"`
	Out        bool `docopt:"-o"`
	Filename   string
	Arg        []string
	Quiet      bool `docopt:"-q"`
}

func main() {
	// see https://github.com/google/go-cmdtest
	os.Exit(run())
}

func run() (rc int) {

	usage := `pitbase

Usage:
  pb init 
  pb putblob <algo>
  pb getblob <canpath>
  pb putnode <algo> <canpaths>... 
  pb getnode <canpath>
  pb linkstream <canpath> <name>
  pb getstream <name>
  pb lsstream [-a] <name>
  pb catstream <name> [-o <filename>] 
  pb cattree <canpath>
  pb putstream [-q] <algo> <name>
  pb canon2abs <filename>
  pb abs2canon <filename>
  pb exec <filename> [<arg>...]

Options:
  -h --help     Show this screen.
  --version     Show version.
`
	parser := &docopt.Parser{OptionsFirst: false}
	o, _ := parser.ParseArgs(usage, os.Args[1:], "0.0")
	var opts Opts
	err := o.Bind(&opts)
	if err != nil {
		log.Error(err)
		return 22
	}
	log.Debug(opts)
	// fmt.Printf("speed is a %T", arguments["--speed"])

	//putblob := optsBool("putblob")
	//getblob := optsBool("getblob")

	switch true {
	case opts.Init:
		msg, err := create()
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(msg)
	case opts.Putblob:
		buf, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Error(err)
			return 5
		}
		blob, err := putBlob(opts.Algo, buf)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(blob.Path.Canon)
	case opts.Getblob:
		buf, err := getBlob(opts.Canpath)
		if err != nil || buf == nil {
			log.Error(err)
			return 42
		}
		_, err = os.Stdout.Write(buf)
		if err != nil {
			log.Error(err)
			return 25
		}
	case opts.Putnode:
		node, err := putNode(opts.Algo, opts.Canpaths)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(node.Path.Canon)
	case opts.Getnode:
		node, err := getNode(opts.Canpath)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(node)
	case opts.Linkstream:
		stream, err := linkStream(opts.Canpath, opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		gotstream, err := getStream(stream.Label)
		if err != nil {
			log.Error(err)
			return 43
		}
		fmt.Printf("stream/%s -> %s\n", gotstream.Label, gotstream.RootNode.Path.Canon)
	case opts.Getstream:
		stream, err := getStream(opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(stream.RootNode.Path.Canon)
	case opts.Lsstream:
		canpaths, err := lsStream(opts.Name, opts.All)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(strings.Join(canpaths, "\n"))
	case opts.Catstream:
		buf, err := catStream(opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		if opts.Out {
			err = ioutil.WriteFile(opts.Filename, buf, 0644)
			if err != nil {
				log.Error(err)
				return 43
			}
		} else {
			fmt.Print(string(buf))
		}
	case opts.Cattree:
		buf, err := catTree(opts.Canpath)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Print(string(buf))
	case opts.Putstream:
		stream, err := putStream(opts.Algo, opts.Name, os.Stdin)
		if err != nil {
			log.Error(err)
			return 42
		}
		gotstream, err := getStream(stream.Label)
		if err != nil {
			log.Error(err)
			return 43
		}
		_ = gotstream
		if !opts.Quiet {
			fmt.Printf("stream/%s -> %s\n", gotstream.Label, gotstream.RootNode.Path.Canon)
		}
	case opts.Canon2abs:
		path, err := canon2abs(opts.Filename)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(path)
	case opts.Abs2canon:
		canon, err := abs2canon(opts.Filename)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(canon)
	case opts.Exec:
		stdout, stderr, rc, err := execute(opts.Filename, opts.Arg...)
		if err != nil {
			log.Error(err)
			return 42
		}

		// show stdout, stderr, rc
		_, err = io.Copy(os.Stdout, stdout)
		if err != nil {
			log.Error(err)
			return 42
		}

		_, err = io.Copy(os.Stderr, stderr)
		if err != nil {
			log.Error(err)
			return 42
		}
		_ = stdout
		_ = stderr
		_ = rc
	}
	return 0
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

func putBlob(algo string, buf []byte) (blob *pb.Blob, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	blob, err = db.PutBlob(algo, buf)
	if err != nil {
		return
	}
	return
}

func getBlob(canpath string) (buf []byte, err error) {
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

func putNode(algo string, canpaths []string) (node *pb.Node, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	var children []pb.Object
	for _, canpath := range canpaths {
		path := pb.Path{}.New(db, canpath)
		child := db.ObjectFromPath(path)
		children = append(children, child)
	}
	node, err = db.PutNode(algo, children...)
	if err != nil {
		return
	}
	return
}

func getNode(canpath string) (node *pb.Node, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	node, err = db.GetNode(path)
	if err != nil {
		return
	}
	return
}

// $ pb linkstream node/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2 stream1
// stream/stream1 -> node/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2
func linkStream(canpath, name string) (stream *pb.Stream, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	node, err := db.GetNode(path)
	if err != nil {
		return
	}
	stream, err = node.LinkStream(name)
	if err != nil {
		return
	}
	return
}

func getStream(name string) (stream *pb.Stream, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	stream, err = db.OpenStream(name)
	if err != nil {
		return
	}
	return
}

func lsStream(name string, all bool) (canpaths []string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	stream, err := db.OpenStream(name)
	if err != nil {
		return
	}
	objs, err := stream.Ls(all)
	for _, obj := range objs {
		// fmt.Printf("obj %#v\n", obj)
		canpath := obj.GetPath().Canon
		canpaths = append(canpaths, canpath)
	}
	return
}

func catStream(name string) (buf []byte, err error) {
	w, err := getStream(name)
	if err != nil {
		return
	}
	buf, err = w.Cat()
	if err != nil {
		return
	}
	return
}

func catTree(canpath string) (buf []byte, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	path := pb.Path{}.New(db, canpath)
	node, err := db.GetNode(path)
	if err != nil {
		return
	}
	buf, err = node.Cat()
	if err != nil {
		return
	}
	return
}

func putStream(algo string, name string, rd io.Reader) (stream *pb.Stream, err error) {
	// XXX add -q flag to keep it from printing output

	db, err := opendb()
	if err != nil {
		return
	}
	node, err := db.PutStream(algo, rd)
	if err != nil {
		return
	}
	if node == nil {
		panic("node is nil")
	}
	stream, err = node.LinkStream(name)
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

	// prepend "node/" to interpreter addr
	interpreterPath := pb.Path{}.New(db, "node/"+interpreterAddr)

	// rewind script file
	_, err = file.Seek(0, 0)
	if err != nil {
		return
	}

	// send script file to db.PutStream()
	rootnode, err := db.PutStream(algo, file)
	if err != nil {
		return
	}

	// get scriptCanon from script stream's root node path
	scriptCanon := rootnode.Path.Canon

	// call xeq
	args = append([]string{scriptCanon}, args...)
	stdout, stderr, rc, err = xeq(interpreterPath, args...)
	return
}

func xeq(interpreterPath *pb.Path, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	db, err := opendb()
	if err != nil {
		return
	}

	// cat the interpreter node to get the interpreter code
	node, err := db.GetNode(interpreterPath)
	if err != nil {
		return
	}
	txt, err := node.Cat()
	if err != nil {
		return
	}
	// fmt.Println(string(*txt))

	// save interpreter code in temporary file
	tempfn, err := WriteTempFile(txt, 0700)
	if err != nil {
		return
	}

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
	if err != nil {
		return
	}
	stderr, err = cmd.StderrPipe()
	if err != nil {
		return
	}

	err = cmd.Start()
	if err != nil {
		return
	}

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
