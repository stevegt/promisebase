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
	Putworld   bool
	Getworld   bool
	Lsworld    bool
	Catworld   bool
	Putstream  bool
	Canon2path bool
	Path2canon bool
	Exec       bool
	Algo       string
	Key        string
	KeyLabel   []string `docopt:"<key_label>"`
	Name       string
	All        bool `docopt:"-a"`
	Out        bool `docopt:"-o"`
	Filename   string
	Arg        []string
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
  pb getblob <key>
  pb putnode <algo> <key_label>... 
  pb getnode <key>
  pb putworld <key> <name>
  pb getworld <name>
  pb lsworld [-a] <name>
  pb catworld <name> [-o <filename>] 
  pb putstream <algo> <name>
  pb canon2path <filename>
  pb path2canon <filename>
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
		key, err := putBlob(opts.Algo, &buf)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(key)
	case opts.Getblob:
		buf, err := getBlob(opts.Key)
		if err != nil || buf == nil {
			log.Error(err)
			return 42
		}
		_, err = os.Stdout.Write(*buf)
		if err != nil {
			log.Error(err)
			return 25
		}
	case opts.Putnode:
		key, err := putNode(opts.Algo, opts.KeyLabel)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(key)
	case opts.Getnode:
		node, err := getNode(opts.Key)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(node)
	case opts.Putworld:
		world, err := putWorld(opts.Key, opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		gotworld, err := getWorld(world.Name)
		if err != nil {
			log.Error(err)
			return 43
		}
		fmt.Printf("world/%s -> %s", gotworld.Name, gotworld.Db.KeyFromPath(gotworld.Src).Canon())
	case opts.Getworld:
		w, err := getWorld(opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(w.Db.KeyFromPath(w.Src).Canon())
	case opts.Lsworld:
		leafs, err := lsWorld(opts.Name, opts.All)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(strings.Join(leafs, ""))
	case opts.Catworld:
		buf, err := catWorld(opts.Name)
		if err != nil {
			log.Error(err)
			return 42
		}
		if opts.Out {
			err = ioutil.WriteFile(opts.Filename, *buf, 0644)
			if err != nil {
				log.Error(err)
				return 43
			}
		} else {
			fmt.Print(string(*buf))
		}
	case opts.Putstream:
		world, err := putStream(opts.Algo, opts.Name, os.Stdin)
		if err != nil {
			log.Error(err)
			return 42
		}
		gotworld, err := getWorld(world.Name)
		if err != nil {
			log.Error(err)
			return 43
		}
		_ = gotworld
		fmt.Printf("world/%s -> %s", gotworld.Name, gotworld.Db.KeyFromPath(gotworld.Src).Canon())
	case opts.Canon2path:
		path, err := canon2Path(opts.Filename)
		if err != nil {
			log.Error(err)
			return 42
		}
		fmt.Println(path)
	case opts.Path2canon:
		canon, err := path2Canon(opts.Filename)
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

		// XXX show stdout, stderr, rc
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

func putBlob(algo string, buf *[]byte) (key *pb.Key, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	key, err = db.PutBlob(algo, buf)
	if err != nil {
		return
	}
	return
}

func getBlob(keypath string) (buf *[]byte, err error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	db, err := opendb()
	if err != nil {
		return
	}
	key := db.KeyFromPath(keypath)
	buf, err = db.GetBlob(key)
	if err != nil {
		return
	}
	return
}

func putNode(algo string, keylabel []string) (keypath string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	var children []*pb.Node
	for _, kl := range keylabel {
		parts := strings.Split(kl, ",")
		k := db.KeyFromPath(parts[0])
		l := parts[1]
		child := &pb.Node{Db: db, Key: k, Label: l}
		children = append(children, child)
	}
	node, err := db.PutNode(algo, children...)
	if err != nil {
		return
	}
	keypath = node.Key.String()
	return
}

func getNode(keypath string) (node *pb.Node, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	key := db.KeyFromPath(keypath)
	node, err = db.GetNode(key)
	if err != nil {
		return
	}
	return
}

// $ pb putworld node/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2 world1
// world/world1 -> node/sha256/3d4f1ab0047e0b567fabe45acb91a239f9453d3a02bcb3047843d0040d43c8d2
func putWorld(keypath, name string) (world *pb.World, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	key := db.KeyFromPath(keypath)
	world, err = db.PutWorld(key, name)
	return
}

func getWorld(name string) (world *pb.World, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	world, err = db.GetWorld(name)
	if err != nil {
		return
	}
	return
}

func lsWorld(name string, all bool) (leafs []string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	world, err := db.GetWorld(name)
	if err != nil {
		return
	}
	nodes, err := world.Ls(all)
	for _, node := range nodes {
		entry := pb.NodeEntry{CanonPath: node.Key.String(), Label: node.Label}
		leafs = append(leafs, entry.String())
	}
	return
}

func catWorld(name string) (buf *[]byte, err error) {
	w, err := getWorld(name)
	if err != nil {
		return
	}
	buf, err = w.Cat()
	if err != nil {
		return
	}
	return
}

func putStream(algo string, name string, rd io.Reader) (world *pb.World, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	node, err := db.PutStream(algo, rd)
	if err != nil {
		return
	}
	world, err = db.PutWorld(node.Key, name)
	if err != nil {
		return
	}
	return
}

func canon2Path(canon string) (path string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	key := db.KeyFromPath(canon)
	// XXX verify hash?
	return key.Path(), nil
}

func path2Canon(path string) (canon string, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	key := db.KeyFromPath(canon)
	// XXX verify hash?
	return key.Canon(), nil
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
	// defer file.Close()
	_, err = ReadAtMost(file, buf)
	// fmt.Println(n, string(buf))

	// extract hash from buf (must start at first byte in stream, must be first
	// word ending with whitepace)
	re := regexp.MustCompile(`^\S+`)
	interpreterHash := string(re.Find(buf))
	algo := filepath.Dir(interpreterHash)
	// prepend "node/" to hash
	interpreterHash = "node/" + interpreterHash
	// fmt.Println(algo, interpreterHash)

	// XXX rewind file
	_, err = file.Seek(0, 0)
	if err != nil {
		return
	}

	// XXX send file to db.PutStream()
	rootnode, err := db.PutStream(algo, file)
	// XXX get scripthash from stream's root node key
	scriptHash := rootnode.Key.Hash
	// call xeq
	args = append([]string{scriptHash}, args...)
	stdout, stderr, rc, err = xeq(interpreterHash, args...)
	return
}

func xeq(interpreterHash string, args ...string) (stdout, stderr io.Reader, rc int, err error) {
	db, err := opendb()
	if err != nil {
		return
	}
	_ = db

	// cat node -- that's the interpreter code
	key := db.KeyFromPath(interpreterHash)
	node, err := db.GetNode(key)
	if err != nil {
		return
	}
	txt, err := node.Cat()
	if err != nil {
		return
	}
	// fmt.Println(string(*txt))

	// save interpreter in temporary file
	tempfn, err := WriteTempFile(*txt, 0700)
	if err != nil {
		return
	}
	defer os.Remove(tempfn) // clean up

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
