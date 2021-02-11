package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
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
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, getGID())
	}
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

type Opts struct {
	Putblob  bool
	Getblob  bool
	Putnode  bool
	Getnode  bool
	Putworld bool
	Getworld bool
	Algo     string
	Key      string
	KeyLabel []string `docopt:"<key_label>"`
	Name     string
}

func main() {
	// see https://github.com/google/go-cmdtest
	os.Exit(run())
}

func run() (rc int) {

	usage := `pitbase

Usage:
  pb putblob <algo>
  pb getblob <key>
  pb putnode <algo> <key_label>... 
  pb getnode <key>
  pb putworld <key> <name>
  pb getworld <name>

Options:
  -h --help     Show this screen.
  --version     Show version.
`
	o, _ := docopt.ParseDoc(usage)
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
		if err != nil {
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
	}

	return 0
}

func putBlob(algo string, buf *[]byte) (key *pb.Key, err error) {
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	db, err := pb.Open(dir)
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
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	db, err := pb.Open(dir)
	if err != nil {
		return
	}
	key := pb.KeyFromPath(keypath)
	buf, err = db.GetBlob(key)
	if err != nil {
		return
	}
	return
}

func putNode(algo string, keylabel []string) (keypath string, err error) {
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	db, err := pb.Open(dir)
	if err != nil {
		return
	}
	var children []*pb.Node
	for _, kl := range keylabel {
		parts := strings.Split(kl, ",")
		k := pb.KeyFromPath(parts[0])
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

/*
func optsbool(name string) (opt bool) {
	opt, err := opts.Bool(name)
	if err != nil {
		panic(err)
	}
	return
}
*/
