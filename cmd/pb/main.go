package main

import (
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	pb "github.com/t7a/pitbase"

	"github.com/docopt/docopt-go"
)

func main() {
	// see https://github.com/google/go-cmdtest
	os.Exit(run())
}

func run() (rc int) {

	usage := `pitbase

Usage:
  pb putblob <algo>
  pb getblob <key>
  pb putnode <key_label> ... 
  pb getnode <key>
  pb putworld <key> <name>
  pb getworld <name>

Options:
  -h --help     Show this screen.
  --version     Show version.
`
	opts, _ := docopt.ParseDoc(usage)
	// fmt.Println(opts)
	// fmt.Printf("speed is a %T", arguments["--speed"])

	putblob, err := opts.Bool("putblob")
	if err != nil {
		return 22
	}

	if putblob {
		algo, err := opts.String("<algo>")
		if err != nil {
			log.Error(err)
			return 22
		}
		buf, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Error(err)
			return 5
		}
		key, err := putBlob(algo, &buf)
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
