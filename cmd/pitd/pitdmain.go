package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/t7a/pitbase/db"
	"github.com/t7a/pitbase/fuse"

	"github.com/docopt/docopt-go"
	. "github.com/stevegt/goadapt"
)

const usage = `pitd

Usage:
  pitd init <dbdir> 
  pitd serve <dbdir> <mountpoint>

Options:
  -h --help     Show this screen.
  --version     Show version.
`

type Opts struct {
	Init       bool
	Serve      bool
	Dbdir      string
	Mountpoint string
}

func main() {
	rc, msg := Run()
	if len(msg) > 0 {
		fmt.Fprintf(os.Stderr, msg+"\n")
	}
	os.Exit(rc)
}

func Run() (rc int, msg string) {
	defer Halt(&rc, &msg)

	parser := &docopt.Parser{OptionsFirst: false}
	o, _ := parser.ParseArgs(usage, os.Args[1:], "0.0")
	var opts Opts
	err := o.Bind(&opts)
	Ck(err)

	dbdir := opts.Dbdir
	if dbdir == "" {
		dbdir = os.Getenv("DBDIR")
	}
	if dbdir == "" {
		dbdir, err = os.Getwd()
		Assert(err == nil, "can't get current directory")
	}

	if opts.Init {
		err := create(dbdir)
		Ck(err)
		fmt.Printf("Initialized empty database in %s", dbdir)
	}

	if opts.Serve {
		err := serve(dbdir, opts.Mountpoint)
		Ck(err)
	}

	return
}

func create(dir string) (err error) {
	defer Return(&err)
	_, err = pb.Db{Dir: dir}.Create()
	Ck(err)
	return
}

func serve(dbdir, mountpoint string) (err error) {
	defer Return(&err)

	var server *gofuse.Server

	// unmount on exit
	defer umount(server)

	// unmount on SIGINT or SIGTERM
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		umount(server)
		os.Exit(1)
	}()

	db, err := opendb(dbdir)
	Ck(err)

	server, err = fuse.Serve(db, mountpoint)
	Ck(err)
	server.Wait()

	return
}

func umount(server *gofuse.Server) {
	if server != nil {
		server.Unmount()
	}
}

func opendb(dir string) (db *pb.Db, err error) {
	defer Return(&err)
	db, err = pb.Open(dir)
	Ck(err)
	return
}
