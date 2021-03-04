package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmdtest"
	"github.com/pkg/fileutils"
)

var update = flag.Bool("update", false, "update test files with results")

func TestCLI(t *testing.T) {
	ts, err := cmdtest.Read("testdata")
	if err != nil {
		t.Fatal(err)
	}
	ts.KeepRootDirs = true
	srcdir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	ts.Setup = func(dir string) (err error) {
		err = fileutils.CopyFile("bigblob", filepath.Join(srcdir, "testdata/bigblob"))
		err = fileutils.CopyFile("lang1.sh", filepath.Join(srcdir, "testdata/lang1.sh"))
		err = fileutils.CopyFile("hello.lang1", filepath.Join(srcdir, "testdata/hello.lang1"))
		if err != nil {
			panic(err)
		}
		return
	}
	ts.Commands["pb"] = cmdtest.InProcessProgram("pb", run)
	ts.Commands["dd"] = cmdtest.Program("/bin/dd")
	ts.Commands["cmp"] = cmdtest.Program("/usr/bin/cmp")
	ts.Run(t, *update)
}
