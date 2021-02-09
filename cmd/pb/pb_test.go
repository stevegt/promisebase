package main

import (
	"flag"
	"testing"

	"github.com/google/go-cmdtest"
)

var update = flag.Bool("update", false, "update test files with results")

func TestCLI(t *testing.T) {
	ts, err := cmdtest.Read("testdata")
	if err != nil {
		t.Fatal(err)
	}
	ts.Commands["pb"] = cmdtest.InProcessProgram("pb", run)
	ts.Run(t, *update)
}
