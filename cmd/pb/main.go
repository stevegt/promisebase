package main

import (
	"fmt"

	"github.com/docopt/docopt-go"
)

func main() {
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
	arguments, _ := docopt.ParseDoc(usage)
	fmt.Println(arguments)
	fmt.Printf("speed is a %T", arguments["--speed"])
}
