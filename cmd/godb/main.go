package main

import (
	"os"

	"github.com/takeuchi-shogo/go-example-database/pkg/repl"
)

func main() {
	repl := repl.NewRepl(os.Stdin, os.Stdout)
	repl.Run()
}
