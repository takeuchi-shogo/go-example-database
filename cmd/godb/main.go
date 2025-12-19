package main

import (
	"log"
	"os"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
	"github.com/takeuchi-shogo/go-example-database/internal/session"
	"github.com/takeuchi-shogo/go-example-database/pkg/repl"
)

func main() {
	dataDir := "data"
	catalog, err := catalog.NewCatalog(dataDir)
	if err != nil {
		log.Fatalf("Failed to create catalog: %v", err)
	}
	executor := executor.NewExecutor(catalog)
	session := session.NewSession(catalog, executor)
	defer session.Close()
	repl := repl.NewRepl(os.Stdin, os.Stdout, session)
	repl.Run()
}
