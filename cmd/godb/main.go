package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/dbtxn"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
	"github.com/takeuchi-shogo/go-example-database/internal/session"
	"github.com/takeuchi-shogo/go-example-database/pkg/repl"
)

func main() {
	dataDir := "data"

	// カタログを作成
	catalog, err := catalog.NewCatalog(dataDir)
	if err != nil {
		log.Fatalf("Failed to create catalog: %v", err)
	}

	// WAL を作成
	walPath := filepath.Join(dataDir, "wal.log")
	wal, err := dbtxn.NewWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Executor と Session を作成
	executor := executor.NewExecutor(catalog, wal)
	session := session.NewSession(catalog, executor, wal)
	defer session.Close()

	// REPL を起動
	repl := repl.NewRepl(os.Stdin, os.Stdout, session)
	repl.Run()
}
