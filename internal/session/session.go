package session

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/dbtxn"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
	"github.com/takeuchi-shogo/go-example-database/internal/parser"
	"github.com/takeuchi-shogo/go-example-database/internal/planner"
)

type Session interface {
	Execute(sqlQuery string) (executor.ResultSet, error)
	Close() error
}

type session struct {
	catalog    catalog.Catalog
	executor   executor.Executor
	planner    planner.Planner
	wal        *dbtxn.WAL
	txnManager *dbtxn.TxnManager
	currentTxn *dbtxn.Transaction
}

func NewSession(catalog catalog.Catalog, executor executor.Executor, wal *dbtxn.WAL) Session {
	txnManager := dbtxn.NewTxnManager(wal)
	return &session{
		catalog:    catalog,
		executor:   executor,
		planner:    planner.NewPlanner(catalog),
		wal:        wal,
		txnManager: txnManager,
		currentTxn: nil,
	}
}

func (s *session) Execute(sqlQuery string) (executor.ResultSet, error) {
	stmt, err := parser.NewParser(parser.NewLexer(sqlQuery)).Parse()
	if err != nil {
		return nil, err
	}
	switch stmt.(type) {
	case *parser.BeginStatement:
		return s.Begin()
	case *parser.CommitStatement:
		return s.Commit()
	case *parser.RollbackStatement:
		return s.Rollback()
	default:
		return s.executeSQL(stmt)
	}
}

func (s *session) executeSQL(stmt parser.Statement) (executor.ResultSet, error) {
	// 1. Statement を PlanNode に変換
	plan, err := s.planner.Plan(stmt)
	if err != nil {
		return nil, err
	}
	// 2. PlanNode を実行して結果を返す
	result, err := s.executor.Execute(plan)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *session) Close() error {
	return s.catalog.Close()
}

func (s *session) Begin() (executor.ResultSet, error) {
	if s.currentTxn != nil {
		return nil, fmt.Errorf("transaction already started")
	}
	txn, err := s.txnManager.Begin()
	if err != nil {
		return nil, err
	}
	s.currentTxn = txn
	s.executor.SetTxnID(txn.ID)
	return executor.NewResultSetWithMessage("BEGIN transaction successfully"), nil
}

func (s *session) Commit() (executor.ResultSet, error) {
	if s.currentTxn == nil {
		return nil, fmt.Errorf("no transaction to commit")
	}
	err := s.txnManager.Commit(s.currentTxn)
	if err != nil {
		return nil, err
	}
	s.currentTxn = nil
	s.executor.SetTxnID(0)
	return executor.NewResultSetWithMessage("COMMIT transaction successfully"), nil
}

func (s *session) Rollback() (executor.ResultSet, error) {
	if s.currentTxn == nil {
		return nil, fmt.Errorf("no transaction to rollback")
	}
	err := s.txnManager.Rollback(s.currentTxn)
	if err != nil {
		return nil, err
	}
	s.currentTxn = nil
	s.executor.SetTxnID(0)
	return executor.NewResultSetWithMessage("ROLLBACK transaction successfully"), nil
}
