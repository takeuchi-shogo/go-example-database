package session

import (
	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
	"github.com/takeuchi-shogo/go-example-database/internal/parser"
	"github.com/takeuchi-shogo/go-example-database/internal/planner"
)

type Session interface {
	Execute(sqlQuery string) (executor.ResultSet, error)
	Close() error
}

type session struct {
	catalog  catalog.Catalog
	executor executor.Executor
	planner  planner.Planner
}

func NewSession(catalog catalog.Catalog, executor executor.Executor) Session {
	return &session{catalog: catalog, executor: executor, planner: planner.NewPlanner(catalog)}
}

func (s *session) Execute(sqlQuery string) (executor.ResultSet, error) {
	// 1. SQL を解析して PlanNode を作成
	p := parser.NewParser(parser.NewLexer(sqlQuery))
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
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
