package planner

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func TestOptimizerConstantFolding(t *testing.T) {
	optimizer := NewOptimizer(nil, nil)

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	// WHERE 1 + 1 = 2 -> should be optimized away
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left: &BinaryExpr{
				Left:     &Literal{Value: 1},
				Operator: "+",
				Right:    &Literal{Value: 1},
			},
			Operator: "=",
			Right:    &Literal{Value: 2},
		},
		Child: &ScanNode{TableName: "users", TableSchema: schema},
	}

	result, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// FilterNode should be removed (1+1=2 is always true)
	scan, ok := result.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", result)
	}
	if scan.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", scan.TableName)
	}
}

func TestOptimizerConstantFoldingFalse(t *testing.T) {
	optimizer := NewOptimizer(nil, nil)

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	// WHERE 1 = 2 -> should return EmptyNode
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &Literal{Value: 1},
			Operator: "=",
			Right:    &Literal{Value: 2},
		},
		Child: &ScanNode{TableName: "users", TableSchema: schema},
	}

	result, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Should return EmptyNode
	_, ok := result.(*EmptyNode)
	if !ok {
		t.Fatalf("expected EmptyNode, got %T", result)
	}
}

func TestOptimizerNoOptimization(t *testing.T) {
	optimizer := NewOptimizer(nil, nil)

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	// WHERE id = 1 -> cannot be optimized (contains column reference)
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "id"},
			Operator: "=",
			Right:    &Literal{Value: 1},
		},
		Child: &ScanNode{TableName: "users", TableSchema: schema},
	}

	result, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// FilterNode should remain
	filter, ok := result.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode, got %T", result)
	}
	if filter.Condition == nil {
		t.Error("expected Condition to be present")
	}
}

func TestOptimizerNestedNodes(t *testing.T) {
	optimizer := NewOptimizer(nil, nil)

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	})

	// Project -> Filter (1=1) -> Scan
	// Filter should be removed
	plan := &ProjectNode{
		Columns: []string{"name"},
		Child: &FilterNode{
			Condition: &BinaryExpr{
				Left:     &Literal{Value: 1},
				Operator: "=",
				Right:    &Literal{Value: 1},
			},
			Child: &ScanNode{TableName: "users", TableSchema: schema},
		},
	}

	result, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Should be Project -> Scan (Filter removed)
	project, ok := result.(*ProjectNode)
	if !ok {
		t.Fatalf("expected ProjectNode, got %T", result)
	}

	_, ok = project.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode as child, got %T", project.Child)
	}
}

func TestOptimizerScanNode(t *testing.T) {
	optimizer := NewOptimizer(nil, nil)

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	plan := &ScanNode{TableName: "users", TableSchema: schema}

	result, err := optimizer.Optimize(plan)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	scan, ok := result.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", result)
	}
	if scan.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", scan.TableName)
	}
}
