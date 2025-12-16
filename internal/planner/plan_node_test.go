package planner

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func TestScanNodeString(t *testing.T) {
	node := &ScanNode{
		TableName: "users",
	}

	expected := "Scan(users)"
	if node.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, node.String())
	}
}

func TestFilterNodeString(t *testing.T) {
	condition := &BinaryExpr{
		Left:     &ColumnRef{Name: "age"},
		Operator: ">",
		Right:    &Literal{Value: 20},
	}
	node := &FilterNode{
		Condition: condition,
		Child:     &ScanNode{TableName: "users"},
	}

	expected := "Filter((age > 20))"
	if node.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, node.String())
	}
}

func TestProjectNodeString(t *testing.T) {
	node := &ProjectNode{
		Columns: []string{"id", "name"},
		Child:   &ScanNode{TableName: "users"},
	}

	expected := "Project([id name])"
	if node.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, node.String())
	}
}

func TestInsertNodeString(t *testing.T) {
	node := &InsertNode{
		TableName: "users",
		Columns:   []string{"name", "age"},
	}

	expected := "Insert(users, [name age])"
	if node.String() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, node.String())
	}
}

func TestColumnRefEvaluate(t *testing.T) {
	// スキーマを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)

	// 行を作成
	values := []storage.Value{
		storage.StringValue("1"),
		storage.StringValue("alice"),
	}
	row := storage.NewRow(values)

	// ColumnRef を評価
	colRef := &ColumnRef{Name: "name"}
	result, err := colRef.Evaluate(row, schema)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result != "alice" {
		t.Errorf("Expected 'alice', got '%v'", result)
	}
}

func TestColumnRefEvaluateNotFound(t *testing.T) {
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	row := storage.NewRow([]storage.Value{storage.StringValue("1")})

	colRef := &ColumnRef{Name: "nonexistent"}
	_, err := colRef.Evaluate(row, schema)
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

func TestLiteralEvaluate(t *testing.T) {
	tests := []struct {
		name     string
		literal  *Literal
		expected any
	}{
		{"string", &Literal{Value: "hello"}, "hello"},
		{"int", &Literal{Value: 42}, 42},
		{"bool", &Literal{Value: true}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.literal.Evaluate(nil, nil)
			if err != nil {
				t.Fatalf("Evaluate failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBinaryExprEvaluateEquality(t *testing.T) {
	columns := []storage.Column{
		*storage.NewColumn("active", storage.ColumnTypeBool, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	row := storage.NewRow([]storage.Value{storage.BoolValue(true)})

	// active = true
	expr := &BinaryExpr{
		Left:     &ColumnRef{Name: "active"},
		Operator: "=",
		Right:    &Literal{Value: true},
	}

	result, err := expr.Evaluate(row, schema)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	if result != true {
		t.Errorf("Expected true, got %v", result)
	}
}

func TestBinaryExprEvaluateComparison(t *testing.T) {
	tests := []struct {
		name     string
		left     int
		operator string
		right    int
		expected bool
	}{
		{"less than true", 5, "<", 10, true},
		{"less than false", 10, "<", 5, false},
		{"greater than true", 10, ">", 5, true},
		{"greater than false", 5, ">", 10, false},
		{"less equal true", 5, "<=", 5, true},
		{"greater equal true", 10, ">=", 10, true},
		{"not equal true", 5, "!=", 10, true},
		{"not equal false", 5, "!=", 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &BinaryExpr{
				Left:     &Literal{Value: tt.left},
				Operator: tt.operator,
				Right:    &Literal{Value: tt.right},
			}

			result, err := expr.Evaluate(nil, nil)
			if err != nil {
				t.Fatalf("Evaluate failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBinaryExprEvaluateLogical(t *testing.T) {
	tests := []struct {
		name     string
		left     bool
		operator string
		right    bool
		expected bool
	}{
		{"AND true true", true, "AND", true, true},
		{"AND true false", true, "AND", false, false},
		{"AND false true", false, "AND", true, false},
		{"AND false false", false, "AND", false, false},
		{"OR true true", true, "OR", true, true},
		{"OR true false", true, "OR", false, true},
		{"OR false true", false, "OR", true, true},
		{"OR false false", false, "OR", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &BinaryExpr{
				Left:     &Literal{Value: tt.left},
				Operator: tt.operator,
				Right:    &Literal{Value: tt.right},
			}

			result, err := expr.Evaluate(nil, nil)
			if err != nil {
				t.Fatalf("Evaluate failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNodeChildren(t *testing.T) {
	scanNode := &ScanNode{TableName: "users"}
	filterNode := &FilterNode{
		Condition: &Literal{Value: true},
		Child:     scanNode,
	}
	projectNode := &ProjectNode{
		Columns: []string{"id"},
		Child:   filterNode,
	}

	// ScanNode has no children
	if len(scanNode.Children()) != 0 {
		t.Error("ScanNode should have no children")
	}

	// FilterNode has one child
	if len(filterNode.Children()) != 1 {
		t.Error("FilterNode should have one child")
	}

	// ProjectNode has one child
	if len(projectNode.Children()) != 1 {
		t.Error("ProjectNode should have one child")
	}
}
