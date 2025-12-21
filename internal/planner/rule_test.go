package planner

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func TestFilterPushDownRuleMatch(t *testing.T) {
	rule := NewFilterPushDownRule()

	usersSchema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("age", storage.ColumnTypeInt32, 0, false),
	})
	ordersSchema := storage.NewSchema("orders", []storage.Column{
		*storage.NewColumn("order_id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("user_id", storage.ColumnTypeInt32, 0, false),
	})

	// Filter -> Join should match
	filterJoin := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "age"},
			Operator: ">",
			Right:    &Literal{Value: 30},
		},
		Child: &JoinNode{
			Left:     &ScanNode{TableName: "users", TableSchema: usersSchema},
			Right:    &ScanNode{TableName: "orders", TableSchema: ordersSchema},
			JoinType: JoinTypeInner,
		},
	}
	if !rule.Match(filterJoin) {
		t.Error("expected Match to return true for Filter -> Join")
	}

	// Filter -> Scan should not match
	filterScan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "age"},
			Operator: ">",
			Right:    &Literal{Value: 30},
		},
		Child: &ScanNode{TableName: "users", TableSchema: usersSchema},
	}
	if rule.Match(filterScan) {
		t.Error("expected Match to return false for Filter -> Scan")
	}
}

func TestFilterPushDownRuleApplyLeft(t *testing.T) {
	rule := NewFilterPushDownRule()

	usersSchema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("age", storage.ColumnTypeInt32, 0, false),
	})
	ordersSchema := storage.NewSchema("orders", []storage.Column{
		*storage.NewColumn("order_id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("user_id", storage.ColumnTypeInt32, 0, false),
	})

	// Filter(age > 30) -> Join -> should push to left
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "age"},
			Operator: ">",
			Right:    &Literal{Value: 30},
		},
		Child: &JoinNode{
			Left:     &ScanNode{TableName: "users", TableSchema: usersSchema},
			Right:    &ScanNode{TableName: "orders", TableSchema: ordersSchema},
			JoinType: JoinTypeInner,
		},
	}

	result, err := rule.Apply(plan)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should be Join with Filter on left
	join, ok := result.(*JoinNode)
	if !ok {
		t.Fatalf("expected JoinNode, got %T", result)
	}

	// Left should be FilterNode
	filter, ok := join.Left.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode on left, got %T", join.Left)
	}

	// Filter's child should be ScanNode(users)
	scan, ok := filter.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", filter.Child)
	}
	if scan.TableName != "users" {
		t.Errorf("expected table 'users', got '%s'", scan.TableName)
	}

	// Right should be ScanNode(orders)
	rightScan, ok := join.Right.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode on right, got %T", join.Right)
	}
	if rightScan.TableName != "orders" {
		t.Errorf("expected table 'orders', got '%s'", rightScan.TableName)
	}
}

func TestFilterPushDownRuleApplyRight(t *testing.T) {
	rule := NewFilterPushDownRule()

	usersSchema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})
	ordersSchema := storage.NewSchema("orders", []storage.Column{
		*storage.NewColumn("order_id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("amount", storage.ColumnTypeInt32, 0, false),
	})

	// Filter(amount > 100) -> Join -> should push to right
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "amount"},
			Operator: ">",
			Right:    &Literal{Value: 100},
		},
		Child: &JoinNode{
			Left:     &ScanNode{TableName: "users", TableSchema: usersSchema},
			Right:    &ScanNode{TableName: "orders", TableSchema: ordersSchema},
			JoinType: JoinTypeInner,
		},
	}

	result, err := rule.Apply(plan)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	join, ok := result.(*JoinNode)
	if !ok {
		t.Fatalf("expected JoinNode, got %T", result)
	}

	// Left should be ScanNode(users)
	leftScan, ok := join.Left.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode on left, got %T", join.Left)
	}
	if leftScan.TableName != "users" {
		t.Errorf("expected table 'users', got '%s'", leftScan.TableName)
	}

	// Right should be FilterNode
	filter, ok := join.Right.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode on right, got %T", join.Right)
	}

	// Filter's child should be ScanNode(orders)
	scan, ok := filter.Child.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", filter.Child)
	}
	if scan.TableName != "orders" {
		t.Errorf("expected table 'orders', got '%s'", scan.TableName)
	}
}

func TestFilterPushDownRuleApplyBothTables(t *testing.T) {
	rule := NewFilterPushDownRule()

	usersSchema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})
	ordersSchema := storage.NewSchema("orders", []storage.Column{
		*storage.NewColumn("user_id", storage.ColumnTypeInt32, 0, false),
	})

	// Filter(id = user_id) -> Join -> cannot push (references both tables)
	plan := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "id"},
			Operator: "=",
			Right:    &ColumnRef{Name: "user_id"},
		},
		Child: &JoinNode{
			Left:     &ScanNode{TableName: "users", TableSchema: usersSchema},
			Right:    &ScanNode{TableName: "orders", TableSchema: ordersSchema},
			JoinType: JoinTypeInner,
		},
	}

	result, err := rule.Apply(plan)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should remain unchanged (Filter -> Join)
	filter, ok := result.(*FilterNode)
	if !ok {
		t.Fatalf("expected FilterNode (unchanged), got %T", result)
	}

	_, ok = filter.Child.(*JoinNode)
	if !ok {
		t.Fatalf("expected JoinNode as child, got %T", filter.Child)
	}
}

func TestConstantFoldingRuleName(t *testing.T) {
	rule := NewConstantFoldingRule()
	if rule.Name() != "constant folding" {
		t.Errorf("expected 'constant folding', got '%s'", rule.Name())
	}
}

func TestConstantFoldingRuleMatch(t *testing.T) {
	rule := NewConstantFoldingRule()

	// FilterNode with constant expression should match
	filterWithConst := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &Literal{Value: 1},
			Operator: "+",
			Right:    &Literal{Value: 1},
		},
		Child: &ScanNode{TableName: "users"},
	}
	if !rule.Match(filterWithConst) {
		t.Error("expected Match to return true for FilterNode with constant expression")
	}

	// FilterNode with column reference should not match
	filterWithColumn := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &ColumnRef{Name: "id"},
			Operator: "=",
			Right:    &Literal{Value: 1},
		},
		Child: &ScanNode{TableName: "users"},
	}
	if rule.Match(filterWithColumn) {
		t.Error("expected Match to return false for FilterNode with column reference")
	}

	// Non-FilterNode should not match
	scanNode := &ScanNode{TableName: "users"}
	if rule.Match(scanNode) {
		t.Error("expected Match to return false for non-FilterNode")
	}
}

func TestConstantFoldingRuleApplyArithmetic(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		left     any
		operator string
		right    any
		expected any
	}{
		{"add int", 1, "+", 2, 3},
		{"subtract int", 5, "-", 3, 2},
		{"multiply int", 4, "*", 3, 12},
		{"divide int", 10, "/", 2, 5},
		{"add int64", int64(100), "+", int64(200), int64(300)},
		{"divide by zero", 10, "/", 0, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &BinaryExpr{
				Left:     &Literal{Value: tt.left},
				Operator: tt.operator,
				Right:    &Literal{Value: tt.right},
			}
			result := rule.foldConstants(expr)
			lit, ok := result.(*Literal)
			if !ok {
				t.Fatalf("expected Literal, got %T", result)
			}
			if lit.Value != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, lit.Value)
			}
		})
	}
}

func TestConstantFoldingRuleApplyComparison(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		left     any
		operator string
		right    any
		expected bool
	}{
		{"equal true", 1, "=", 1, true},
		{"equal false", 1, "=", 2, false},
		{"not equal true", 1, "!=", 2, true},
		{"not equal false", 1, "!=", 1, false},
		{"less than true", 1, "<", 2, true},
		{"less than false", 2, "<", 1, false},
		{"greater than true", 2, ">", 1, true},
		{"greater than false", 1, ">", 2, false},
		{"less equal true", 1, "<=", 1, true},
		{"greater equal true", 2, ">=", 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &BinaryExpr{
				Left:     &Literal{Value: tt.left},
				Operator: tt.operator,
				Right:    &Literal{Value: tt.right},
			}
			result := rule.foldConstants(expr)
			lit, ok := result.(*Literal)
			if !ok {
				t.Fatalf("expected Literal, got %T", result)
			}
			if lit.Value != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, lit.Value)
			}
		})
	}
}

func TestConstantFoldingRuleApplyLogical(t *testing.T) {
	rule := &ConstantFoldingRule{}

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
			result := rule.foldConstants(expr)
			lit, ok := result.(*Literal)
			if !ok {
				t.Fatalf("expected Literal, got %T", result)
			}
			if lit.Value != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, lit.Value)
			}
		})
	}
}

func TestConstantFoldingRuleApplyNestedExpression(t *testing.T) {
	rule := &ConstantFoldingRule{}

	// (1 + 2) * 3 = 9
	expr := &BinaryExpr{
		Left: &BinaryExpr{
			Left:     &Literal{Value: 1},
			Operator: "+",
			Right:    &Literal{Value: 2},
		},
		Operator: "*",
		Right:    &Literal{Value: 3},
	}

	result := rule.foldConstants(expr)
	lit, ok := result.(*Literal)
	if !ok {
		t.Fatalf("expected Literal, got %T", result)
	}
	if lit.Value != 9 {
		t.Errorf("expected 9, got %v", lit.Value)
	}
}

func TestConstantFoldingRuleApplyAlwaysTrue(t *testing.T) {
	rule := NewConstantFoldingRule()

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	// WHERE 1 = 1 -> FilterNode should be removed
	filter := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &Literal{Value: 1},
			Operator: "=",
			Right:    &Literal{Value: 1},
		},
		Child: &ScanNode{TableName: "users", TableSchema: schema},
	}

	result, err := rule.Apply(filter)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should return ScanNode directly (FilterNode removed)
	scan, ok := result.(*ScanNode)
	if !ok {
		t.Fatalf("expected ScanNode, got %T", result)
	}
	if scan.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", scan.TableName)
	}
}

func TestConstantFoldingRuleApplyAlwaysFalse(t *testing.T) {
	rule := NewConstantFoldingRule()

	schema := storage.NewSchema("users", []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	})

	// WHERE 1 = 2 -> EmptyNode
	filter := &FilterNode{
		Condition: &BinaryExpr{
			Left:     &Literal{Value: 1},
			Operator: "=",
			Right:    &Literal{Value: 2},
		},
		Child: &ScanNode{TableName: "users", TableSchema: schema},
	}

	result, err := rule.Apply(filter)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Should return EmptyNode
	_, ok := result.(*EmptyNode)
	if !ok {
		t.Fatalf("expected EmptyNode, got %T", result)
	}
}

func TestIsAlwaysTrue(t *testing.T) {
	tests := []struct {
		name     string
		expr     Expression
		expected bool
	}{
		{"literal true", &Literal{Value: true}, true},
		{"literal false", &Literal{Value: false}, false},
		{"literal int", &Literal{Value: 1}, false},
		{"column ref", &ColumnRef{Name: "id"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAlwaysTrue(tt.expr)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsAlwaysFalse(t *testing.T) {
	tests := []struct {
		name     string
		expr     Expression
		expected bool
	}{
		{"literal false", &Literal{Value: false}, true},
		{"literal true", &Literal{Value: true}, false},
		{"literal int", &Literal{Value: 0}, false},
		{"column ref", &ColumnRef{Name: "id"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAlwaysFalse(tt.expr)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
