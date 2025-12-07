package parser

import (
	"testing"
)

func TestParser_SelectAll(t *testing.T) {
	input := "SELECT * FROM users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt, ok := stmt.(*selectStatement)
	if !ok {
		t.Fatalf("expected *selectStatement, got %T", stmt)
	}

	if selectStmt.from != "users" {
		t.Errorf("expected from='users', got %q", selectStmt.from)
	}

	if len(selectStmt.columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(selectStmt.columns))
	}

	if _, ok := selectStmt.columns[0].(*asterisk); !ok {
		t.Errorf("expected asterisk, got %T", selectStmt.columns[0])
	}
}

func TestParser_SelectColumns(t *testing.T) {
	input := "SELECT id, name, age FROM users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt := stmt.(*selectStatement)

	if len(selectStmt.columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(selectStmt.columns))
	}

	expectedCols := []string{"id", "name", "age"}
	for i, col := range selectStmt.columns {
		ident, ok := col.(*identifier)
		if !ok {
			t.Errorf("column[%d]: expected identifier, got %T", i, col)
			continue
		}
		if ident.value != expectedCols[i] {
			t.Errorf("column[%d]: expected %q, got %q", i, expectedCols[i], ident.value)
		}
	}
}

func TestParser_SelectWithWhere(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt, ok := stmt.(*selectStatement)
	if !ok {
		t.Fatalf("expected *selectStatement, got %T", stmt)
	}

	if selectStmt.where == nil {
		t.Fatal("expected where clause")
	}

	binExpr, ok := selectStmt.where.(*binaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", selectStmt.where)
	}

	if binExpr.operator != "=" {
		t.Errorf("expected operator '=', got %q", binExpr.operator)
	}

	left, ok := binExpr.left.(*identifier)
	if !ok || left.value != "id" {
		t.Errorf("expected left to be identifier{id}, got %v", binExpr.left)
	}

	right, ok := binExpr.right.(*integerLiteral)
	if !ok || right.value != 1 {
		t.Errorf("expected right to be integerLiteral{1}, got %v", binExpr.right)
	}
}

func TestParser_SelectWithOrderBy(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name DESC"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt := stmt.(*selectStatement)

	if len(selectStmt.orderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(selectStmt.orderBy))
	}

	if selectStmt.orderBy[0].column != "name" {
		t.Errorf("expected OrderBy column='name', got %q", selectStmt.orderBy[0].column)
	}

	if selectStmt.orderBy[0].asc {
		t.Error("expected OrderBy asc=false (DESC)")
	}
}

func TestParser_SelectWithLimit(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt := stmt.(*selectStatement)

	if selectStmt.limit == nil {
		t.Fatal("expected limit to be set")
	}

	if *selectStmt.limit != 10 {
		t.Errorf("expected limit=10, got %d", *selectStmt.limit)
	}
}

func TestParser_Insert(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice')"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	insertStmt, ok := stmt.(*insertStatement)
	if !ok {
		t.Fatalf("expected *insertStatement, got %T", stmt)
	}

	if insertStmt.tableName != "users" {
		t.Errorf("expected tableName='users', got %q", insertStmt.tableName)
	}

	if len(insertStmt.columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(insertStmt.columns))
	}

	if insertStmt.columns[0] != "id" || insertStmt.columns[1] != "name" {
		t.Errorf("expected columns [id, name], got %v", insertStmt.columns)
	}

	if len(insertStmt.values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(insertStmt.values))
	}

	// 最初の値が整数リテラル
	intVal, ok := insertStmt.values[0].(*integerLiteral)
	if !ok {
		t.Errorf("expected values[0] to be integerLiteral, got %T", insertStmt.values[0])
	} else if intVal.value != 1 {
		t.Errorf("expected values[0]=1, got %d", intVal.value)
	}

	// 2番目の値が文字列リテラル
	strVal, ok := insertStmt.values[1].(*stringLiteral)
	if !ok {
		t.Errorf("expected values[1] to be stringLiteral, got %T", insertStmt.values[1])
	} else if strVal.value != "Alice" {
		t.Errorf("expected values[1]='Alice', got %q", strVal.value)
	}
}

func TestParser_Update(t *testing.T) {
	input := "UPDATE users SET name = 'Bob' WHERE id = 1"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	updateStmt, ok := stmt.(*updateStatement)
	if !ok {
		t.Fatalf("expected *updateStatement, got %T", stmt)
	}

	if updateStmt.tableName != "users" {
		t.Errorf("expected tableName='users', got %q", updateStmt.tableName)
	}

	if len(updateStmt.setExpressions) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(updateStmt.setExpressions))
	}

	nameVal, ok := updateStmt.setExpressions["name"]
	if !ok {
		t.Fatal("expected 'name' in SET clause")
	}

	strLit, ok := nameVal.(*stringLiteral)
	if !ok {
		t.Errorf("expected stringLiteral, got %T", nameVal)
	} else if strLit.value != "Bob" {
		t.Errorf("expected name='Bob', got %q", strLit.value)
	}

	if updateStmt.where == nil {
		t.Fatal("expected where clause")
	}
}

func TestParser_Delete(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	deleteStmt, ok := stmt.(*deleteStatement)
	if !ok {
		t.Fatalf("expected *deleteStatement, got %T", stmt)
	}

	if deleteStmt.tableName != "users" {
		t.Errorf("expected tableName='users', got %q", deleteStmt.tableName)
	}

	if deleteStmt.where == nil {
		t.Fatal("expected where clause")
	}

	binExpr, ok := deleteStmt.where.(*binaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", deleteStmt.where)
	}

	if binExpr.operator != "=" {
		t.Errorf("expected operator '=', got %q", binExpr.operator)
	}
}

func TestParser_DeleteWithoutWhere(t *testing.T) {
	input := "DELETE FROM users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	deleteStmt, ok := stmt.(*deleteStatement)
	if !ok {
		t.Fatalf("expected *deleteStatement, got %T", stmt)
	}

	if deleteStmt.tableName != "users" {
		t.Errorf("expected tableName='users', got %q", deleteStmt.tableName)
	}

	if deleteStmt.where != nil {
		t.Error("expected where to be nil")
	}
}

func TestParser_CreateTable(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*createTableStatement)
	if !ok {
		t.Fatalf("expected *createTableStatement, got %T", stmt)
	}

	if createStmt.tableName != "users" {
		t.Errorf("expected tableName='users', got %q", createStmt.tableName)
	}

	if len(createStmt.columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(createStmt.columns))
	}

	// id INT PRIMARY KEY
	if createStmt.columns[0].name != "id" {
		t.Errorf("expected column[0].name='id', got %q", createStmt.columns[0].name)
	}
	if !createStmt.columns[0].primaryKey {
		t.Error("expected column[0].primaryKey=true")
	}

	// name VARCHAR
	if createStmt.columns[1].name != "name" {
		t.Errorf("expected column[1].name='name', got %q", createStmt.columns[1].name)
	}
}

func TestParser_Explain(t *testing.T) {
	input := "EXPLAIN SELECT * FROM users"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	explainStmt, ok := stmt.(*explainStatement)
	if !ok {
		t.Fatalf("expected *explainStatement, got %T", stmt)
	}

	_, ok = explainStmt.statement.(*selectStatement)
	if !ok {
		t.Fatalf("expected inner statement to be *selectStatement, got %T",
			explainStmt.statement)
	}
}

func TestParser_WhereWithAnd(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1 AND name = 'Alice'"

	lexer := NewLexer(input)
	parser := NewParser(lexer)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	selectStmt := stmt.(*selectStatement)

	if selectStmt.where == nil {
		t.Fatal("expected where clause")
	}

	// WHERE句が BinaryExpression であることを確認
	binExpr, ok := selectStmt.where.(*binaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", selectStmt.where)
	}

	// AND 演算子があることを確認
	if binExpr.operator != "AND" {
		t.Errorf("expected operator 'AND', got %q", binExpr.operator)
	}
}

func TestParser_Errors(t *testing.T) {
	parser := &parser{errors: []string{"error1", "error2"}}

	errors := parser.Errors()

	if len(errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errors))
	}
}
