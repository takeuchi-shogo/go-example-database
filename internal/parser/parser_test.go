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

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *selectStatement, got %T", stmt)
	}

	if selectStmt.From != "users" {
		t.Errorf("expected from='users', got %q", selectStmt.From)
	}

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(selectStmt.Columns))
	}

	if _, ok := selectStmt.Columns[0].(*Asterisk); !ok {
		t.Errorf("expected asterisk, got %T", selectStmt.Columns[0])
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

	selectStmt := stmt.(*SelectStatement)

	if len(selectStmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(selectStmt.Columns))
	}

	expectedCols := []string{"id", "name", "age"}
	for i, col := range selectStmt.Columns {
		ident, ok := col.(*Identifier)
		if !ok {
			t.Errorf("column[%d]: expected identifier, got %T", i, col)
			continue
		}
		if ident.Value != expectedCols[i] {
			t.Errorf("column[%d]: expected %q, got %q", i, expectedCols[i], ident.Value)
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

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *selectStatement, got %T", stmt)
	}

	if selectStmt.Where == nil {
		t.Fatal("expected where clause")
	}

	binExpr, ok := selectStmt.Where.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", selectStmt.Where)
	}

	if binExpr.Operator != "=" {
		t.Errorf("expected operator '=', got %q", binExpr.Operator)
	}

	left, ok := binExpr.Left.(*Identifier)
	if !ok || left.Value != "id" {
		t.Errorf("expected left to be identifier{id}, got %v", binExpr.Left)
	}

	right, ok := binExpr.Right.(*IntegerLiteral)
	if !ok || right.Value != 1 {
		t.Errorf("expected right to be integerLiteral{1}, got %v", binExpr.Right)
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

	selectStmt := stmt.(*SelectStatement)

	if len(selectStmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY clause, got %d", len(selectStmt.OrderBy))
	}

	if selectStmt.OrderBy[0].Column != "name" {
		t.Errorf("expected OrderBy column='name', got %q", selectStmt.OrderBy[0].Column)
	}

	if selectStmt.OrderBy[0].Asc {
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

	selectStmt := stmt.(*SelectStatement)

	if selectStmt.Limit == nil {
		t.Fatal("expected limit to be set")
	}

	if *selectStmt.Limit != 10 {
		t.Errorf("expected limit=10, got %d", *selectStmt.Limit)
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

	insertStmt, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected *insertStatement, got %T", stmt)
	}

	if insertStmt.TableName != "users" {
		t.Errorf("expected tableName='users', got %q", insertStmt.TableName)
	}

	if len(insertStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(insertStmt.Columns))
	}

	if insertStmt.Columns[0] != "id" || insertStmt.Columns[1] != "name" {
		t.Errorf("expected columns [id, name], got %v", insertStmt.Columns)
	}

	if len(insertStmt.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(insertStmt.Values))
	}

	// 最初の値が整数リテラル
	intVal, ok := insertStmt.Values[0].(*IntegerLiteral)
	if !ok {
		t.Errorf("expected values[0] to be integerLiteral, got %T", insertStmt.Values[0])
	} else if intVal.Value != 1 {
		t.Errorf("expected values[0]=1, got %d", intVal.Value)
	}

	// 2番目の値が文字列リテラル
	strVal, ok := insertStmt.Values[1].(*StringLiteral)
	if !ok {
		t.Errorf("expected values[1] to be stringLiteral, got %T", insertStmt.Values[1])
	} else if strVal.Value != "Alice" {
		t.Errorf("expected values[1]='Alice', got %q", strVal.Value)
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

	updateStmt, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected *updateStatement, got %T", stmt)
	}

	if updateStmt.TableName != "users" {
		t.Errorf("expected tableName='users', got %q", updateStmt.TableName)
	}

	if len(updateStmt.SetExpressions) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(updateStmt.SetExpressions))
	}

	nameVal, ok := updateStmt.SetExpressions["name"]
	if !ok {
		t.Fatal("expected 'name' in SET clause")
	}

	strLit, ok := nameVal.(*StringLiteral)
	if !ok {
		t.Errorf("expected stringLiteral, got %T", nameVal)
	} else if strLit.Value != "Bob" {
		t.Errorf("expected name='Bob', got %q", strLit.Value)
	}

	if updateStmt.Where == nil {
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

	deleteStmt, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected *deleteStatement, got %T", stmt)
	}

	if deleteStmt.TableName != "users" {
		t.Errorf("expected tableName='users', got %q", deleteStmt.TableName)
	}

	if deleteStmt.Where == nil {
		t.Fatal("expected where clause")
	}

	binExpr, ok := deleteStmt.Where.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", deleteStmt.Where)
	}

	if binExpr.Operator != "=" {
		t.Errorf("expected operator '=', got %q", binExpr.Operator)
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

	deleteStmt, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected *deleteStatement, got %T", stmt)
	}

	if deleteStmt.TableName != "users" {
		t.Errorf("expected tableName='users', got %q", deleteStmt.TableName)
	}

	if deleteStmt.Where != nil {
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

	createStmt, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected *createTableStatement, got %T", stmt)
	}

	if createStmt.TableName != "users" {
		t.Errorf("expected tableName='users', got %q", createStmt.TableName)
	}

	if len(createStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(createStmt.Columns))
	}

	// id INT PRIMARY KEY
	if createStmt.Columns[0].Name != "id" {
		t.Errorf("expected column[0].name='id', got %q", createStmt.Columns[0].Name)
	}
	if !createStmt.Columns[0].PrimaryKey {
		t.Error("expected column[0].primaryKey=true")
	}

	// name VARCHAR
	if createStmt.Columns[1].Name != "name" {
		t.Errorf("expected column[1].name='name', got %q", createStmt.Columns[1].Name)
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

	explainStmt, ok := stmt.(*ExplainStatement)
	if !ok {
		t.Fatalf("expected *explainStatement, got %T", stmt)
	}

	_, ok = explainStmt.Statement.(*SelectStatement)
	if !ok {
		t.Fatalf("expected inner statement to be *selectStatement, got %T",
			explainStmt.Statement)
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

	selectStmt := stmt.(*SelectStatement)

	if selectStmt.Where == nil {
		t.Fatal("expected where clause")
	}

	// WHERE句が BinaryExpression であることを確認
	binExpr, ok := selectStmt.Where.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected *binaryExpression, got %T", selectStmt.Where)
	}

	// AND 演算子があることを確認
	if binExpr.Operator != "AND" {
		t.Errorf("expected operator 'AND', got %q", binExpr.Operator)
	}
}

func TestParser_Errors(t *testing.T) {
	parser := &parser{errors: []string{"error1", "error2"}}

	errors := parser.Errors()

	if len(errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errors))
	}
}
