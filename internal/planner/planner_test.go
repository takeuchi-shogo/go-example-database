package planner

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/parser"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

// mockCatalog はテスト用のモックカタログ
type mockCatalog struct {
	schemas map[string]*storage.Schema
	tables  map[string]bool
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		schemas: make(map[string]*storage.Schema),
		tables:  make(map[string]bool),
	}
}

func (m *mockCatalog) CreateTable(name string, schema *storage.Schema) error {
	m.schemas[name] = schema
	m.tables[name] = true
	return nil
}

func (m *mockCatalog) GetTable(name string) (*storage.Table, error) {
	return nil, nil
}

func (m *mockCatalog) DropTable(name string) error {
	delete(m.schemas, name)
	delete(m.tables, name)
	return nil
}

func (m *mockCatalog) TableExists(name string) bool {
	return m.tables[name]
}

func (m *mockCatalog) ListTables() []*storage.Table {
	return nil
}

func (m *mockCatalog) GetSchema(name string) (*storage.Schema, error) {
	if schema, ok := m.schemas[name]; ok {
		return schema, nil
	}
	return nil, nil
}

func (m *mockCatalog) Close() error {
	return nil
}

// Catalog インターフェースを実装していることを確認
var _ catalog.Catalog = (*mockCatalog)(nil)

func setupTestCatalog() *mockCatalog {
	mock := newMockCatalog()
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
		*storage.NewColumn("active", storage.ColumnTypeBool, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	mock.CreateTable("users", schema)
	return mock
}

func TestPlanSelect(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// SELECT * FROM users
	sql := "SELECT * FROM users"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// ScanNode が返されるはず
	scanNode, ok := plan.(*ScanNode)
	if !ok {
		t.Fatalf("Expected ScanNode, got %T", plan)
	}

	if scanNode.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", scanNode.TableName)
	}
}

func TestPlanSelectWithColumns(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// SELECT name FROM users
	sql := "SELECT name FROM users"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// ProjectNode が返されるはず
	projectNode, ok := plan.(*ProjectNode)
	if !ok {
		t.Fatalf("Expected ProjectNode, got %T", plan)
	}

	if len(projectNode.Columns) != 1 {
		t.Errorf("Expected 1 column, got %d", len(projectNode.Columns))
	}

	if projectNode.Columns[0] != "name" {
		t.Errorf("Expected column 'name', got '%s'", projectNode.Columns[0])
	}
}

func TestPlanSelectWithWhere(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// SELECT * FROM users WHERE active = true
	sql := "SELECT * FROM users WHERE active = true"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// FilterNode が返されるはず
	filterNode, ok := plan.(*FilterNode)
	if !ok {
		t.Fatalf("Expected FilterNode, got %T", plan)
	}

	// 子ノードは ScanNode
	_, ok = filterNode.Child.(*ScanNode)
	if !ok {
		t.Fatalf("Expected ScanNode as child, got %T", filterNode.Child)
	}
}

func TestPlanInsert(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// INSERT INTO users (name, active) VALUES ('alice', true)
	sql := "INSERT INTO users (name, active) VALUES ('alice', true)"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// InsertNode が返されるはず
	insertNode, ok := plan.(*InsertNode)
	if !ok {
		t.Fatalf("Expected InsertNode, got %T", plan)
	}

	if insertNode.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insertNode.TableName)
	}

	if len(insertNode.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertNode.Columns))
	}

	if len(insertNode.Values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(insertNode.Values))
	}
}

func TestPlanCreateTable(t *testing.T) {
	mock := newMockCatalog()
	planner := NewPlanner(mock)

	// CREATE TABLE products (id INT, name VARCHAR(255))
	sql := "CREATE TABLE products (id INT, name VARCHAR(255))"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// CreateTableNode が返されるはず
	createNode, ok := plan.(*CreateTableNode)
	if !ok {
		t.Fatalf("Expected CreateTableNode, got %T", plan)
	}

	if createNode.TableName != "products" {
		t.Errorf("Expected table name 'products', got '%s'", createNode.TableName)
	}

	if createNode.TableSchema.GetColumnCount() != 2 {
		t.Errorf("Expected 2 columns, got %d", createNode.TableSchema.GetColumnCount())
	}
}

func TestPlanUpdate(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// UPDATE users SET name = 'bob' WHERE active = true
	sql := "UPDATE users SET name = 'bob' WHERE active = true"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// UpdateNode が返されるはず
	updateNode, ok := plan.(*UpdateNode)
	if !ok {
		t.Fatalf("Expected UpdateNode, got %T", plan)
	}

	if updateNode.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", updateNode.TableName)
	}

	if len(updateNode.Sets) != 1 {
		t.Errorf("Expected 1 set expression, got %d", len(updateNode.Sets))
	}
}

func TestPlanDelete(t *testing.T) {
	mock := setupTestCatalog()
	planner := NewPlanner(mock)

	// DELETE FROM users WHERE active = false
	sql := "DELETE FROM users WHERE active = false"
	p := parser.NewParser(parser.NewLexer(sql))
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}

	// DeleteNode が返されるはず
	deleteNode, ok := plan.(*DeleteNode)
	if !ok {
		t.Fatalf("Expected DeleteNode, got %T", plan)
	}

	if deleteNode.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", deleteNode.TableName)
	}

	// 子ノードは FilterNode
	_, ok = deleteNode.Child.(*FilterNode)
	if !ok {
		t.Fatalf("Expected FilterNode as child, got %T", deleteNode.Child)
	}
}
