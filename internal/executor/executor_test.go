package executor

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/planner"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func setupTestEnvironment(t *testing.T) (catalog.Catalog, Executor) {
	tempDir := t.TempDir()
	cat, err := catalog.NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	exec := NewExecutor(cat)
	return cat, exec
}

func TestExecuteCreateTable(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// CreateTableNode を作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)
	node := &planner.CreateTableNode{
		TableName:   "users",
		TableSchema: schema,
	}

	result, err := exec.Execute(node)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	// テーブルが作成されたか確認
	if !cat.TableExists("users") {
		t.Error("Table 'users' should exist after CreateTable")
	}
}

func TestExecuteScan(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)
	err := cat.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// データを挿入
	table, _ := cat.GetTable("users")
	row := storage.NewRow([]storage.Value{storage.StringValue("alice")})
	table.Insert(row)

	// ScanNode を実行
	scanNode := &planner.ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	result, err := exec.Execute(scanNode)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}
}

func TestExecuteFilter(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
		*storage.NewColumn("active", storage.ColumnTypeBool, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	err := cat.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// データを挿入
	table, _ := cat.GetTable("users")
	table.Insert(storage.NewRow([]storage.Value{storage.StringValue("alice"), storage.BoolValue(true)}))
	table.Insert(storage.NewRow([]storage.Value{storage.StringValue("bob"), storage.BoolValue(false)}))

	// FilterNode を実行（active = true）
	filterNode := &planner.FilterNode{
		Condition: &planner.BinaryExpr{
			Left:     &planner.ColumnRef{Name: "active"},
			Operator: "=",
			Right:    &planner.Literal{Value: true},
		},
		Child: &planner.ScanNode{
			TableName:   "users",
			TableSchema: schema,
		},
	}

	result, err := exec.Execute(filterNode)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// active = true の行は 1 つ（alice）
	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row (alice with active=true), got %d", result.GetRowCount())
	}
}

func TestExecuteProject(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeString, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
		*storage.NewColumn("email", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)
	err := cat.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// データを挿入
	table, _ := cat.GetTable("users")
	table.Insert(storage.NewRow([]storage.Value{
		storage.StringValue("1"),
		storage.StringValue("alice"),
		storage.StringValue("alice@example.com"),
	}))

	// ProjectNode を実行（name のみ取得）
	projectNode := &planner.ProjectNode{
		Columns: []string{"name"},
		Child: &planner.ScanNode{
			TableName:   "users",
			TableSchema: schema,
		},
	}

	result, err := exec.Execute(projectNode)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}

	// 行の値を確認
	rows := result.GetRows()
	if len(rows) > 0 {
		values := rows[0].GetValues()
		if len(values) != 1 {
			t.Errorf("Expected 1 column in projected row, got %d", len(values))
		}
	}
}

func TestExecuteInsert(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
		*storage.NewColumn("active", storage.ColumnTypeBool, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	err := cat.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// InsertNode を実行
	insertNode := &planner.InsertNode{
		TableName: "users",
		Columns:   []string{"name", "active"},
		Values: []planner.Expression{
			&planner.Literal{Value: "alice"},
			&planner.Literal{Value: true},
		},
	}

	result, err := exec.Execute(insertNode)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	// データが挿入されたか確認
	table, _ := cat.GetTable("users")
	rows, _ := table.Scan()
	if len(rows) != 1 {
		t.Errorf("Expected 1 row after insert, got %d", len(rows))
	}
}

func TestExecuteUpdateNotImplemented(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	updateNode := &planner.UpdateNode{
		TableName: "users",
	}

	result, err := exec.Execute(updateNode)
	if err != nil {
		t.Fatalf("Execute should not return error for unimplemented: %v", err)
	}

	// メッセージが含まれているか確認
	if result == nil {
		t.Fatal("Result should not be nil")
	}
}

func TestExecuteDeleteNotImplemented(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	deleteNode := &planner.DeleteNode{
		TableName: "users",
	}

	result, err := exec.Execute(deleteNode)
	if err != nil {
		t.Fatalf("Execute should not return error for unimplemented: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}
}

func TestExecuteTableNotFound(t *testing.T) {
	cat, exec := setupTestEnvironment(t)
	defer cat.Close()

	// 存在しないテーブルをスキャン
	scanNode := &planner.ScanNode{
		TableName: "nonexistent",
	}

	_, err := exec.Execute(scanNode)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestToStorageValue(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectErr bool
	}{
		{"string", "hello", false},
		{"bool true", true, false},
		{"bool false", false, false},
		{"int", 42, false},
		{"int32", int32(42), false},
		{"int64", int64(42), false},
		{"float", 3.14, true}, // 未実装
		{"nil", nil, true},    // 未対応
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := toStorageValue(tt.input)
			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
