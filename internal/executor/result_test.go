package executor

import (
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func TestNewResultSet(t *testing.T) {
	rs := NewResultSet()
	if rs == nil {
		t.Fatal("NewResultSet returned nil")
	}
	if rs.GetRowCount() != 0 {
		t.Errorf("Expected 0 rows, got %d", rs.GetRowCount())
	}
}

func TestNewResultSetWithMessage(t *testing.T) {
	message := "Table created successfully"
	rs := NewResultSetWithMessage(message)

	if rs == nil {
		t.Fatal("NewResultSetWithMessage returned nil")
	}

	// resultSet の具体型にキャストしてメッセージを確認
	concrete, ok := rs.(*resultSet)
	if !ok {
		t.Fatal("Expected *resultSet type")
	}

	if concrete.message != message {
		t.Errorf("Expected message '%s', got '%s'", message, concrete.message)
	}
}

func TestNewResultSetWithRows(t *testing.T) {
	rows := []*storage.Row{
		storage.NewRow([]storage.Value{storage.StringValue("alice")}),
		storage.NewRow([]storage.Value{storage.StringValue("bob")}),
	}

	rs := NewResultSetWithRows(rows)
	if rs == nil {
		t.Fatal("NewResultSetWithRows returned nil")
	}

	if rs.GetRowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", rs.GetRowCount())
	}
}

func TestNewResultSetWithRowsAndSchema(t *testing.T) {
	columns := []storage.Column{
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)

	rows := []*storage.Row{
		storage.NewRow([]storage.Value{storage.StringValue("alice")}),
	}

	rs := NewResultSetWithRowsAndSchema(schema, rows)
	if rs == nil {
		t.Fatal("NewResultSetWithRowsAndSchema returned nil")
	}

	if rs.GetSchema() != schema {
		t.Error("Schema mismatch")
	}

	if rs.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", rs.GetRowCount())
	}
}

func TestResultSetAddRow(t *testing.T) {
	rs := NewResultSet()

	row1 := storage.NewRow([]storage.Value{storage.StringValue("alice")})
	row2 := storage.NewRow([]storage.Value{storage.StringValue("bob")})

	rs.AddRow(row1)
	if rs.GetRowCount() != 1 {
		t.Errorf("Expected 1 row after first add, got %d", rs.GetRowCount())
	}

	rs.AddRow(row2)
	if rs.GetRowCount() != 2 {
		t.Errorf("Expected 2 rows after second add, got %d", rs.GetRowCount())
	}
}

func TestResultSetGetRows(t *testing.T) {
	row1 := storage.NewRow([]storage.Value{storage.StringValue("alice")})
	row2 := storage.NewRow([]storage.Value{storage.StringValue("bob")})

	rs := NewResultSetWithRows([]*storage.Row{row1, row2})

	rows := rs.GetRows()
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestResultSetIsEmpty(t *testing.T) {
	// 空の ResultSet
	emptyRS := NewResultSet()
	if !emptyRS.IsEmpty() {
		t.Error("Empty ResultSet should return true for IsEmpty")
	}

	// 行がある ResultSet
	row := storage.NewRow([]storage.Value{storage.StringValue("alice")})
	nonEmptyRS := NewResultSetWithRows([]*storage.Row{row})
	if nonEmptyRS.IsEmpty() {
		t.Error("Non-empty ResultSet should return false for IsEmpty")
	}
}

func TestResultSetGetColumnCount(t *testing.T) {
	// columns が空の場合
	rs := NewResultSet()
	if rs.GetColumnCount() != 0 {
		t.Errorf("Expected 0 columns, got %d", rs.GetColumnCount())
	}
}

func TestResultSetString(t *testing.T) {
	rs := NewResultSetWithMessage("test message")

	str := rs.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}

	// String() にはメッセージが含まれるはず
	if len(str) == 0 {
		t.Error("String() should contain some content")
	}
}

func TestResultSetGetSchema(t *testing.T) {
	// スキーマなしの場合
	rsNoSchema := NewResultSet()
	if rsNoSchema.GetSchema() != nil {
		t.Error("Expected nil schema for empty ResultSet")
	}

	// スキーマありの場合
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)
	rsWithSchema := NewResultSetWithRowsAndSchema(schema, nil)

	if rsWithSchema.GetSchema() != schema {
		t.Error("GetSchema should return the schema")
	}
}

func TestResultSetGetRowCountWithNilRows(t *testing.T) {
	rs := &resultSet{
		rows: nil,
	}
	if rs.GetRowCount() != 0 {
		t.Errorf("Expected 0 rows for nil rows, got %d", rs.GetRowCount())
	}
}

func TestResultSetIntegration(t *testing.T) {
	// 統合テスト: スキーマと行を持つ ResultSet の完全なテスト
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeString, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
		*storage.NewColumn("active", storage.ColumnTypeBool, 0, false),
	}
	schema := storage.NewSchema("users", columns)

	rows := []*storage.Row{
		storage.NewRow([]storage.Value{
			storage.StringValue("1"),
			storage.StringValue("alice"),
			storage.BoolValue(true),
		}),
		storage.NewRow([]storage.Value{
			storage.StringValue("2"),
			storage.StringValue("bob"),
			storage.BoolValue(false),
		}),
	}

	rs := NewResultSetWithRowsAndSchema(schema, rows)

	// すべてのメソッドをテスト
	if rs.GetSchema() != schema {
		t.Error("Schema mismatch")
	}
	if rs.GetRowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", rs.GetRowCount())
	}
	if rs.IsEmpty() {
		t.Error("ResultSet should not be empty")
	}
	if len(rs.GetRows()) != 2 {
		t.Errorf("Expected 2 rows from GetRows, got %d", len(rs.GetRows()))
	}

	// 新しい行を追加
	newRow := storage.NewRow([]storage.Value{
		storage.StringValue("3"),
		storage.StringValue("charlie"),
		storage.BoolValue(true),
	})
	rs.AddRow(newRow)

	if rs.GetRowCount() != 3 {
		t.Errorf("Expected 3 rows after adding, got %d", rs.GetRowCount())
	}
}
