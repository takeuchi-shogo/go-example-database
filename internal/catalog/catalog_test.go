package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func TestNewCatalog(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()

	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	if catalog == nil {
		t.Fatal("NewCatalog returned nil")
	}
}

func TestCreateTable(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	// スキーマを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)

	// テーブルを作成
	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// ファイルが作成されているか確認
	filePath := filepath.Join(tempDir, "users.db")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("Table file was not created: %s", filePath)
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)

	// 1回目のCreateTable
	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("First CreateTable failed: %v", err)
	}

	// 2回目のCreateTable（重複）
	err = catalog.CreateTable("users", schema)
	if err == nil {
		t.Error("Expected error for duplicate table, got nil")
	}
}

func TestGetTable(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)

	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// テーブルを取得
	table, err := catalog.GetTable("users")
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	if table == nil {
		t.Error("GetTable returned nil")
	}
}

func TestGetTableNotFound(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	// 存在しないテーブルを取得
	_, err = catalog.GetTable("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent table, got nil")
	}
}

func TestTableExists(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)

	// テーブル作成前
	if catalog.TableExists("users") {
		t.Error("TableExists should return false before creation")
	}

	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// テーブル作成後
	if !catalog.TableExists("users") {
		t.Error("TableExists should return true after creation")
	}
}

func TestGetSchema(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)

	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// スキーマを取得
	retrievedSchema, err := catalog.GetSchema("users")
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}

	if retrievedSchema.GetTableName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", retrievedSchema.GetTableName())
	}

	if retrievedSchema.GetColumnCount() != 2 {
		t.Errorf("Expected 2 columns, got %d", retrievedSchema.GetColumnCount())
	}
}

func TestDropTable(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}
	schema := storage.NewSchema("users", columns)

	err = catalog.CreateTable("users", schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// テーブルを削除
	err = catalog.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// テーブルが存在しないことを確認
	if catalog.TableExists("users") {
		t.Error("Table should not exist after drop")
	}

	// ファイルが削除されているか確認
	filePath := filepath.Join(tempDir, "users.db")
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("Table file should be deleted after drop")
	}
}

func TestListTables(t *testing.T) {
	tempDir := t.TempDir()
	catalog, err := NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("NewCatalog failed: %v", err)
	}
	defer catalog.Close()

	// 空の状態
	tables := catalog.ListTables()
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(tables))
	}

	// テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt64, 0, false),
	}

	err = catalog.CreateTable("users", storage.NewSchema("users", columns))
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	err = catalog.CreateTable("orders", storage.NewSchema("orders", columns))
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tables = catalog.ListTables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}
