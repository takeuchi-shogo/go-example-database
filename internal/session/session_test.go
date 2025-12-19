package session

import (
	"os"
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
)

func setupTestSession(t *testing.T) (Session, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "session_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cat, err := catalog.NewCatalog(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create catalog: %v", err)
	}

	exec := executor.NewExecutor(cat)
	sess := NewSession(cat, exec)

	cleanup := func() {
		sess.Close()
		os.RemoveAll(tempDir)
	}

	return sess, cleanup
}

func TestSessionCreateTable(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	result, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	msg := result.GetMessage()
	if msg != "table created: users" {
		t.Errorf("Expected message 'table created: users', got '%s'", msg)
	}
}

func TestSessionInsert(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	result, err := sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	msg := result.GetMessage()
	if msg != "row inserted: users" {
		t.Errorf("Expected message 'row inserted: users', got '%s'", msg)
	}
}

func TestSessionSelect(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SELECT 実行
	result, err := sess.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.GetRowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.GetRowCount())
	}
}

func TestSessionSelectWithWhere(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SELECT WHERE 実行
	result, err := sess.Execute("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}
}

func TestSessionTableNotFound(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// 存在しないテーブルへの SELECT
	_, err := sess.Execute("SELECT * FROM nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}
}

func TestSessionSyntaxError(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// 構文エラー
	_, err := sess.Execute("SELEC * FROM users")
	if err == nil {
		t.Fatal("Expected syntax error")
	}
}

func TestSessionMultipleOperations(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// 複数テーブルの作成と操作
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = sess.Execute("CREATE TABLE orders (id INT, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// users にデータ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}

	// orders にデータ挿入
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}

	// 両方のテーブルを SELECT
	result1, err := sess.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT from users failed: %v", err)
	}
	if result1.GetRowCount() != 1 {
		t.Errorf("Expected 1 row in users, got %d", result1.GetRowCount())
	}

	result2, err := sess.Execute("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("SELECT from orders failed: %v", err)
	}
	if result2.GetRowCount() != 1 {
		t.Errorf("Expected 1 row in orders, got %d", result2.GetRowCount())
	}
}

func TestSessionClose(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// Close が正常に動作することを確認
	err := sess.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
