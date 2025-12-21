package session

import (
	"os"
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/executor"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
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

func TestSessionJoin(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// users テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// orders テーブル作成
	_, err = sess.Execute("CREATE TABLE orders (id INT, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// users にデータ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}

	// orders にデータ挿入
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 150)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}

	// JOIN 実行: users.id = orders.user_id
	result, err := sess.Execute("SELECT * FROM users JOIN orders ON id = user_id")
	if err != nil {
		t.Fatalf("SELECT JOIN failed: %v", err)
	}

	// alice は 2 件、bob は 1 件で合計 3 件
	if result.GetRowCount() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.GetRowCount())
	}

	// カラム数は users(2) + orders(3) = 5
	if result.GetColumnCount() != 5 {
		t.Errorf("Expected 5 columns, got %d", result.GetColumnCount())
	}
}

func TestSessionJoinNoMatch(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// users テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// orders テーブル作成
	_, err = sess.Execute("CREATE TABLE orders (id INT, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// users にデータ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}

	// orders にデータ挿入（user_id = 999 でマッチしない）
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 999, 100)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}

	// JOIN 実行
	result, err := sess.Execute("SELECT * FROM users JOIN orders ON id = user_id")
	if err != nil {
		t.Fatalf("SELECT JOIN failed: %v", err)
	}

	// マッチする行がないので 0 件
	if result.GetRowCount() != 0 {
		t.Errorf("Expected 0 rows, got %d", result.GetRowCount())
	}
}

func TestSessionJoinWithQualifiedColumns(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// users テーブル作成
	_, err := sess.Execute("CREATE TABLE users (id INT, name VARCHAR(255))")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// orders テーブル作成
	_, err = sess.Execute("CREATE TABLE orders (id INT, user_id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// users にデータ挿入
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT into users failed: %v", err)
	}

	// orders にデータ挿入
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO orders (id, user_id, amount) VALUES (2, 2, 200)")
	if err != nil {
		t.Fatalf("INSERT into orders failed: %v", err)
	}

	// 修飾子付き JOIN 実行: users.id = orders.user_id
	result, err := sess.Execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("SELECT JOIN with qualified columns failed: %v", err)
	}

	// alice と bob で合計 2 件
	if result.GetRowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.GetRowCount())
	}

	// カラム数は users(2) + orders(3) = 5
	if result.GetColumnCount() != 5 {
		t.Errorf("Expected 5 columns, got %d", result.GetColumnCount())
	}
}

func TestSessionAggregateCount(t *testing.T) {
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
	_, err = sess.Execute("INSERT INTO users (id, name) VALUES (3, 'charlie')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// COUNT(*) 実行
	result, err := sess.Execute("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("SELECT COUNT(*) failed: %v", err)
	}

	// 結果は1行
	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}

	// COUNT の結果を確認
	rows := result.GetRows()
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	values := rows[0].GetValues()
	if len(values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(values))
	}
	// COUNT は storage.Int64Value を返す
	count, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	if int64(count) != 3 {
		t.Errorf("Expected COUNT(*) = 3, got %d", int64(count))
	}
}

func TestSessionAggregateSum(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE orders (id INT, amount INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO orders (id, amount) VALUES (1, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO orders (id, amount) VALUES (2, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO orders (id, amount) VALUES (3, 300)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// SUM(amount) 実行
	result, err := sess.Execute("SELECT SUM(amount) FROM orders")
	if err != nil {
		t.Fatalf("SELECT SUM(amount) failed: %v", err)
	}

	// 結果は1行
	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}

	// SUM の結果を確認 (100 + 200 + 300 = 600)
	rows := result.GetRows()
	values := rows[0].GetValues()
	sum, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	if int64(sum) != 600 {
		t.Errorf("Expected SUM(amount) = 600, got %d", int64(sum))
	}
}

func TestSessionAggregateAvg(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE scores (id INT, score INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO scores (id, score) VALUES (1, 80)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO scores (id, score) VALUES (2, 90)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO scores (id, score) VALUES (3, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// AVG(score) 実行
	result, err := sess.Execute("SELECT AVG(score) FROM scores")
	if err != nil {
		t.Fatalf("SELECT AVG(score) failed: %v", err)
	}

	// 結果は1行
	if result.GetRowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.GetRowCount())
	}

	// AVG の結果を確認 ((80 + 90 + 100) / 3 = 90)
	rows := result.GetRows()
	values := rows[0].GetValues()
	avg, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	if int64(avg) != 90 {
		t.Errorf("Expected AVG(score) = 90, got %d", int64(avg))
	}
}

func TestSessionAggregateMaxMin(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE values_table (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO values_table (id, value) VALUES (1, 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO values_table (id, value) VALUES (2, 100)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO values_table (id, value) VALUES (3, 25)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// MAX(value) 実行
	result, err := sess.Execute("SELECT MAX(value) FROM values_table")
	if err != nil {
		t.Fatalf("SELECT MAX(value) failed: %v", err)
	}

	rows := result.GetRows()
	values := rows[0].GetValues()
	max, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	if int64(max) != 100 {
		t.Errorf("Expected MAX(value) = 100, got %d", int64(max))
	}

	// MIN(value) 実行
	result, err = sess.Execute("SELECT MIN(value) FROM values_table")
	if err != nil {
		t.Fatalf("SELECT MIN(value) failed: %v", err)
	}

	rows = result.GetRows()
	values = rows[0].GetValues()
	min, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	if int64(min) != 25 {
		t.Errorf("Expected MIN(value) = 25, got %d", int64(min))
	}
}

func TestSessionAggregateWithWhere(t *testing.T) {
	sess, cleanup := setupTestSession(t)
	defer cleanup()

	// テーブル作成
	_, err := sess.Execute("CREATE TABLE products (id INT, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// データ挿入
	_, err = sess.Execute("INSERT INTO products (id, price) VALUES (1, 50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO products (id, price) VALUES (2, 150)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = sess.Execute("INSERT INTO products (id, price) VALUES (3, 200)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// WHERE + COUNT 実行
	result, err := sess.Execute("SELECT COUNT(*) FROM products WHERE price > 100")
	if err != nil {
		t.Fatalf("SELECT COUNT(*) WHERE failed: %v", err)
	}

	rows := result.GetRows()
	values := rows[0].GetValues()
	count, ok := values[0].(storage.Int64Value)
	if !ok {
		t.Fatalf("Expected storage.Int64Value, got %T", values[0])
	}
	// price > 100 は 150, 200 の2件
	if int64(count) != 2 {
		t.Errorf("Expected COUNT(*) WHERE price > 100 = 2, got %d", int64(count))
	}
}
