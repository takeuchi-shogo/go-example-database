package dbtxn

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewWAL(t *testing.T) {
	// 一時ファイルで WAL を作成
	// ファイルが作成されることを確認
	// nextLSN が 1 であることを確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	if wal.nextLSN != 1 {
		t.Errorf("expected nextLSN=1, got %d", wal.nextLSN)
	}
}

func TestAppendAndFlush(t *testing.T) {
	// レコードを追加
	// LSN が連番で割り当てられることを確認
	// バッファにレコードが追加されることを確認
	// フラッシュでディスクに書き込まれることを確認
	// ファイルサイズが0より大きいことを確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	// レコード追加
	record := &LogRecord{
		LogType:   LogInsert,
		TxnID:     1,
		TableName: "users",
		RowID:     100,
		After:     []byte("test data"),
	}

	if err := wal.Append(record); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// LSN が割り当てられているか
	if record.LSN != 1 {
		t.Errorf("expected LSN=1, got %d", record.LSN)
	}

	// フラッシュ
	if err := wal.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// ファイルサイズが0より大きいか
	info, _ := os.Stat(path)
	if info.Size() == 0 {
		t.Error("WAL file is empty after flush")
	}
}

func TestRead(t *testing.T) {
	// レコードを追加
	// 再オープンして読み込み
	// レコードが読み込まれることを確認
	// テーブル名が正しいことを確認
	// データが正しいことを確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// レコード追加
	wal.Append(&LogRecord{
		LogType:   LogInsert,
		TxnID:     1,
		TableName: "users",
		RowID:     100,
		After:     []byte("alice"),
	})
	wal.Flush()
	wal.Close()

	// 再オープンして読み込み
	wal2, _ := NewWAL(path)
	defer wal2.Close()

	records, err := wal2.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	if records[0].TableName != "users" {
		t.Errorf("expected TableName=users, got %s", records[0].TableName)
	}

	if string(records[0].After) != "alice" {
		t.Errorf("expected After=alice, got %s", string(records[0].After))
	}
}

func TestMultipleRecords(t *testing.T) {
	// 複数レコード追加
	// 読み込み
	// レコードが読み込まれることを確認
	// LSN が連番か
	// 型の確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)

	// 複数レコード追加
	wal.Append(&LogRecord{LogType: LogBegin, TxnID: 1})
	wal.Append(&LogRecord{LogType: LogInsert, TxnID: 1, TableName: "users", RowID: 1, After: []byte("alice")})
	wal.Append(&LogRecord{LogType: LogInsert, TxnID: 1, TableName: "users", RowID: 2, After: []byte("bob")})
	wal.Append(&LogRecord{LogType: LogCommit, TxnID: 1})
	wal.Flush()
	wal.Close()

	// 読み込み
	wal2, _ := NewWAL(path)
	defer wal2.Close()

	records, _ := wal2.Read()

	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}

	// LSN が連番か
	for i, r := range records {
		if r.LSN != uint64(i+1) {
			t.Errorf("record %d: expected LSN=%d, got %d", i, i+1, r.LSN)
		}
	}

	// 型の確認
	if records[0].LogType != LogBegin {
		t.Errorf("expected LogBegin, got %d", records[0].LogType)
	}
	if records[3].LogType != LogCommit {
		t.Errorf("expected LogCommit, got %d", records[3].LogType)
	}
}

func TestLogHelpers(t *testing.T) {
	// ヘルパー関数のテスト
	// ヘルパー関数を使用してレコードを追加
	// 読み込み
	// レコードが読み込まれることを確認
	// Update の Before/After 確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	// ヘルパー関数のテスト
	wal.LogBegin(1)
	wal.LogInsert(1, "users", 100, []byte("alice"), []byte("alice"))
	wal.LogUpdate(1, "users", 100, []byte("alice"), []byte("alice2"))
	wal.LogDelete(1, "users", 100, []byte("alice2"))
	wal.LogCommit(1)
	wal.Flush()

	records, _ := wal.Read()

	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}

	// Update の Before/After 確認
	updateRecord := records[2]
	if string(updateRecord.Before) != "alice" {
		t.Errorf("expected Before=alice, got %s", string(updateRecord.Before))
	}
	if string(updateRecord.After) != "alice2" {
		t.Errorf("expected After=alice2, got %s", string(updateRecord.After))
	}
}

func TestCheckpoint(t *testing.T) {
	// チェックポイント
	// チェックポイントレコードが追加されることを確認
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	wal.LogBegin(1)
	wal.LogInsert(1, "users", 100, []byte("alice"), []byte("alice"))
	wal.LogCommit(1)

	// チェックポイント
	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	records, _ := wal.Read()

	// 最後のレコードが Checkpoint か
	lastRecord := records[len(records)-1]
	if lastRecord.LogType != LogCheckpoint {
		t.Errorf("expected LogCheckpoint, got %d", lastRecord.LogType)
	}
}

func TestReopen(t *testing.T) {
	// 再オープン
	// nextLSN が継続されているか
	// 追加して確認
	// 最後のレコードの LSN が正しいか
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// 最初のセッション
	wal1, _ := NewWAL(path)
	wal1.LogBegin(1)
	wal1.LogInsert(1, "users", 100, []byte("alice"), []byte("alice"))
	wal1.LogCommit(1)
	wal1.Flush()
	wal1.Close()

	// 再オープン
	wal2, _ := NewWAL(path)

	// nextLSN が継続されているか
	if wal2.nextLSN != 4 {
		t.Errorf("expected nextLSN=4, got %d", wal2.nextLSN)
	}

	// 追加して確認
	wal2.LogBegin(2)
	wal2.Flush()
	wal2.Close()

	// 最終確認
	wal3, _ := NewWAL(path)
	defer wal3.Close()

	records, _ := wal3.Read()
	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}

	// 最後のレコードの LSN
	if records[3].LSN != 4 {
		t.Errorf("expected LSN=4, got %d", records[3].LSN)
	}
}

func TestLogAbort(t *testing.T) {
	// ロールバック
	// ロールバックレコードが追加されることを確認
	// ロールバックレコードの型が正しいか
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	wal.LogBegin(1)
	wal.LogInsert(1, "users", 100, []byte("alice"), []byte("alice"))
	wal.LogRollback(1) // ロールバック
	wal.Flush()

	records, _ := wal.Read()

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[2].LogType != LogRollback {
		t.Errorf("expected LogAbort, got %d", records[2].LogType)
	}
}
