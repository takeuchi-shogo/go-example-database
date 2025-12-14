package dbtxn

import (
	"path/filepath"
	"testing"
)

func TestNewTxnManager(t *testing.T) {
	// TxnManager が正しく初期化されるか
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	tm := NewTxnManager(wal)

	if tm.nextTxnID != 1 {
		t.Errorf("expected nextTxnID=1, got %d", tm.nextTxnID)
	}

	if len(tm.activeTxns) != 0 {
		t.Errorf("expected empty activeTxns, got %d", len(tm.activeTxns))
	}
}

func TestBegin(t *testing.T) {
	// トランザクション開始のテスト
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	tm := NewTxnManager(wal)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// ID が割り当てられているか
	if txn.ID != 1 {
		t.Errorf("expected ID=1, got %d", txn.ID)
	}

	// 状態が Active か
	if txn.State != TxnStateActive {
		t.Errorf("expected TxnStateActive, got %d", txn.State)
	}

	// activeTxns に追加されているか
	if len(tm.activeTxns) != 1 {
		t.Errorf("expected 1 active txn, got %d", len(tm.activeTxns))
	}
}

func TestCommit(t *testing.T) {
	// コミットのテスト
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	tm := NewTxnManager(wal)

	txn, _ := tm.Begin()

	if err := tm.Commit(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 状態が Committed か
	if txn.State != TxnStateCommitted {
		t.Errorf("expected TxnStateCommitted, got %d", txn.State)
	}

	// activeTxns から削除されているか
	if len(tm.activeTxns) != 0 {
		t.Errorf("expected 0 active txns, got %d", len(tm.activeTxns))
	}

	// WAL に COMMIT ログがあるか
	records, _ := wal.Read()
	hasCommit := false
	for _, r := range records {
		if r.LogType == LogCommit && r.TxnID == txn.ID {
			hasCommit = true
			break
		}
	}
	if !hasCommit {
		t.Error("COMMIT log not found in WAL")
	}
}

func TestRollback(t *testing.T) {
	// ロールバックのテスト
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	tm := NewTxnManager(wal)

	txn, _ := tm.Begin()

	if err := tm.Rollback(txn); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// 状態が RolledBack か
	if txn.State != TxnStateRolledBack {
		t.Errorf("expected TxnStateRolledBack, got %d", txn.State)
	}

	// activeTxns から削除されているか
	if len(tm.activeTxns) != 0 {
		t.Errorf("expected 0 active txns, got %d", len(tm.activeTxns))
	}
}

func TestCommitInactiveTxn(t *testing.T) {
	// 非アクティブなトランザクションをコミットしようとするとエラー
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	tm := NewTxnManager(wal)

	txn, _ := tm.Begin()
	tm.Commit(txn)

	// 2回目のコミットはエラー
	err := tm.Commit(txn)
	if err == nil {
		t.Error("expected error for committing inactive txn")
	}
}

func TestMultipleTransactions(t *testing.T) {
	// 複数トランザクションのテスト
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	tm := NewTxnManager(wal)

	txn1, _ := tm.Begin()
	txn2, _ := tm.Begin()

	// ID が異なるか
	if txn1.ID == txn2.ID {
		t.Error("txn1 and txn2 should have different IDs")
	}

	// 両方アクティブか
	if len(tm.activeTxns) != 2 {
		t.Errorf("expected 2 active txns, got %d", len(tm.activeTxns))
	}

	tm.Commit(txn1)
	tm.Rollback(txn2)

	if len(tm.activeTxns) != 0 {
		t.Errorf("expected 0 active txns, got %d", len(tm.activeTxns))
	}
}
