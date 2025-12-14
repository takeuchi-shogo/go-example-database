package dbtxn

import (
	"path/filepath"
	"testing"
)

func TestRecoveryNoLogs(t *testing.T) {
	// ログがない場合、リカバリは何もしない
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	rm := NewRecoveryManager(wal)

	if err := rm.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
}

func TestRecoveryCommittedTxn(t *testing.T) {
	// コミット済みトランザクションのリカバリ
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)

	// BEGIN → INSERT → COMMIT
	wal.LogBegin(1)
	wal.LogInsert(1, "users", 100, nil, []byte("alice"))
	wal.LogCommit(1)
	wal.Flush()
	wal.Close()

	// 再オープンしてリカバリ
	wal2, _ := NewWAL(path)
	defer wal2.Close()

	rm := NewRecoveryManager(wal2)

	if err := rm.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// REDO が実行されたことを確認（TODO: 実際のデータ確認は後のフェーズ）
}

func TestRecoveryUncommittedTxn(t *testing.T) {
	// 未コミットトランザクションのリカバリ（UNDO）
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)

	// BEGIN → INSERT（COMMIT なし = クラッシュをシミュレート）
	wal.LogBegin(1)
	wal.LogInsert(1, "users", 100, nil, []byte("alice"))
	wal.Flush()
	wal.Close()

	// 再オープンしてリカバリ
	wal2, _ := NewWAL(path)
	defer wal2.Close()

	rm := NewRecoveryManager(wal2)

	if err := rm.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// UNDO が実行されたことを確認
	// Rollback ログが追記されているか
	records, _ := wal2.Read()
	hasRollback := false
	for _, r := range records {
		if r.LogType == LogRollback && r.TxnID == 1 {
			hasRollback = true
			break
		}
	}
	if !hasRollback {
		t.Error("ROLLBACK log not found after recovery")
	}
}

func TestAnalyzeTransactions(t *testing.T) {
	// トランザクション分析のテスト
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	wal, _ := NewWAL(path)
	defer wal.Close()

	// 複数のトランザクション
	wal.LogBegin(1)
	wal.LogInsert(1, "users", 1, nil, []byte("alice"))
	wal.LogCommit(1)

	wal.LogBegin(2)
	wal.LogInsert(2, "users", 2, nil, []byte("bob"))
	// TxnID=2 は COMMIT なし

	wal.LogBegin(3)
	wal.LogInsert(3, "users", 3, nil, []byte("charlie"))
	wal.LogRollback(3)

	wal.Flush()

	rm := NewRecoveryManager(wal)
	records, _ := wal.Read()
	txnMap := rm.analyzeTransactions(records)

	// TxnID=1 は Committed
	if txnMap[1].State != TxnStateCommitted {
		t.Errorf("TxnID=1 should be Committed, got %d", txnMap[1].State)
	}

	// TxnID=2 は Active（未コミット）
	if txnMap[2].State != TxnStateActive {
		t.Errorf("TxnID=2 should be Active, got %d", txnMap[2].State)
	}

	// TxnID=3 は RolledBack
	if txnMap[3].State != TxnStateRolledBack {
		t.Errorf("TxnID=3 should be RolledBack, got %d", txnMap[3].State)
	}
}
