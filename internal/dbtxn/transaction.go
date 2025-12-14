package dbtxn

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// TxnState はトランザクションの状態を表す
type TxnState uint8

const (
	TxnStateActive TxnState = iota
	TxnStateCommitted
	TxnStateRolledBack
)

// Transaction はトランザクションを管理する
type Transaction struct {
	ID       uint64
	State    TxnState
	StartLSN uint64
	wal      *WAL
	mu       sync.Mutex
}

type TxnManager struct {
	wal        *WAL
	nextTxnID  uint64                  // 次のトランザクションID
	activeTxns map[uint64]*Transaction // アクティブなトランザクション
	mu         sync.Mutex
}

func NewTxnManager(wal *WAL) *TxnManager {
	return &TxnManager{
		wal:        wal,
		nextTxnID:  1,
		activeTxns: make(map[uint64]*Transaction),
	}
}

// Begin は新しいトランザクションを開始する
func (tm *TxnManager) Begin() (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txnID := atomic.AddUint64(&tm.nextTxnID, 1) - 1
	// WAL に BEGIN ログを追加
	if err := tm.wal.LogBegin(txnID); err != nil {
		return nil, err
	}

	txn := &Transaction{
		ID:       txnID,
		State:    TxnStateActive,
		StartLSN: tm.wal.nextLSN,
	}

	tm.activeTxns[txnID] = txn
	return txn, nil
}

// Commit はトランザクションをコミットする
func (tm *TxnManager) Commit(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxnStateActive {
		return fmt.Errorf("transaction is not active")
	}

	txn.State = TxnStateCommitted
	// WAL に COMMIT ログを追加
	if err := tm.wal.LogCommit(txn.ID); err != nil {
		return err
	}
	// バッファをフラッシュ
	if err := tm.wal.Flush(); err != nil {
		return err
	}

	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	return nil
}

// Rollback はトランザクションをロールバックする
func (tm *TxnManager) Rollback(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.State != TxnStateActive {
		return fmt.Errorf("transaction is not active")
	}

	txn.State = TxnStateRolledBack
	// WAL に ROLLBACK ログを追加
	if err := tm.wal.LogRollback(txn.ID); err != nil {
		return err
	}
	// バッファをフラッシュ
	if err := tm.wal.Flush(); err != nil {
		return err
	}

	// TODO: UNDO 操作

	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()

	return nil
}
