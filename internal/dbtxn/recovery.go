package dbtxn

import (
	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

type RecoveryManager struct {
	wal     *WAL
	catalog catalog.Catalog
}

type TxnStatus struct {
	ID      uint64
	State   TxnState
	Records []LogRecord
}

func NewRecoveryManager(wal *WAL, catalog catalog.Catalog) *RecoveryManager {
	return &RecoveryManager{
		wal:     wal,
		catalog: catalog,
	}
}

func (rm *RecoveryManager) Recover() error {
	// WAL を読み込む
	records, err := rm.wal.Read()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil // リカバリー必要なし
	}
	// トランザクションごとに分類
	txnMap := rm.analyzeTransactions(records)

	// REDO 処理
	if err := rm.redo(txnMap); err != nil {
		return err
	}

	// UNDO 処理
	if err := rm.undo(txnMap); err != nil {
		return err
	}

	return nil
}

func (rm *RecoveryManager) analyzeTransactions(records []LogRecord) map[uint64]*TxnStatus {
	txnMap := make(map[uint64]*TxnStatus)
	for _, record := range records {
		txnID := record.TxnID
		// トランザクション
		if _, exists := txnMap[txnID]; !exists {
			txnMap[txnID] = &TxnStatus{
				ID:      txnID,
				State:   TxnStateActive, // 初期状態はアクティブ
				Records: make([]LogRecord, 0),
			}
		}

		txnMap[txnID].Records = append(txnMap[txnID].Records, record)
		// 状態を更新
		switch record.LogType {
		case LogCommit:
			txnMap[txnID].State = TxnStateCommitted
		case LogRollback:
			txnMap[txnID].State = TxnStateRolledBack
		}
	}
	return txnMap
}

// redo は REDO 処理を行う
func (rm *RecoveryManager) redo(txnMap map[uint64]*TxnStatus) error {
	// catalog が nil の場合は REDO をスキップ
	if rm.catalog == nil {
		return nil
	}
	for _, status := range txnMap {
		if status.State != TxnStateCommitted {
			continue
		}
		for _, record := range status.Records {
			switch record.LogType {
			case LogInsert:
				// After を Before に、Before を After に変換
				// データベースに対して INSERT 操作を行う
				table, err := rm.catalog.GetTable(record.TableName)
				if err != nil {
					return err
				}
				schema, err := rm.catalog.GetSchema(record.TableName)
				if err != nil {
					return err
				}
				row, err := storage.DecodeRow(record.After, schema)
				if err != nil {
					return err
				}
				err = table.Insert(row)
				if err != nil {
					return err
				}
			case LogUpdate:
				// TODO: UPDATE 操作の実装後に追加
			case LogDelete:
				// TODO: DELETE 操作の実装後に追加
			}
		}
	}
	return nil
}

// undo は UNDO 処理を行う
func (rm *RecoveryManager) undo(txnMap map[uint64]*TxnStatus) error {
	for _, status := range txnMap {
		if status.State != TxnStateActive {
			continue
		}
		// 逆順に処理
		for i := len(status.Records) - 1; i >= 0; i-- {
			record := status.Records[i]
			switch record.LogType {
			case LogInsert:
				// TODO: データベースに対して DELETE 操作を行う
				rm.wal.LogDelete(record.TxnID, record.TableName, record.RowID, record.After)
			case LogUpdate:
				// TODO: データベースに対して UPDATE 操作を行う
				rm.wal.LogUpdate(record.TxnID, record.TableName, record.RowID, record.After, record.Before)
			case LogDelete:
				// TODO: データベースに対して INSERT 操作を行う
				rm.wal.LogInsert(record.TxnID, record.TableName, record.RowID, record.Before, record.After)
			}
		}
		// Rollback したログを追記
		rm.wal.LogRollback(status.ID)
	}
	// UNDO ログをディスクに書き込む
	return rm.wal.Flush()
}
