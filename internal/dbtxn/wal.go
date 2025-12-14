package dbtxn

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
	"sync"
)

type LogType uint8

const (
	// データ操作
	LogInsert LogType = iota
	LogUpdate
	LogDelete
	// トランザクション管理
	LogBegin
	LogCommit
	LogRollback
	// チェックポイント
	LogCheckpoint
	LogCompensate // UNDO 時の補償ログ
)

type LogRecord struct {
	LSN       uint64  // ログシーケンス番号
	TxnID     uint64  // トランザクションID
	LogType   LogType // ログタイプ
	TableName string  // テーブル名
	RowID     uint64  // 行ID
	Before    []byte  // 変更前のデータ
	After     []byte  // 変更後のデータ
}

// WALはWrite-Ahead Logを管理する
type WAL struct {
	filePath string
	file     *os.File
	mu       sync.Mutex
	nextLSN  uint64
	buffer   []LogRecord
}

// NewWAL はWALを初期化する
func NewWAL(path string) (*WAL, error) {
	// ファイルを開く
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		filePath: path,
		file:     file,
		mu:       sync.Mutex{},
		nextLSN:  1,
		buffer:   make([]LogRecord, 0),
	}

	// 既存ログがあれば最後のLSNを取得
	lastLSN, err := wal.getLastLSN()
	if err == nil && lastLSN > 0 {
		wal.nextLSN = lastLSN + 1
	}

	return wal, nil
}

func (w *WAL) getLastLSN() (uint64, error) {
	records, err := w.Read()
	if err != nil || len(records) == 0 {
		return 0, err
	}
	return records[len(records)-1].LSN, nil
}

// Append はLogRecordをバッファに追加する
func (w *WAL) Append(record *LogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record.LSN = w.nextLSN
	w.nextLSN++

	w.buffer = append(w.buffer, *record)
	return nil
}

// Flush はバッファ内のレコードをディスクに書き込む
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buffer) == 0 {
		return nil
	}
	// バッファ内の全てのレコードをシリアライズ
	for _, record := range w.buffer {
		data, err := w.serialize(&record)
		if err != nil {
			return err
		}
		// レコード長 + データを書き込む
		length := uint32(len(data))
		if err := binary.Write(w.file, binary.LittleEndian, length); err != nil {
			return err
		}
		if _, err := w.file.Write(data); err != nil {
			return err
		}
	}
	// fsync でディスクに確実に書き込む
	if err := w.file.Sync(); err != nil {
		return err
	}
	// バッファをクリア
	w.buffer = make([]LogRecord, 0)
	return nil
}

// serialize はLogRecordをシリアライズする
func (w *WAL) serialize(record *LogRecord) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(record); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Read はディスクからLogRecordを読み込む
func (w *WAL) Read() ([]LogRecord, error) {
	// ファイルの先頭に移動
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []LogRecord

	for {
		// レコード長を読む
		var length uint32
		if err := binary.Read(w.file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// read record data
		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			return nil, err
		}
		// deserialize
		record, err := w.deserialize(data)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}

	return records, nil
}

// deserialize はシリアライズされたデータをLogRecordにデシリアライズする
func (w *WAL) deserialize(data []byte) (*LogRecord, error) {
	var record LogRecord
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&record); err != nil {
		return nil, err
	}
	return &record, nil
}

// Checkpoint はバッファ内のレコードをディスクに書き込み、チェックポイントレコードを追加する
func (w *WAL) Checkpoint() error {
	// バッファをフラッシュ
	if err := w.Flush(); err != nil {
		return err
	}
	// チェックポイントレコードを追加
	record := &LogRecord{
		LogType: LogCheckpoint,
		TxnID:   0, // システムレコード
	}

	if err := w.Append(record); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

// Close はWALを閉じる
func (w *WAL) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// LogInsert はINSERT操作をログに記録する
func (w *WAL) LogInsert(txnID uint64, tableName string, rowID uint64, before []byte, after []byte) error {
	return w.Append(&LogRecord{
		LogType:   LogInsert,
		TxnID:     txnID,
		TableName: tableName,
		RowID:     rowID,
		Before:    before,
		After:     after,
	})
}

// LogUpdate はUPDATE操作をログに記録する
func (w *WAL) LogUpdate(txnID uint64, tableName string, rowID uint64, before []byte, after []byte) error {
	return w.Append(&LogRecord{
		LogType:   LogUpdate,
		TxnID:     txnID,
		TableName: tableName,
		RowID:     rowID,
		Before:    before,
		After:     after,
	})
}

// LogDelete はDELETE操作をログに記録する
func (w *WAL) LogDelete(txnID uint64, tableName string, rowID uint64, before []byte) error {
	return w.Append(&LogRecord{
		LogType:   LogDelete,
		TxnID:     txnID,
		TableName: tableName,
		RowID:     rowID,
		Before:    before,
	})
}

// LogBegin はトランザクション開始をログに記録する
func (w *WAL) LogBegin(txnID uint64) error {
	return w.Append(&LogRecord{
		LogType: LogBegin,
		TxnID:   txnID,
	})
}

// LogCommit はトランザクション完了をログに記録する
func (w *WAL) LogCommit(txnID uint64) error {
	return w.Append(&LogRecord{
		LogType: LogCommit,
		TxnID:   txnID,
	})
}

// LogAbort はトランザクション中断をログに記録する
func (w *WAL) LogRollback(txnID uint64) error {
	return w.Append(&LogRecord{
		LogType: LogRollback,
		TxnID:   txnID,
	})
}
