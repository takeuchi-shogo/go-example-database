# Phase 4: トランザクション実装レポート

## 概要

Phase 4 では WAL（Write-Ahead Log）、トランザクション管理、クラッシュリカバリを実装しました。

## 実装ファイル (`internal/dbtxn/`)

| ファイル | 役割 |
|----------|------|
| `wal.go` | Write-Ahead Log の管理 |
| `wal_test.go` | WAL のテスト |
| `transaction.go` | トランザクション管理 (BEGIN / COMMIT / ROLLBACK) |
| `transaction_test.go` | トランザクションのテスト |
| `recovery.go` | クラッシュリカバリ (REDO / UNDO) |
| `recovery_test.go` | リカバリのテスト |

## 主要概念

### WAL (Write-Ahead Log)

データを変更する前にログを先に書く仕組み。クラッシュ時のデータ復旧を可能にする。

```
┌─────────────────────────────────────────────────────────┐
│  アプリのバッファ  →  OS バッファ  →  ディスク          │
│   (WAL.buffer)      (カーネル)       (永続化)           │
│                                                         │
│  Flush() で両方を通過させて、確実にディスクへ書く        │
└─────────────────────────────────────────────────────────┘
```

### ログタイプ

| タイプ | 説明 |
|--------|------|
| LogBegin | トランザクション開始 |
| LogCommit | トランザクションコミット |
| LogRollback | トランザクションロールバック |
| LogInsert | INSERT 操作 |
| LogUpdate | UPDATE 操作 |
| LogDelete | DELETE 操作 |
| LogCheckpoint | チェックポイント |

### トランザクション状態

| 状態 | 説明 |
|------|------|
| TxnStateActive | 実行中 |
| TxnStateCommitted | コミット済み |
| TxnStateRolledBack | ロールバック済み |

### クラッシュリカバリ

| 処理 | 説明 |
|------|------|
| REDO | コミット済みトランザクションの変更を再適用 |
| UNDO | 未コミットトランザクションの変更を取り消す |

## Q&A・議論まとめ

### Q1: パッケージ名について

**問題:** `tx` は一般的な変数名と衝突しやすい

**解決:** `dbtxn` を採用

### Q2: WAL はなぜ必要か？

**回答:**

銀行振込の例で説明:

1. A の残高を減らす
2. B の残高を増やす

途中でクラッシュすると、A のお金が消える。WAL があれば:

- ログを先に書く
- 再起動時にログを読んで復旧

### Q3: メモリ、ディスク、Flush の関係は？

**回答:**

```
メモリ（RAM）: 高速だが電源を切ると消える
ディスク: 遅いが永続化される
Flush: メモリ → ディスクへ強制書き込み
fsync: OS バッファを飛ばしてディスクへ直接書く
```

### Q4: OS バッファとは？

**回答:**

- OS がファイル I/O を高速化するために用意しているメモリ領域
- `write()` を呼んでもすぐにディスクには書かれない
- `fsync()` で強制的にディスクへ書き込む

### Q5: Rollback 時に UNDO は必要？

**回答:**

Phase 4 では不要。理由:

- トランザクション中の変更はまだディスクに書かれていない前提
- UNDO は recovery.go のクラッシュリカバリで実装

## バグ修正

### 1. transaction.go: デッドロック

**問題:** `Commit` と `Rollback` で `tm.mu.Lock()` を2回呼んでいた

**修正前:**

```go
func (tm *TxnManager) Commit(txn *Transaction) error {
    tm.mu.Lock()         // 1回目
    defer tm.mu.Unlock()
    // ...
    tm.mu.Lock()         // 2回目 → デッドロック!
    delete(tm.activeTxns, txn.ID)
    tm.mu.Unlock()
}
```

**修正後:**

```go
func (tm *TxnManager) Commit(txn *Transaction) error {
    txn.mu.Lock()
    defer txn.mu.Unlock()
    // ...
    tm.mu.Lock()
    delete(tm.activeTxns, txn.ID)
    tm.mu.Unlock()
}
```

### 2. ファイル名のタイポ

**問題:** `recorery.go` → `recovery.go`

## ゴール

- WAL による永続化
- BEGIN / COMMIT / ROLLBACK が動作すること
- クラッシュリカバリの基本構造

## PR

- [#7 feat(dbtxn): トランザクション管理の実装](https://github.com/takeuchi-shogo/go-example-database/pull/7)

## 次のフェーズ

Phase 5: クエリ実行エンジン（予定）

- SQL パーサーとストレージエンジンの接続
- 実際のデータ操作の実行
