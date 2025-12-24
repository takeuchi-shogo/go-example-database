# Phase 4.1: トランザクション統合レポート

## 概要

Phase 4 で実装した WAL とトランザクション管理を、実際のクエリ実行パイプラインに統合しました。

## 実装内容

### Step 1: WAL と Executor の統合

Executor に WAL を渡し、INSERT 操作時に WAL ログを記録するようにしました。

**変更ファイル:** `internal/executor/executor.go`

```go
type executor struct {
    catalog internalcatalog.Catalog
    wal     *dbtxn.WAL
    txnID   uint64
}

func NewExecutor(c internalcatalog.Catalog, wal *dbtxn.WAL) Executor {
    return &executor{catalog: c, wal: wal, txnID: 0}
}
```

INSERT 実行時に WAL へログを書き込む：

```go
func (e *executor) executeInsert(plan *planner.InsertPlan) (ResultSet, error) {
    // ... INSERT 処理 ...

    if e.wal != nil && e.txnID > 0 {
        afterData, err := storage.EncodeRow(row, schema)
        if err != nil {
            return nil, err
        }
        e.wal.LogInsert(e.txnID, plan.TableName, uint64(row.ID), nil, afterData)
    }
    return NewResultSetWithMessage("INSERT executed successfully"), nil
}
```

### Step 2: TxnManager と Session の統合

Session に TxnManager を追加し、BEGIN / COMMIT / ROLLBACK コマンドを処理できるようにしました。

**変更ファイル:**
- `internal/parser/token.go` - トランザクショントークン追加
- `internal/parser/ast.go` - トランザクション AST 追加
- `internal/parser/parser.go` - トランザクション文のパース
- `internal/session/session.go` - トランザクション管理

```go
type session struct {
    catalog    catalog.Catalog
    executor   executor.Executor
    planner    planner.Planner
    wal        *dbtxn.WAL
    txnManager *dbtxn.TxnManager
    currentTxn *dbtxn.Transaction
}

func (s *session) Execute(sqlQuery string) (executor.ResultSet, error) {
    stmt, err := parser.NewParser(parser.NewLexer(sqlQuery)).Parse()
    if err != nil {
        return nil, err
    }
    switch stmt.(type) {
    case *parser.BeginStatement:
        return s.Begin()
    case *parser.CommitStatement:
        return s.Commit()
    case *parser.RollbackStatement:
        return s.Rollback()
    default:
        return s.executeSQL(stmt)
    }
}
```

### Step 3: Recovery の修正

Recovery 処理で WAL へ再度ログを書き込んでいた不要なコードを削除しました。

**変更前（誤り）:**
```go
case LogInsert:
    table.Insert(row)
    rm.wal.LogInsert(...)  // 不要！
```

**変更後（正しい）:**
```go
case LogInsert:
    table.Insert(row)
    // WAL への書き込みは不要
```

## 追加したトークン・AST

### トークン (`token.go`)

| トークン | 説明 |
|----------|------|
| TOKEN_BEGIN | BEGIN |
| TOKEN_COMMIT | COMMIT |
| TOKEN_ROLLBACK | ROLLBACK |

### AST (`ast.go`)

| 構造体 | 説明 |
|--------|------|
| BeginStatement | BEGIN 文 |
| CommitStatement | COMMIT 文 |
| RollbackStatement | ROLLBACK 文 |

## バグ修正

### 1. gob シリアライズエラー

**問題:** `gob: type not registered for interface: storage.Int32Value`

**原因:** `storage.Value` インターフェースの具象型が gob に登録されていなかった

**修正:** `internal/storage/types.go` に init() を追加

```go
func init() {
    gob.Register(Int32Value(0))
    gob.Register(Int64Value(0))
    gob.Register(StringValue(""))
    gob.Register(BoolValue(false))
}
```

### 2. WAL ファイルパスエラー

**問題:** `open .../001: is a directory`

**原因:** `NewWAL` にディレクトリパスを渡していた

**修正:** ファイルパスを渡すように変更

```go
// Before
dbtxn.NewWAL(tempDir)

// After
dbtxn.NewWAL(filepath.Join(tempDir, "wal.log"))
```

### 3. TxnManager nil ポインタ

**問題:** `txnManager` が初期化されていなかった

**修正:** `NewSession` で初期化

```go
func NewSession(...) Session {
    txnManager := dbtxn.NewTxnManager(wal)
    return &session{
        // ...
        txnManager: txnManager,
        currentTxn: nil,
    }
}
```

## テスト追加

`internal/session/session_test.go` にトランザクションテストを追加：

| テスト | 内容 |
|--------|------|
| TestSession_Begin | BEGIN 成功 |
| TestSession_BeginTwice | 二重 BEGIN エラー |
| TestSession_CommitWithoutBegin | BEGIN なし COMMIT エラー |
| TestSession_RollbackWithoutBegin | BEGIN なし ROLLBACK エラー |
| TestSession_BeginCommit | BEGIN → COMMIT フロー |

### Step 4: TiDB 方式の RowID 追加

TiDB/CockroachDB のような分散 DB を参考に、論理キー（RowID）方式を採用しました。

**変更ファイル:**
- `internal/storage/row.go` - Row に RowID フィールド追加
- `internal/storage/table.go` - Table に rowIndex マップ、Update/Delete メソッド追加

#### Row の変更

```go
type Row struct {
    rowID  int64    // 論理キー（TiDB の _tidb_rowid 相当）
    values []Value
}

func NewRowWithID(rowID int64, values []Value) *Row
func (r *Row) GetRowID() int64
func (r *Row) SetRowID(rowID int64)
```

#### Table の変更

```go
type Table struct {
    // ...
    nextRowID int64                 // 次に割り当てる RowID
    rowIndex  map[int64]RowLocation // RowID → 物理位置
}

type RowLocation struct {
    pageID PageID
    slotID int64   // ページ内のスロット番号
}

// Insert で RowID を自動採番
func (t *Table) Insert(row *Row) error

// RowID で行を更新
func (t *Table) Update(rowID int64, row *Row) (*Row, error)

// RowID で行を削除
func (t *Table) Delete(rowID int64) (*Row, error)

// RowID で行を取得
func (t *Table) FindByRowID(rowID int64) (*Row, error)

// 起動時に rowIndex を再構築
func (t *Table) rebuildIndex() error
```

#### Encode/Decode の変更

Row のシリアライズに RowID を含めるよう変更:

```go
func (r *Row) Encode() []byte {
    var buf []byte
    // 1. RowID を先頭に書き込み（8 bytes）
    rowIDBytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(rowIDBytes, uint64(r.rowID))
    buf = append(buf, rowIDBytes...)
    // 2. カラム値をエンコード
    // ...
}
```

#### RowID vs SlotID

| 用語 | 意味 | 用途 |
|------|------|------|
| RowID | 論理キー（TiDB の `_tidb_rowid`） | WAL、ユーザー向け識別子 |
| SlotID | ページ内スロット番号 | 物理位置の特定 |

#### テスト追加

| テスト | 内容 |
|--------|------|
| TestRow_RowID | RowID の取得・設定 |
| TestNewRowWithID | RowID 付きで行を作成 |
| TestRowEncodeWithRowID | RowID を含む Encode |
| TestDecodeRowWithRowID | RowID を含む Decode |
| TestRowSerializeDeserialize | gob シリアライズで RowID が保持される |

## 残作業

| 項目 | 状態 |
|------|------|
| Executor の executeUpdate 実装 | 未実装 |
| Executor の executeDelete 実装 | 未実装 |
| UPDATE の WAL 統合 | 未実装 |
| DELETE の WAL 統合 | 未実装 |
| Recovery REDO/UNDO の実装 | 未実装 |

## 学んだこと

1. **WAL の目的**: データ変更前にログを書くことで、クラッシュ時の復旧を可能にする
2. **Recovery の原則**: REDO/UNDO は実際のテーブル操作を行う（WAL への再書き込みではない）
3. **gob シリアライズ**: インターフェースの具象型は明示的に登録が必要
4. **RowID vs 物理位置**: TiDB/CockroachDB は論理キー（RowID）方式を採用。分散環境では物理位置より論理キーの方が扱いやすい
5. **PostgreSQL との違い**: PostgreSQL は ctid（物理位置）を使うが、分散 DB は KV ストアベースなので論理キーが自然
