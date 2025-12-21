# Phase 5: クエリ実行エンジン 実装レポート

## 概要

Phase 5 では SQL パーサーとストレージエンジンを接続し、実際にクエリを実行できるようにする。

## 実装ファイル

### `internal/planner/`

| ファイル | 役割 |
|----------|------|
| `plan_node.go` | 実行計画ノードの定義 |
| `planner.go` | AST → Plan 変換 |
| `optimizer.go` | 最適化ルール（後半） |
| `cost.go` | コスト計算（後半） |

### `internal/executor/`

| ファイル | 役割 |
|----------|------|
| `executor.go` | Plan を実行 |
| `result.go` | 実行結果の構造体 |
| `catalog.go` | テーブル・スキーマ管理 |
| `iterator.go` | 行イテレータ |
| `join.go` | JOIN 実装（後半） |
| `aggregate.go` | 集約関数（後半） |

## アーキテクチャ

```
SQL文
  ↓
Parser (pkg/parser/)
  ↓
AST
  ↓
Planner (internal/planner/)
  ↓
Plan Tree
  ↓
Optimizer (internal/planner/)
  ↓
Optimized Plan
  ↓
Executor (internal/executor/)
  ↓
Storage (internal/storage/)
  ↓
結果
```

## Q&A・議論まとめ

### Q1: Catalog とは何か？

**回答:**

Catalog = テーブルの一覧表（カタログ）

```go
type Catalog struct {
    dataDir string                      // データの保存先
    tables  map[string]*storage.Table   // テーブル名 → Table
    schemas map[string]*storage.Schema  // テーブル名 → Schema
    mu      sync.RWMutex                // 並行アクセス保護
}
```

役割:

- 「users テーブルはどこにある？」→ `tables["users"]`
- 「users のカラム構成は？」→ `schemas["users"]`

これがないと、テーブルを探すたびにファイルシステムを走査する必要がある。

### Q2: CreateTable の実装がなぜこうなるのか？

**回答:**

#### データベースの構造

```
データベース
  └── テーブル（users, orders, ...）
        └── ファイル（users.db, orders.db, ...）
              └── ページ（4KB のブロック）
                    └── 行データ
```

**各テーブル = 1つのファイル** という設計。

#### CreateTable の処理の流れ

```go
func (c *Catalog) CreateTable(name string, schema *storage.Schema) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Step 1: 重複チェック
    if _, exists := c.tables[name]; exists {
        return fmt.Errorf("table '%s' already exists", name)
    }

    // Step 2: ファイルパス作成
    // "./data" + "users" + ".db" → "./data/users.db"
    filePath := filepath.Join(c.dataDir, name+".db")

    // Step 3: Pager 作成（ファイルを作成/オープン）
    pager, err := storage.NewPager(filePath)
    if err != nil {
        return fmt.Errorf("failed to create pager: %w", err)
    }

    // Step 4: Table 作成
    table := storage.NewTable(storage.TableName(name), schema, pager)

    // Step 5: マップに登録
    c.tables[name] = table
    c.schemas[name] = schema

    return nil
}
```

#### 各ステップの理由

**Step 1: 重複チェック**

- 同じ名前のテーブルを2回作れないようにする

**Step 2: ファイルパス作成**

- シンプルで分かりやすい
- ファイルシステムで直接確認できる
- テーブルごとに独立したファイル = バックアップしやすい

**Step 3: Pager 作成**

- Pager の役割: ファイルを開く、ページ単位の読み書きを抽象化

```
アプリ → Pager → ファイル
         ↑
     ページ単位で
     読み書き
```

**Step 4: Table 作成**

- Table の役割: Insert(), Scan() など行操作
- 内部で Pager を使ってファイルI/O

**Step 5: マップに登録**

- 後で `SELECT * FROM users` が来たとき、`c.tables["users"]` で取得できるようにする

#### なぜ mutex が必要か

並行アクセスの問題を防ぐため:

```
Goroutine A: CreateTable("users", ...)
Goroutine B: CreateTable("users", ...)

時刻1: A が exists チェック → false
時刻2: B が exists チェック → false  ← まだ A が登録してない！
時刻3: A が tables["users"] = table
時刻4: B が tables["users"] = table  ← 上書き！データ消失
```

Lock で排他制御することで、同時に1つの goroutine しか CreateTable できない。

#### 図解

```
CREATE TABLE users (id INT, name VARCHAR(255));
                    ↓
┌─────────────────────────────────────────────────────────┐
│ Catalog.CreateTable("users", schema)                    │
├─────────────────────────────────────────────────────────┤
│ 1. Lock 取得                                            │
│ 2. tables["users"] 存在チェック → なし                  │
│ 3. Pager 作成 → ./data/users.db ファイル作成            │
│ 4. Table 作成（name, schema, pager を持つ）             │
│ 5. tables["users"] = table                              │
│ 6. schemas["users"] = schema                            │
│ 7. Lock 解放                                            │
└─────────────────────────────────────────────────────────┘
```

### Q3: なぜ tables と schemas を別々に持つのか？

**回答:**

#### 結論: 用途が違う

| 項目 | tables | schemas |
|------|--------|---------|
| 型 | `*storage.Table` | `*storage.Schema` |
| 用途 | **データ操作** | **構造情報** |
| 例 | Insert, Scan, Delete | カラム名、型、制約 |

#### 具体例

```sql
-- schemas が必要な場面（構造を知りたい）
SELECT * FROM users;
→ 「users のカラムは何？」→ schemas["users"].GetColumns()
→ id (INT), name (VARCHAR), age (INT)

-- tables が必要な場面（データを操作したい）
INSERT INTO users VALUES (1, 'alice', 20);
→ tables["users"].Insert(row)
```

#### なぜ Table に Schema を持たせないのか？

実は **Table は Schema を持っている**:

```go
type Table struct {
    name   TableName
    schema *Schema   // ← ここ！
    pager  *Pager
    numPages NumPages
}
```

でも Catalog が両方持つ理由:

```go
// パターン A: tables だけ持つ場合
schema := c.tables["users"].schema  // Table 経由でアクセス

// パターン B: schemas も持つ場合（現在）
schema := c.schemas["users"]        // 直接アクセス
```

**メリット:**

1. **高速アクセス**: Schema だけ欲しいとき Table を経由しなくていい
2. **疎結合**: Schema の取得に Table の内部実装を知らなくていい
3. **将来の拡張**: Schema だけをメモリに残し、Table は遅延ロードできる

### Q4: なぜ Pager と Table を分けているのか？

**回答:**

#### 責務の分離（Single Responsibility）

```
┌─────────────────────────────────────────────────────┐
│                      Table                          │
│  「行」を扱う                                        │
│  - Insert(row)   行を追加                           │
│  - Scan()        全行取得                           │
│  - Find(id)      行を検索                           │
└───────────────────────┬─────────────────────────────┘
                        │ 使う
┌───────────────────────▼─────────────────────────────┐
│                      Pager                          │
│  「ページ」を扱う（4KB ブロック）                     │
│  - ReadPage(id)   ページを読む                      │
│  - WritePage(p)   ページを書く                      │
│  - ファイルI/O の抽象化                             │
└───────────────────────┬─────────────────────────────┘
                        │ 使う
┌───────────────────────▼─────────────────────────────┐
│                   ファイル                          │
│  users.db                                           │
└─────────────────────────────────────────────────────┘
```

#### なぜ分けるのか？

**1. 抽象化レベルが違う**

```
Table:  「この行を保存して」
Pager:  「このバイト列をページ3に書いて」
File:   「このバイト列をオフセット12288に書いて」
```

**2. 再利用性**

```go
// Pager は色々なものに使える
userTable := storage.NewTable("users", schema, pager1)
indexTree := btree.NewBTree(pager2)  // B+Tree もページを使う
walFile := dbtxn.NewWAL(pager3)      // WAL もページを使える
```

**3. テストしやすい**

```go
// Table のテスト: Pager をモックできる
type MockPager struct { ... }
table := NewTable("test", schema, mockPager)

// Pager のテスト: ファイルI/O だけテスト
pager := NewPager("/tmp/test.db")
```

**4. 将来の拡張**

```go
// メモリPager（テスト用）
type MemoryPager struct { pages map[PageID][]byte }

// キャッシュ付きPager
type CachedPager struct {
    underlying *Pager
    cache      map[PageID][]byte
}

// 分散ストレージPager
type DistributedPager struct { ... }
```

#### 図解: 全体の関係

```
┌─────────────────────────────────────────────────────────────┐
│                         Catalog                             │
│  ┌─────────────────────┐  ┌─────────────────────┐           │
│  │ tables              │  │ schemas             │           │
│  │ "users" → Table     │  │ "users" → Schema    │           │
│  │ "orders" → Table    │  │ "orders" → Schema   │           │
│  └─────────────────────┘  └─────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Table     │      │   Table     │      │   Schema    │
│  (users)    │      │  (orders)   │      │  (users)    │
│             │      │             │      │             │
│ - schema ───┼──────┼─────────────┼──────┤ - columns   │
│ - pager     │      │ - pager     │      │   - id      │
│ - numPages  │      │ - numPages  │      │   - name    │
└──────┬──────┘      └──────┬──────┘      │   - age     │
       │                    │             └─────────────┘
       ▼                    ▼
┌─────────────┐      ┌─────────────┐
│   Pager     │      │   Pager     │
│ users.db    │      │ orders.db   │
└─────────────┘      └─────────────┘
```

#### まとめ

| 分離 | 理由 |
|------|------|
| tables と schemas | 用途が違う（データ操作 vs 構造情報）、高速アクセス |
| Pager と Table | 責務が違う（ページI/O vs 行操作）、再利用性、テスト容易性 |

### Q6: Iterator の実装詳細

**質問:** iterator.go で実装した関数の詳細を教えて

**回答:**

#### Iterator インターフェース

```go
type Iterator interface {
    Next() (bool, error)    // 次の行へ進む。行があれば true
    GetRow() *storage.Row   // 現在の行を取得
    Reset()                 // 最初に戻る
    Close() error           // リソース解放
}
```

**なぜこの設計か:**

- `Next()` + `GetRow()` の分離 = カーソルパターン
- `for iter.Next() { row := iter.GetRow() }` という自然なループが書ける

#### tableIterator

```go
type tableIterator struct {
    table *storage.Table    // データ元のテーブル
    rows  []*storage.Row    // Scan() で取得した全行
    index int               // 現在の位置
}
```

**Next() の動作:**

```go
func (i *tableIterator) Next() (bool, error) {
    // 初回呼び出し時に rows をロード（遅延初期化）
    if len(i.rows) == 0 {
        rows, err := i.table.Scan()
        if err != nil {
            return false, err
        }
        i.rows = rows
        i.index = -1  // Next() で 0 になるように
    }
    i.index++
    return i.index < len(i.rows), nil
}
```

**遅延初期化の理由:**

```
NewTableIterator() 時点   →  まだ Scan() しない
     ↓
最初の Next() 呼び出し   →  ここで Scan()
```

- イテレータを作っても使わない場合がある
- 複数のイテレータを組み合わせる場合、実行タイミングを制御できる

**index = -1 の理由:**

```
初期状態:  index = -1
Next() 1回目: index++ → 0  → rows[0] を指す
Next() 2回目: index++ → 1  → rows[1] を指す
```

**GetRow() の境界チェック:**

```go
func (i *tableIterator) GetRow() *storage.Row {
    if i.index < 0 || i.index >= len(i.rows) {
        return nil  // 範囲外なら nil
    }
    return i.rows[i.index]
}
```

- `Next()` を呼ぶ前に `GetRow()` を呼ぶと `index = -1`
- `Next()` が `false` を返した後に呼ぶと範囲外
- panic を防ぐ

#### filterIterator

```go
type filterIterator struct {
    source    Iterator                      // 元のイテレータ
    predicate func(row *storage.Row) bool   // フィルタ条件
    current   *storage.Row                  // 現在の行
}
```

**デコレータパターン:**

```
tableIterator → filterIterator → 結果
    全行          WHERE 条件      絞り込まれた行
```

**Next() の動作:**

```go
func (i *filterIterator) Next() (bool, error) {
    for {
        hasNext, err := i.source.Next()
        if err != nil {
            return false, err
        }
        if !hasNext {
            return false, nil  // もう行がない
        }
        row := i.source.GetRow()
        if i.predicate(row) {
            i.current = row    // 条件に合う行を保存
            return true, nil
        }
        // 条件に合わない → 次の行へ（ループ継続）
    }
}
```

**動作の流れ:**

```
元データ: [Alice(20), Bob(30), Charlie(25)]
条件: age >= 25

Next() 1回目:
  source.Next() → Alice(20) → predicate(20>=25) → false → 次へ
  source.Next() → Bob(30)   → predicate(30>=25) → true  → current = Bob, return true

Next() 2回目:
  source.Next() → Charlie(25) → predicate(25>=25) → true → current = Charlie, return true

Next() 3回目:
  source.Next() → false → return false
```

#### 使用例

```go
// SELECT * FROM users WHERE age >= 20

table, _ := catalog.GetTable("users")
iter := NewTableIterator(table)
filtered := NewFilterIterator(iter, func(row *storage.Row) bool {
    age := row.GetValue("age").(int)
    return age >= 20
})

for {
    hasNext, _ := filtered.Next()
    if !hasNext {
        break
    }
    row := filtered.GetRow()
    fmt.Println(row)
}
filtered.Close()
```

#### イテレータの組み合わせ（将来の拡張）

```
SQL: SELECT name FROM users WHERE age >= 20 ORDER BY name

tableIterator
    ↓
filterIterator (WHERE age >= 20)
    ↓
sortIterator (ORDER BY name)  ← 後で実装
    ↓
projectIterator (SELECT name) ← 後で実装
    ↓
結果
```

イテレータをチェーンすることで、複雑なクエリを表現できる。

### Q5: 「ログ」の種類について

**質問:** catalog.go にログは必要？アプリケーションログと WAL は別物？

**回答:**

#### 「ログ」の種類

| 種類 | 目的 | 例 |
|------|------|-----|
| **アプリケーションログ** | デバッグ・監視 | `log.Printf("user created")` |
| **WAL (Write-Ahead Log)** | データ復旧 | トランザクションの操作記録 |
| **監査ログ** | セキュリティ・コンプライアンス | 誰がいつ何をしたか |

#### アプリケーションログ

```go
log.Printf("CreateTable: %s", name)
log.Printf("Error: %v", err)
```

- 人間が読む
- 消えても DB は壊れない
- 標準出力やファイルに出力

#### WAL (Write-Ahead Log)

```go
// Phase 4 で実装した
wal.LogInsert(txnID, "users", rowID, data)
wal.LogCommit(txnID)
```

- DB エンジンが読む（人間用ではない）
- これがないとデータが消える可能性
- バイナリ形式でディスクに保存

#### 違いの図解

```
┌─────────────────────────────────────────────────────────────┐
│                       アプリケーション                        │
└─────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────┐      ┌─────────────────────┐
│ アプリケーションログ  │      │        WAL          │
│                     │      │                     │
│ log.Printf(...)     │      │ wal.LogInsert(...)  │
│                     │      │                     │
│ 目的: デバッグ       │      │ 目的: データ復旧     │
│ 消えても: OK        │      │ 消えたら: データ消失  │
│ 読む人: 開発者       │      │ 読む人: DB エンジン   │
└─────────────────────┘      └─────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────┐      ┌─────────────────────┐
│   app.log           │      │   wal.log           │
│   (テキスト)         │      │   (バイナリ)         │
└─────────────────────┘      └─────────────────────┘
```

#### 結論

| 質問 | 回答 |
|------|------|
| アプリケーションログは必要？ | 今は不要。後で追加しても OK |
| WAL は catalog.go で必要？ | 不要。Phase 4 で実装済み、Executor で使う |
| 2つは別物？ | **完全に別物**。目的も形式も違う |

## 実装順序

| Step | ファイル | 内容 |
|------|----------|------|
| 1 | `plan_node.go` | PlanNode インターフェースと基本ノード |
| 2 | `result.go` | 実行結果の構造体 |
| 3 | `catalog.go` | テーブル・スキーマ管理 |
| 4 | `iterator.go` | 行イテレータ |
| 5 | `planner.go` | AST → Plan 変換 |
| 6 | `executor.go` | Plan 実行エンジン |
| 7 | REPL 統合 | 実際に SQL 実行 |
| 8 | `join.go` | JOIN 実装 |
| 9 | `aggregate.go` | GROUP BY・集約関数 |
| 10 | `cost.go` | コスト計算 |
| 11 | `optimizer.go` | 最適化ルール |

## 進捗

- [x] Step 1: plan_node.go
- [x] Step 2: result.go
- [x] Step 3: catalog.go（独立パッケージとして分離）
- [x] Step 4: iterator.go
- [x] Step 5: planner.go
- [x] Step 6: executor.go
- [x] Step 7: REPL 統合（Session パターン）
- [x] Step 8: JOIN 実装
- [x] Step 9: aggregate.go（GROUP BY なしの集約関数）
- [ ] Step 10: cost.go
- [ ] Step 11: optimizer.go

## 完了した実装

### Parser の変更

AST 型をパブリックに変更し、外部パッケージからアクセス可能に:

- `SelectStatement`, `InsertStatement`, `UpdateStatement`, `DeleteStatement`
- `CreateTableStatement`, `ExplainStatement`
- `Identifier`, `StringLiteral`, `IntegerLiteral`, `BooleanLiteral`
- `BinaryExpression`, `Asterisk`, `ColumnDefinition`, `OrderByClause`

### Storage の変更

`Table.Scan()` メソッドを追加:

```go
func (t *Table) Scan() ([]*Row, error)
```

テーブル内の全行をスキャンして返す。

### Catalog パッケージの分離

循環参照を解消するため、`internal/catalog/` として独立パッケージに:

```go
type Catalog interface {
    CreateTable(name string, schema *storage.Schema) error
    GetTable(name string) (*storage.Table, error)
    DropTable(name string) error
    TableExists(name string) bool
    ListTables() []*storage.Table
    GetSchema(name string) (*storage.Schema, error)
    Close() error
}
```

### Executor の実装

以下の PlanNode を実行可能:

| Node | 機能 |
|------|------|
| ScanNode | テーブル全スキャン |
| FilterNode | WHERE 条件でフィルタリング |
| ProjectNode | SELECT カラム選択 |
| InsertNode | INSERT 文実行 |
| CreateTableNode | CREATE TABLE 文実行 |
| UpdateNode | (未実装) |
| DeleteNode | (未実装) |

### テスト

各パッケージに包括的なテストを追加:

- `internal/catalog/catalog_test.go` - 9 tests
- `internal/planner/planner_test.go` - 7 tests
- `internal/planner/plan_node_test.go` - 11 tests
- `internal/executor/executor_test.go` - 10 tests
- `internal/executor/result_test.go` - 11 tests
- `internal/session/session_test.go` - 8 tests

### Int32/Int64 型のサポート

数値型の完全なサポートを実装:

**storage/types.go:**
```go
// Int32
type Int32Value int32

func (v Int32Value) Type() ColumnType { return ColumnTypeInt32 }
func (v Int32Value) Size() int { return 4 }
func (v Int32Value) Encode() []byte {
    buf := make([]byte, 4)
    binary.LittleEndian.PutUint32(buf, uint32(v))
    return buf
}

// Int64
type Int64Value int64

func (v Int64Value) Type() ColumnType { return ColumnTypeInt64 }
func (v Int64Value) Size() int { return 8 }
func (v Int64Value) Encode() []byte {
    buf := make([]byte, 8)
    binary.LittleEndian.PutUint64(buf, uint64(v))
    return buf
}
```

**storage/row.go - DecodeRow:**
```go
case ColumnTypeInt32:
    val := int32(binary.LittleEndian.Uint32(data[offset:]))
    values[i] = Int32Value(val)
    offset += 4

case ColumnTypeInt64:
    val := int64(binary.LittleEndian.Uint64(data[offset:]))
    values[i] = Int64Value(val)
    offset += 8
```

**planner/planner.go - parseColumnType:**
```go
case "INT":
    return storage.ColumnTypeInt32
case "BIGINT":
    return storage.ColumnTypeInt64
```

**planner/plan_node.go - extractValue:**
```go
case storage.Int32Value:
    return int(val)
case storage.Int64Value:
    return int64(val)
```

**executor/executor.go - toStorageValue:**
```go
case int:
    return storage.Int32Value(int32(v)), nil
case int32:
    return storage.Int32Value(v), nil
case int64:
    return storage.Int64Value(v), nil
```

### Table.Insert バグ修正

複数行の INSERT が正しく動作しない問題を修正:

**変更前（バグあり）:**
```go
_, err = page.InsertRow(rowData)
if err != ErrPageFull {
    return err  // err == nil でも return してしまう！
}
if err == nil {  // ここには到達しない
    return t.savePage(...)
}
```

**変更後:**
```go
_, err = page.InsertRow(rowData)
if err == nil {
    // 挿入成功、ページを保存
    return t.savePage((t.numPages - 1).ToPageID(), page)
}
if err != ErrPageFull {
    // ErrPageFull 以外のエラー
    return err
}
// ErrPageFull の場合は新しいページを作成
```

### Session パッケージの実装

TiDB の session パッケージを参考に、SQL 実行パイプラインを統合:

```go
// internal/session/session.go
type Session interface {
    Execute(sqlQuery string) (executor.ResultSet, error)
    Close() error
}

func (s *session) Execute(sqlQuery string) (executor.ResultSet, error) {
    // 1. SQL を解析して Statement を作成
    p := parser.NewParser(parser.NewLexer(sqlQuery))
    stmt, err := p.Parse()
    if err != nil {
        return nil, err
    }
    // 2. Statement を PlanNode に変換
    plan, err := s.planner.Plan(stmt)
    if err != nil {
        return nil, err
    }
    // 3. PlanNode を実行して結果を返す
    return s.executor.Execute(plan)
}
```

**実行パイプライン:**

```
SQL文字列
    ↓
Lexer (トークン分割)
    ↓
Parser (AST 生成)
    ↓
Planner (PlanNode 生成)
    ↓
Executor (実行)
    ↓
ResultSet
```

### REPL の変更

Session を使った SQL 実行に変更:

```go
// pkg/repl/repl.go
type Repl struct {
    input   io.Reader
    output  io.Writer
    session session.Session  // parser.Parser から変更
}

func (r *Repl) eval(input string) {
    result, err := r.session.Execute(input)
    if err != nil {
        fmt.Fprintln(r.output, "Error:", err)
        return
    }
    fmt.Fprintln(r.output, result.String())
}
```

### main.go の変更

コンポーネントの組み立て:

```go
// cmd/godb/main.go
func main() {
    dataDir := "data"
    catalog, err := catalog.NewCatalog(dataDir)
    if err != nil {
        log.Fatalf("Failed to create catalog: %v", err)
    }
    executor := executor.NewExecutor(catalog)
    session := session.NewSession(catalog, executor)
    defer session.Close()
    repl := repl.NewRepl(os.Stdin, os.Stdout, session)
    repl.Run()
}
```

## Q&A: スキーマ永続化について

### Q7: データは永続化されているのにスキーマが永続化されないのはなぜ？

**回答:**

#### 永続化の状態

| 何が | 永続化されている？ | どこに |
|------|------------------|--------|
| 行データ（INSERT した内容） | ○ | `.db` ファイルにページとして書き込み |
| スキーマ（カラム名、型） | × | メモリ上の `map` のみ |
| テーブル名とファイルの対応 | × | メモリ上の `map` のみ |

#### 具体例

**セッション1:**
```
godb> CREATE TABLE users (id INT, name VARCHAR(255))
→ users.db 作成（0バイト）
→ メモリ: schemas["users"] = {id: INT, name: VARCHAR}

godb> INSERT INTO users VALUES (1, 'alice')
→ users.db に 4KB のページが書き込まれる
→ ファイル内容: [ページヘッダ][スロット配列][1, 'alice' のバイト列]
```

**セッション2（REPL再起動後）:**
```
godb> SELECT * FROM users
→ Error: table not found

なぜ？
→ NewCatalog() は空の map を作るだけ
→ users.db ファイルは存在するが、カタログに登録されていない
→ さらに、ファイルにはスキーマ情報（カラム名、型）が書かれていない
→ たとえファイルを読んでもどう解釈するかがわからない
```

#### 図解

```
[CREATE TABLE]
  ↓
catalog.CreateTable()
  ↓
storage.NewPager() → ファイル作成（空）
storage.NewTable() → メモリ上にテーブル作成
c.tables[name] = table → メモリに登録
c.schemas[name] = schema → メモリに登録
  ↓
（ファイルにはスキーマ情報が書かれない！）

[INSERT]
  ↓
table.Insert(row)
  ↓
page.InsertRow() → ページにデータ追加
pager.WritePage() → ファイルにページ書き込み ← ここでデータは永続化される

[REPL再起動]
  ↓
NewCatalog() → 空の map
  ↓
users.db ファイルは存在するがスキーマがないので読めない
```

#### Pager の担当範囲

Pager は「ページ単位の読み書き」を担当:
- ファイルの作成・オープン・クローズ
- `ReadPage(pageID)` / `WritePage(page)`
- ページ数の管理

**Pager が担当しないこと:**
- スキーマ情報の永続化
- テーブル一覧の管理
- 起動時のメタデータ復元

#### 解決方法（将来の実装）

1. **システムテーブル方式** - `_schema` テーブルにスキーマ情報を保存
2. **ファイルヘッダ方式** - 各 `.db` ファイルの先頭にスキーマを保存
3. **メタデータファイル方式** - `catalog.json` に全テーブル情報を保存

**TiDB の方式:**
- TiKV に `m_` プレフィックス付きキーでメタデータを保存
- スキーマ情報をシリアライズして永続化

#### 現在のプロジェクト計画での位置づけ

- Phase 1: Pager は「ページ単位のI/O」まで ← 完了
- Phase 10: `cluster/metadata.go` でメタデータ管理 ← 将来実装

現在の Phase 5 では、同一セッション内での SQL 実行が動作すれば OK。

## Step 8: JOIN 実装

### 概要

INNER JOIN クエリのサポートを追加。修飾子付きカラム参照（`users.id = orders.user_id`）にも対応。

### 変更ファイル

| ファイル | 変更内容 |
|----------|----------|
| `parser/token.go` | `TOKEN_JOIN`, `TOKEN_ON`, `TOKEN_DOT` 追加 |
| `parser/lexer.go` | `.` 文字のトークン化 |
| `parser/ast.go` | `Join`, `QualifiedIdentifier` 構造体追加 |
| `parser/parser.go` | JOIN 句と修飾子付きカラムのパース |
| `planner/plan_node.go` | `JoinNode`, `ColumnRef.TableName` 追加 |
| `planner/planner.go` | `JoinNode` 作成、`QualifiedIdentifier` 変換 |
| `executor/executor.go` | `executeJoin()` (Nested Loop Join) |
| `executor/result.go` | `GetColumnCount()` の schema 対応 |
| `storage/schema.go` | `Schema.Merge()` メソッド追加 |
| `session/session_test.go` | JOIN テスト 3 件追加 |

### JoinNode の構造

```go
type JoinNode struct {
    Left      PlanNode   // 左テーブル
    Right     PlanNode   // 右テーブル
    JoinType  JoinType   // INNER, LEFT, RIGHT, FULL OUTER
    Condition Expression // 結合条件
}

type JoinType string

const (
    JoinTypeInner JoinType = "INNER"
    JoinTypeLeft  JoinType = "LEFT"
    JoinTypeRight JoinType = "RIGHT"
    JoinTypeFull  JoinType = "FULL OUTER"
)
```

### Nested Loop Join アルゴリズム

```go
func (e *executor) executeJoin(node *planner.JoinNode) (ResultSet, error) {
    leftResult, _ := e.Execute(node.Left)
    rightResult, _ := e.Execute(node.Right)
    joinSchema := node.Schema()

    var joinedRows []*storage.Row
    for _, leftRow := range leftResult.GetRows() {
        for _, rightRow := range rightResult.GetRows() {
            mergedRow := mergeRows(leftRow, rightRow)
            result, _ := node.Condition.Evaluate(mergedRow, joinSchema)
            if match, ok := result.(bool); ok && match {
                joinedRows = append(joinedRows, mergedRow)
            }
        }
    }
    return NewResultSetWithRowsAndSchema(joinSchema, joinedRows), nil
}
```

計算量: O(n × m) where n = 左テーブル行数, m = 右テーブル行数

### 修飾子付きカラム参照

```sql
-- 対応前（カラム名のみ）
SELECT * FROM users JOIN orders ON id = user_id

-- 対応後（テーブル名.カラム名）
SELECT * FROM users JOIN orders ON users.id = orders.user_id
```

パース処理:

```go
case TOKEN_IDENT:
    ident := p.currentToken.literal
    if p.peekTokenIs(TOKEN_DOT) {
        p.nextToken() // . へ
        p.nextToken() // カラム名へ
        return &QualifiedIdentifier{TableName: ident, ColumnName: p.currentToken.literal}, nil
    }
    return &Identifier{Value: ident}, nil
```

### Schema.Merge

```go
func (s *Schema) Merge(other *Schema) *Schema {
    mergedColumns := make([]Column, 0, len(s.columns)+len(other.columns))
    mergedColumns = append(mergedColumns, s.columns...)
    mergedColumns = append(mergedColumns, other.columns...)
    return NewSchema(s.tableName, mergedColumns)
}
```

JOIN 結果のスキーマ = 左テーブルのカラム + 右テーブルのカラム

### テスト

```go
func TestSessionJoin(t *testing.T)                    // 基本 JOIN
func TestSessionJoinNoMatch(t *testing.T)             // マッチなし
func TestSessionJoinWithQualifiedColumns(t *testing.T) // 修飾子付き
```

### バグ修正

`dbtxn/recovery.go`: `undo()` 後に `Flush()` を呼び出すように修正

```go
// 修正前
return nil

// 修正後
return rm.wal.Flush()
```

UNDO ログがディスクに永続化されず、リカバリテストが失敗していた。

## Step 9: 集約関数（GROUP BY, COUNT, SUM）実装

### 概要

集約関数（COUNT, SUM, AVG, MAX, MIN）と GROUP BY 句のサポートを追加。

### データフロー図

```
SQL クエリ
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ Parser (parser.go)                                              │
│   parseSelectColumns() で集約関数を検出                          │
│   parseGroupBy() で GROUP BY カラムを抽出                        │
│   ↓                                                             │
│   SelectStatement {                                             │
│     Columns: [AggregateFunction{Function: "COUNT", Arg: *}]     │
│     GroupBy: ["name"]                                           │
│   }                                                             │
└─────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ Planner (planner.go)                                            │
│   hasAggregateFunction() で集約関数の有無を判定                   │
│   extractAggregateFunctions() で集約式を抽出                     │
│   ↓                                                             │
│   AggregateNode {                                               │
│     Child: ScanNode/FilterNode                                  │
│     GroupBy: ["name"]                                           │
│     Aggregates: [{Function: "COUNT", Column: ""}]               │
│   }                                                             │
└─────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────────┐
│ Executor (executor.go / aggregate.go)                           │
│   executeAggregate() で集約処理を実行                            │
│   - GROUP BY なし: 全行を1グループとして集約                     │
│   - GROUP BY あり: グループ化して各グループを集約                 │
│   ↓                                                             │
│   ResultSet (集約結果)                                          │
└─────────────────────────────────────────────────────────────────┘
```

### 変更ファイル一覧

| ファイル | 変更内容 | 状態 |
|----------|----------|------|
| `parser/token.go` | `TOKEN_COUNT`, `TOKEN_SUM`, `TOKEN_AVG`, `TOKEN_MAX`, `TOKEN_MIN` 追加 | ✅ 完了 |
| `parser/ast.go` | `AggregateFunction` 構造体、`SelectStatement.GroupBy` 追加 | ✅ 完了 |
| `parser/parser.go` | `parseAggregateFunction()`, `parseGroupBy()`, `isAggregateFunctionToken()` 追加 | ✅ 完了 |
| `planner/plan_node.go` | `AggregateNode`, `AggregateExpression` 追加 | ✅ 完了 |
| `planner/planner.go` | `hasAggregateFunction()`, `extractAggregateFunctions()`, ProjectNode との排他制御 | ✅ 完了 |
| `executor/executor.go` | `executeAggregate()` 呼び出し追加 | ✅ 完了 |
| `executor/aggregate.go` | 集約処理の実装（COUNT, SUM, AVG, MAX, MIN） | ✅ 完了 |
| `session/session_test.go` | 集約関数テスト 5 件追加 | ✅ 完了 |

### 実装済みの構造体

#### parser/ast.go - AggregateFunction

```go
// AggregateFunction は集約関数を表す
// 例: COUNT(*), SUM(amount), AVG(price)
type AggregateFunction struct {
    Function string     // COUNT, SUM, AVG, MAX, MIN
    Argument Expression // 引数（* または カラム名）
}
```

#### parser/ast.go - SelectStatement

```go
type SelectStatement struct {
    Columns []Expression    // 選択するカラム（Identifier または AggregateFunction）
    From    string          // テーブル名
    Join    *Join           // 結合条件
    Where   Expression      // 条件
    GroupBy []string        // GROUP BY カラム ← 追加
    OrderBy []OrderByClause // ソート条件
    Limit   *int            // 最大行数
    Offset  *int            // オフセット
}
```

#### planner/plan_node.go - AggregateNode

```go
// AggregateNode は集約操作を表す
type AggregateNode struct {
    Child      PlanNode              // 入力ノード（ScanNode, FilterNode 等）
    GroupBy    []string              // GROUP BY カラム
    Aggregates []AggregateExpression // 集約式のリスト
    schema     *storage.Schema       // 結果スキーマ
}

// AggregateExpression は個々の集約式を表す
type AggregateExpression struct {
    Function string // COUNT, SUM, AVG, MAX, MIN
    Column   string // 対象カラム名（* の場合は空文字）
    Alias    string // AS で指定した別名（オプション）
}
```

### パース処理の流れ

```sql
SELECT COUNT(*), name FROM users GROUP BY name
```

1. **parseSelectColumns()** で `COUNT(*)` を検出
   - `isAggregateFunctionToken()` が `TOKEN_COUNT` を検出
   - `parseAggregateFunction()` で `AggregateFunction{Function: "COUNT", Argument: Asterisk{}}` を作成

2. **parseGroupBy()** で `GROUP BY name` を処理
   - `stmt.GroupBy = ["name"]` に設定

3. **結果の AST**:
   ```go
   SelectStatement{
       Columns: [
           &AggregateFunction{Function: "COUNT", Argument: &Asterisk{}},
           &Identifier{Value: "name"},
       ],
       From:    "users",
       GroupBy: []string{"name"},
   }
   ```

### プランニング処理の流れ

```go
// planner.go の planSelect()

// 1. テーブルスキャン
plan := &ScanNode{TableName: "users", ...}

// 2. WHERE があればフィルタ
if stmt.Where != nil {
    plan = &FilterNode{Child: plan, Condition: ...}
}

// 3. 集約関数があれば AggregateNode を追加
if hasAggregateFunction(stmt.Columns) {
    aggregates := extractAggregateFunctions(stmt.Columns)
    plan = &AggregateNode{
        Child:      plan,
        GroupBy:    stmt.GroupBy,       // ["name"]
        Aggregates: aggregates,          // [{Function: "COUNT", Column: ""}]
    }
}
```

### 実装済み: executor/aggregate.go

```go
func (e *executor) executeAggregate(node *planner.AggregateNode) (ResultSet, error) {
    childResult, err := e.Execute(node.Child)
    if err != nil {
        return nil, err
    }
    rows := childResult.GetRows()

    // GROUP BY がない場合: 全行を1グループとして集約
    if len(node.GroupBy) == 0 {
        values := make([]storage.Value, len(node.Aggregates))
        for i, agg := range node.Aggregates {
            result, err := e.calculateAggregate(agg, rows, childResult.GetSchema())
            if err != nil {
                return nil, err
            }
            values[i] = result
        }
        resultRow := storage.NewRow(values)
        return NewResultSetWithRowsAndSchema(node.Schema(), []*storage.Row{resultRow}), nil
    }
    // TODO: GROUP BY ありの場合
    return NewResultSetWithMessage("group by not implemented"), nil
}

func (e *executor) calculateAggregate(agg planner.AggregateExpression, rows []*storage.Row, schema *storage.Schema) (storage.Value, error) {
    funcName := strings.ToUpper(agg.Function)
    switch funcName {
    case "COUNT":
        return storage.Int64Value(int64(len(rows))), nil
    case "SUM":
        colIdx := schema.GetColumnIndex(agg.Column)
        if colIdx < 0 {
            return nil, fmt.Errorf("column not found: %s", agg.Column)
        }
        var sum int64
        for _, row := range rows {
            val := row.GetValues()[colIdx]
            sum += int64(val.(storage.Int32Value))
        }
        return storage.Int64Value(sum), nil
    case "AVG":
        // 実装済み: SUM / COUNT
    case "MAX":
        // 実装済み: 最初の行を初期値として最大値を探す
    case "MIN":
        // 実装済み: 最初の行を初期値として最小値を探す
    }
    return nil, fmt.Errorf("unsupported aggregate function: %s", agg.Function)
}
```

### 対応する SQL 例

```sql
-- GROUP BY なし（全行を集約）✅ 対応済み
SELECT COUNT(*) FROM users
SELECT SUM(amount) FROM orders
SELECT AVG(score) FROM scores
SELECT MAX(value), MIN(value) FROM values_table

-- WHERE + 集約関数 ✅ 対応済み
SELECT COUNT(*) FROM products WHERE price > 100

-- GROUP BY あり ⏳ TODO
SELECT name, COUNT(*) FROM users GROUP BY name
```

### テスト

```go
func TestSessionAggregateCount(t *testing.T)     // COUNT(*)
func TestSessionAggregateSum(t *testing.T)       // SUM(column)
func TestSessionAggregateAvg(t *testing.T)       // AVG(column)
func TestSessionAggregateMaxMin(t *testing.T)    // MAX, MIN
func TestSessionAggregateWithWhere(t *testing.T) // WHERE + COUNT
```

### 修正内容

1. **planner.go**: 集約関数がある場合は `ProjectNode` を追加しない
   - `if hasAggregateFunction` と `else if !isSelectAll` の排他制御

2. **aggregate.go**:
   - `strings.ToUpper(agg.Function)` で関数名を正規化（小文字対応）
   - `colIdx < 0` のエラーチェック追加
   - MAX/MIN の初期値を最初の行の値に設定（0 だと負数で失敗）

3. **session_test.go**: 型アサーションを `storage.Int64Value` に修正

### TODO

- [ ] GROUP BY ありの集約処理（`groupRows()` の実装）

## 次のステップ

Step 10 以降: コスト計算、最適化の実装。

または、GROUP BY の完全実装を先に行う場合は別途対応が必要。
