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
- [x] Step 3: catalog.go
- [x] Step 4: iterator.go
- [x] Step 5: planner.go
- [ ] Step 6: executor.go
- [ ] Step 7: REPL 統合

## 次のステップ

Step 6 の executor.go の実装を開始する。
