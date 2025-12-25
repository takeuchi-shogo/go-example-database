# minidb 開発ロードマップ

## 実装進捗サマリー

| Phase | 状態 | 備考 |
|-------|------|------|
| Phase 1: ストレージエンジン | ✅ 完了 | |
| Phase 2: インデックス | ✅ 完了 | Delete は未実装 |
| Phase 3: SQLパーサー & REPL | ✅ 完了 | |
| Phase 4: トランザクション | ✅ 完了 | WAL/Recovery 統合完了 |
| Phase 5: クエリ最適化 & 高度な機能 | ✅ 完了 | |
| Phase 6: レプリケーション | 🔜 次のフェーズ | |

---

## Phase 1: ストレージエンジン ✅ 完了

- [x] page.go - ページ構造体
- [x] pager.go - ファイル ↔ ページの読み書き
- [x] types.go - カラム型定義（Int32, Int64, String, Bool）
- [x] schema.go - スキーマ定義
- [x] row.go - 行のシリアライズ/デシリアライズ
- [x] page_layout.go - スロット配列方式のページレイアウト
- [x] table.go - テーブル操作（Insert, Scan）

---

## Phase 2: インデックス ✅ 完了

- [x] btree.go - B+Tree の実装
  - [x] ノード構造（リーフノード / 内部ノード）
  - [x] 検索（Search）- 二分探索で O(log n)
  - [x] 挿入（Insert）
  - [x] ノード分割（Split）
  - [ ] 削除（Delete）※後回し
- [x] index.go - インデックス管理
  - [x] プライマリキーインデックス
  - [x] ユニークインデックス
  - [x] セカンダリインデックス

**達成**: `table.Find(id)` が O(log n) で動く

---

## Phase 3: SQLパーサー & REPL ✅ 完了

- [x] lexer.go - トークン分割（字句解析）
- [x] token.go - トークン定義
- [x] ast.go - 抽象構文木（AST）
- [x] parser.go - 構文解析
  - [x] SELECT文
  - [x] INSERT文
  - [x] CREATE TABLE文
  - [x] DELETE文
  - [x] UPDATE文
  - [x] WHERE句
  - [x] JOIN句
  - [x] GROUP BY句
- [x] repl.go - 対話シェル（Read-Eval-Print Loop）
- [x] session.go - セッション管理
- [x] catalog.go - カタログ管理

**達成**: `SELECT * FROM users WHERE id = 1` が動く

---

## Phase 4: トランザクション ✅ 完了

- [x] wal.go - Write-Ahead Log
  - [x] ログレコード構造（LSN, TxnID, LogType, Before/After）
  - [x] ログ書き込み（Append, Flush）
  - [x] ログ読み込み（Read）
  - [x] チェックポイント（Checkpoint）
- [x] transaction.go - トランザクション管理
  - [x] BEGIN / COMMIT / ROLLBACK
  - [x] 実際のテーブル操作との統合
- [x] recovery.go - クラッシュリカバリ
  - [x] REDO処理（INSERT/UPDATE/DELETE）
  - [x] UNDO処理（INSERT/UPDATE/DELETE）
  - [x] 実際のテーブル操作との統合

**達成**: INSERT/UPDATE/DELETE が WAL に記録され、クラッシュリカバリが動く

---

## Phase 5: クエリ最適化 & 高度な機能 ✅ 完了

- [x] planner.go - 実行計画作成
  - [x] PlanNode インターフェース
  - [x] ScanNode, FilterNode, ProjectNode
  - [x] JoinNode, AggregateNode
  - [x] InsertNode, UpdateNode, DeleteNode
  - [x] CreateTableNode, EmptyNode
- [x] optimizer.go - クエリ最適化
  - [x] Rule インターフェース
  - [x] 再帰的な子ノード最適化
- [x] rule.go - 最適化ルール
  - [x] FilterPushDownRule - フィルタ押し下げ
  - [x] ConstantFoldingRule - 定数畳み込み
- [x] cost.go - コストモデル
  - [x] Cost インターフェース
  - [x] 行数ベースのコスト計算
- [x] cost_estimator.go - コスト推定
  - [x] 各 PlanNode のコスト推定
- [x] executor.go - クエリ実行エンジン
  - [x] Iterator パターン
  - [x] 各ノードの実行
- [x] aggregate.go - 集約関数
  - [x] COUNT, SUM, AVG, MAX, MIN

**達成**: 複数テーブルの JOIN が動く

---

## Phase 6: レプリケーション（複製）👈 次のフェーズ

- [ ] raft/raft.go - Raftコンセンサスアルゴリズム
  - [ ] リーダー選挙（Leader Election）
  - [ ] ログ複製（Log Replication）
  - [ ] ハートビート
- [ ] raft/log.go - Raftログ管理
- [ ] raft/state.go - ノード状態（Leader/Follower/Candidate）
- [ ] replication/manager.go - レプリケーション管理
  - [ ] 同期レプリケーション
  - [ ] リーダーフェイルオーバー

**ゴール**: 3ノードクラスタで1台落ちてもデータが失われない

---

## Phase 7: 分散トランザクション

- [ ] distributed/coordinator.go - トランザクションコーディネーター
- [ ] distributed/two_phase_commit.go - 2相コミット（2PC）
  - [ ] Prepare フェーズ
  - [ ] Commit フェーズ
  - [ ] Abort 処理
- [ ] distributed/timestamp.go - 分散タイムスタンプ
  - [ ] Hybrid Logical Clock（HLC）
  - [ ] MVCC（Multi-Version Concurrency Control）

**ゴール**: 複数ノードにまたがるトランザクションがACID保証される

---

## Phase 8: シャーディング（データ分割）

- [ ] sharding/range.go - レンジベースシャーディング
  - [ ] キー範囲でデータ分割
  - [ ] レンジ分割（Split）
  - [ ] レンジマージ（Merge）
- [ ] sharding/router.go - クエリルーティング
  - [ ] どのノードにデータがあるか判定
  - [ ] マルチノードクエリ
- [ ] sharding/rebalance.go - データ再配置
  - [ ] ノード追加時のリバランス
  - [ ] ホットスポット対策

**ゴール**: 1TB超のデータを複数ノードに分散して格納

---

## Phase 9: 分散クエリ実行

- [ ] distributed/planner.go - 分散クエリプランナー
  - [ ] クエリを複数ノードに分解
  - [ ] Scatter-Gather パターン
- [ ] distributed/executor.go - 分散実行エンジン
  - [ ] 並列実行
  - [ ] 結果のマージ
- [ ] distributed/join.go - 分散JOIN
  - [ ] Broadcast Join
  - [ ] Shuffle Join

**ゴール**: 複数ノードに分散したテーブル同士のJOINが動く

---

## Phase 10: 本番運用機能

- [ ] cluster/membership.go - クラスタメンバーシップ
  - [ ] ノード参加/離脱
  - [ ] ヘルスチェック
- [ ] cluster/metadata.go - メタデータ管理
  - [ ] テーブル定義の分散管理
  - [ ] スキーマ変更の伝播
- [ ] monitoring/metrics.go - メトリクス収集
  - [ ] Prometheus形式
  - [ ] QPS, レイテンシ, エラー率
- [ ] admin/api.go - 管理API
  - [ ] ノード追加/削除
  - [ ] バックアップ/リストア

**ゴール**: 本番環境で運用できる分散DB

---

## アーキテクチャ（最終形）

```
                    ┌─────────────────┐
                    │   SQL Client    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   SQL Layer     │  ← Parser, Planner, Executor
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
┌────────▼────────┐ ┌────────▼────────┐ ┌────────▼────────┐
│     Node 1      │ │     Node 2      │ │     Node 3      │
│  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │
│  │  Storage  │  │ │  │  Storage  │  │ │  │  Storage  │  │
│  │  Engine   │  │ │  │  Engine   │  │ │  │  Engine   │  │
│  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │
│  ┌───────────┐  │ │  ┌───────────┐  │ │  ┌───────────┐  │
│  │   Raft    │◄─┼─┼─►│   Raft    │◄─┼─┼─►│   Raft    │  │
│  │ Consensus │  │ │  │ Consensus │  │ │  │ Consensus │  │
│  └───────────┘  │ │  └───────────┘  │ │  └───────────┘  │
│  Range A, C     │ │  Range B, A     │ │  Range C, B     │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

---

## 現在のディレクトリ構成

```
minidb/
├── cmd/
│   └── minidb/
│       └── main.go
├── internal/
│   ├── storage/          # Phase 1 ✅
│   │   ├── page.go
│   │   ├── pager.go
│   │   ├── page_layout.go
│   │   ├── types.go
│   │   ├── schema.go
│   │   ├── row.go
│   │   └── table.go
│   ├── index/            # Phase 2 ✅
│   │   ├── btree.go
│   │   └── index.go
│   ├── parser/           # Phase 3 ✅
│   │   ├── token.go
│   │   ├── lexer.go
│   │   ├── ast.go
│   │   └── parser.go
│   ├── executor/         # Phase 5 ✅
│   │   ├── executor.go
│   │   ├── iterator.go
│   │   ├── result.go
│   │   └── aggregate.go
│   ├── planner/          # Phase 5 ✅
│   │   ├── planner.go
│   │   ├── plan_node.go
│   │   ├── optimizer.go
│   │   ├── rule.go
│   │   ├── cost.go
│   │   └── cost_estimator.go
│   ├── dbtxn/            # Phase 4 ⚠
│   │   ├── wal.go
│   │   ├── transaction.go
│   │   └── recovery.go
│   ├── catalog/          # Phase 3 ✅
│   │   └── catalog.go
│   ├── session/          # Phase 3 ✅
│   │   └── session.go
│   ├── raft/             # Phase 6 (未実装)
│   ├── sharding/         # Phase 8 (未実装)
│   ├── distributed/      # Phase 7, 9 (未実装)
│   ├── cluster/          # Phase 10 (未実装)
│   ├── monitoring/       # Phase 10 (未実装)
│   └── admin/            # Phase 10 (未実装)
├── pkg/
│   └── repl/             # Phase 3 ✅
│       └── repl.go
├── docs/
│   ├── plans/
│   │   └── database-impliment-plan.md
│   ├── reports/
│   │   └── phase5-query-engine.md
│   └── agent/
├── go.mod
└── README.md
```

---

## 参考資料

### 基礎（Phase 1-5）

- [db_tutorial](https://cstack.github.io/db_tutorial/) - SQLiteクローンのチュートリアル
- [bbolt](https://github.com/etcd-io/bbolt) - GoのB+Tree KVS
- [CMU Database Course](https://15445.courses.cs.cmu.edu/) - DB講義

### 分散システム（Phase 6-10）

- [Raft論文](https://raft.github.io/raft.pdf) - コンセンサスアルゴリズム
- [etcd/raft](https://github.com/etcd-io/raft) - GoのRaft実装
- [TiDB](https://github.com/pingcap/tidb) - MySQL互換の分散DB
- [CockroachDB](https://github.com/cockroachdb/cockroach) - PostgreSQL互換の分散DB
- [Spanner論文](https://research.google/pubs/pub39966/) - Googleの分散DB
- [Percolator論文](https://research.google/pubs/pub36726/) - 分散トランザクション

---

## マイルストーン

| Phase | 達成すると... | 状態 |
|-------|-------------|------|
| 1-3   | 自作DBでSQLが動く！ | ✅ 達成 |
| 4-5   | 本格的なRDBMSになる！ | ⚠ ほぼ達成 |
| 6-7   | 落ちても復旧できる分散DB！ | 🔜 次 |
| 8-10  | TiDB/CockroachDB級の分散DB！ | 📋 予定 |

---

## 次のアクション

### オプション A: Phase 6 に進む（レプリケーション）

分散システムの基盤となる Raft コンセンサスアルゴリズムを実装する。

### オプション B: Phase 4 を完成させる（トランザクション統合）

WAL とテーブル操作を統合し、ACID 保証を完成させる。

### オプション C: Phase 2 の残りを実装（B+Tree Delete）

B+Tree の Delete 操作を実装して、インデックス機能を完成させる。
