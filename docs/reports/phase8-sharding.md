# Phase 8: シャーディング（データ分割）実装レポート

## 概要

Phase 8 では、大量のデータを複数のノードに分散して格納するための**シャーディング**機能を実装しました。

---

## 実装したコンポーネント

| コンポーネント | ファイル | 役割 |
|--------------|---------|------|
| Range | range.go | キー範囲（シャードの単位） |
| RangeManager | range.go | Range の管理（Split/Merge） |
| Router | router.go | クエリルーティング |
| Rebalancer | rebalancer.go | データ再配置 |

---

## Step 1: Range（キー範囲）

### 概念

**Range** は、シャーディングにおけるデータ分割の最小単位です。

```
全キー空間: ["", "∞")
     │
     ▼
┌─────────────┬─────────────┬─────────────┐
│   Range 1   │   Range 2   │   Range 3   │
│ ["", "m")   │ ["m", "t")  │ ["t", "∞")  │
│   Node 1    │   Node 2    │   Node 3    │
└─────────────┴─────────────┴─────────────┘
```

### Range 構造体

```go
type Range struct {
    ID       string // 一意な識別子
    StartKey string // 開始キー（含む）
    EndKey   string // 終了キー（含まない）、空文字 = 無限大
    NodeID   string // このRangeを担当するノードID
}
```

### 主要メソッド

| メソッド | 説明 |
|---------|------|
| `Contains(key)` | キーがこの Range に含まれるか判定 |
| `FindRange(key)` | キーが属する Range を二分探索で検索 |
| `SplitRange(id, splitKey, newNodeID)` | Range を2つに分割 |
| `MergeRanges(id1, id2)` | 隣接する2つの Range を統合 |

### 発見したバグと修正

1. **デッドロック**: `findRangeIndex` 内でロックを取得していたが、呼び出し元で既にロックを取得済みだった
   - 修正: `findRangeIndex` からロックを削除

2. **配列初期化**: `make([]*Range, len)` で長さを指定していたため、`append` 時に先頭に nil が残った
   - 修正: `make([]*Range, 0, cap)` で長さ 0、容量のみ指定

---

## Step 2: Router（クエリルーティング）

### 概念

**Router** は、クエリを適切なノードに振り分けます。

```
クライアント
    │
    │ SELECT * FROM users WHERE id = 'mike'
    ▼
┌─────────────┐
│   Router    │  ← キー 'mike' はどのノードにあるか？
└─────────────┘
    │
    │ RangeManager.FindRange("mike") → Range["m","t") → Node2
    ▼
┌─────────────┐
│   Node 2    │
└─────────────┘
```

### Router 構造体

```go
type Router struct {
    rangeManager *RangeManager
}
```

### 主要メソッド

| メソッド | 説明 |
|---------|------|
| `GetNodeIDByRangeKey(key)` | 単一キーのルーティング |
| `GetRangeByRangeKey(key)` | キーに対応する Range を取得 |
| `RouteRange(startKey, endKey)` | 範囲クエリのルーティング（複数ノード対応） |
| `GetRangesByRange(startKey, endKey)` | 範囲に対応する Range 一覧を取得 |

### 範囲クエリの例

```
Ranges: ["","m") → node1, ["m","t") → node2, ["t","") → node3

RouteRange("a", "f")     → [node1]              (1ノード)
RouteRange("f", "p")     → [node1, node2]       (2ノード)
RouteRange("", "")       → [node1, node2, node3] (全ノード)
```

### overlaps アルゴリズム

Range と Query の重なり判定:

```
Range: [A, B)
Query: [C, D)

重なる条件: A < D かつ C < B

例: Range ["m", "t"), Query ["a", "s")
    "m" < "s" ✓ かつ "a" < "t" ✓ → 重なる
```

---

## Step 3: Rebalancer（データ再配置）

### 概念

**Rebalancer** は、ノード追加/削除時にデータ（Range）を再配置し、負荷を均等化します。

```
Before: ノード追加前
┌─────────────┬─────────────┐
│   Node 1    │   Node 2    │
│  4 Ranges   │  1 Range    │
└─────────────┴─────────────┘

After: リバランス後
┌─────────────┬─────────────┐
│   Node 1    │   Node 2    │
│  3 Ranges   │  2 Ranges   │
└─────────────┴─────────────┘
```

### Rebalancer 構造体

```go
type Rebalancer struct {
    rangeManager *RangeManager
}

type RebalanceSuggestion struct {
    RangeID      string
    SourceNodeID string
    TargetNodeID string
}
```

### 主要メソッド

| メソッド | 説明 |
|---------|------|
| `GetNodeRangeCounts()` | 各ノードの Range 数を取得 |
| `GetRangesByNode()` | ノードごとの Range 一覧を取得 |
| `SuggestRebalance()` | リバランス提案を生成 |
| `ExecuteRebalance(suggestion)` | リバランスを実行 |

### リバランスアルゴリズム

```
1. 各ノードの Range 数をカウント
2. 最大と最小の差が 1 以下になるまでループ
3. 最も多いノードから最も少ないノードへ移動を提案
4. 提案リストを返す（実際の移動は ExecuteRebalance で実行）
```

### 発見したバグと修正

**無限ループ**: 移動先が見つからない場合にループが終了しなかった
- 修正: `maxCount - minCount <= 1` で終了条件を明確化
- 修正: ノードIDをソートして順序を安定化

---

## Step 4: 統合テスト

### テストシナリオ

| シナリオ | 内容 |
|---------|------|
| 1 | 基本的なシャーディング動作 |
| 2 | 範囲クエリのルーティング |
| 3 | ノード追加とリバランス |
| 4 | Range Split とルーティング整合性 |
| 5 | Range Merge とルーティング整合性 |
| 6 | エンドツーエンド - データ分散シミュレーション |
| 7 | 大規模シミュレーション（26 Range, 5 Node） |

### エンドツーエンドテストの流れ

```
Step 1: 初期状態
  Ranges: 1, Nodes: 1

Step 2: データ増加（Range 分割）
  Split at "d": 2 Ranges
  Split at "h": 3 Ranges
  Split at "l": 4 Ranges
  → node2 を追加
  Split at "p": 5 Ranges
  Split at "t": 6 Ranges
  → node3 を追加
  Split at "x": 7 Ranges

Step 3: リバランス前
  node1: 5 Ranges
  node2: 1 Ranges
  node3: 1 Ranges

Step 4: リバランス実行
  提案数: 2
  node1 → node2
  node1 → node3

Step 5: リバランス後
  node1: 3 Ranges
  node2: 2 Ranges
  node3: 2 Ranges

Step 6: ルーティング確認
  "a" → node1
  "e" → node1
  "i" → node1
  "m" → node2
  "q" → node3
  "u" → node3
  "z" → node2
```

---

## テスト結果

```
=== 全テスト結果 ===
PASS: 49 テスト（サブテスト含む）
ok      .../internal/sharding    0.472s
```

| カテゴリ | テスト数 |
|---------|---------|
| Range | 18 |
| Router | 13 |
| Rebalancer | 11 |
| 統合テスト | 7 |

---

## ファイル構成

```
internal/sharding/
├── range.go           # Range, RangeManager
├── range_test.go      # Range のテスト
├── router.go          # Router（ルーティング）
├── router_test.go     # Router のテスト
├── rebalancer.go      # Rebalancer（再配置）
├── rebalancer_test.go # Rebalancer のテスト
└── integration_test.go # 統合テスト
```

---

## 用語集

| 用語 | 英語 | 説明 |
|------|------|------|
| シャーディング | Sharding | データを複数ノードに分散する手法 |
| Range | Range | キー範囲、シャードの単位 |
| 半開区間 | Half-open interval | `[start, end)` - start を含み、end を含まない |
| Split | Split | Range を2つに分割 |
| Merge | Merge | 2つの Range を1つに統合 |
| ルーティング | Routing | クエリを適切なノードに振り分け |
| リバランス | Rebalance | データを再配置して負荷を均等化 |
| Scatter-Gather | Scatter-Gather | 複数ノードに並列クエリを送信し、結果をマージ |

---

## Phase 7 との関係

| Phase 7 コンポーネント | Phase 8 での使用場面 |
|----------------------|---------------------|
| HLC | シャード間でのタイムスタンプ統一 |
| MVCC | 各シャード内のバージョン管理 |
| 2PC | 複数シャードにまたがる書き込み |

---

## 今後の課題

### 未実装機能

1. **実データの移動**: 現在はメタデータ（NodeID）のみ更新。実データ移動には Transport 層が必要
2. **ホットスポット検出**: アクセス頻度に基づく自動 Split
3. **自動リバランス**: 定期的なバランスチェックと自動調整

### 次のフェーズ

- **Phase 9: 分散クエリ実行** - 複数シャードにまたがるクエリの並列実行
- **Phase 10: 本番運用機能** - クラスタメンバーシップ、メトリクス収集

---

## 参考資料

- [TiDB PD (Placement Driver)](https://docs.pingcap.com/tidb/stable/pd-configuration-file) - TiDB の Range 管理
- [CockroachDB Range](https://www.cockroachlabs.com/docs/stable/architecture/overview.html) - CockroachDB のシャーディング
- [Google Spanner](https://cloud.google.com/spanner/docs/whitepapers/life-of-reads-and-writes) - Spanner のデータ分散
