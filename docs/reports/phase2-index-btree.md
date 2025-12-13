# Phase 2: インデックス（B+Tree）実装レポート

## 概要

Phase 2 では B+Tree インデックスを実装し、効率的なデータ検索を可能にしました。

## 実装ファイル (`internal/index/`)

| ファイル | 役割 |
|----------|------|
| `btree.go` | B+Tree の実装 |
| `index.go` | インデックス管理 |

## 主要概念

### B+Tree

- バランス木構造
- リーフノードにデータを格納
- 内部ノードはルーティング用

### ノード構造

**内部ノード (Internal Node)**

- キーと子ノードへのポインタ
- ルーティング専用

**リーフノード (Leaf Node)**

- キーと実データへのポインタ
- 次のリーフへのポインタ（範囲検索用）

### 操作

| 操作 | 計算量 | 説明 |
|------|--------|------|
| Search | O(log n) | キーでデータを検索 |
| Insert | O(log n) | データを挿入 |
| Split | - | ノードが満杯時に分割 |
| Delete | O(log n) | データを削除（後回し可） |

## ゴール

`table.Find(id)` が O(log n) で動作すること

## PR

- [#2 feat(index): B+Tree インデックスの実装](https://github.com/takeuchi-shogo/go-example-database/pull/2)

## 次のフェーズ

Phase 3: SQL パーサー & REPL
