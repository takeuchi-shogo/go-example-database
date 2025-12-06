# Claude.md

## 1. プロジェクトの説明

このプロジェクトは、Golangで実装されたデータベースです。

### フェーズの説明

現在のフェーズは、データベースの基本機能の実装を行っています。

**Goal**:

- TiDB や CockroachDB などのオープンソースのデータベースを参考に、分散型の本格的なデータベースを実装できるようになること
  - [TiDB](https://github.com/pingcap/tidb)
  - [CockroachDB](https://github.com/cockroachdb/cockroach)
- Golang の基本的な文法を理解し、応用できるように実際にコードを書いてみること

## プロジェクトの技術スタック

- Golang

## コーディング規則

- コードは Golang のコーディング規約に従う
- 関数は小さく、シンプルで、読みやすいものにする
- ドキュメントコメントは簡潔に、でも詳細にわかりやすいように書く
- インラインコメントは適宜使用する
- 命名については、わかりやすく、一貫性のあるものにする。c や f などの短縮語は使用しない
- プライベート、パブリックな関数や変数、フィールドの判断を迷う場合は基本的にプライベートで実装する。明らかにパブリックなものはパブリックで実装して良い。

## 呼び出すファイルの位置

- ファイル構成などアーキテクチャについては、[docs/agent/service-architecture.md](docs/agent/service-architecture.md) を参照してください。
- テストについては、[docs/agent/running-tests.md](docs/agent/running-tests.md) を参照してください。

## 実装フェーズについて

現在の実装フェーズは、[docs/plans/database-impliment-plan.md](docs/plans/database-impliment-plan.md) を参照してください。
