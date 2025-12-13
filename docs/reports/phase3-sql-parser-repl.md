# Phase 3: SQL パーサー & REPL 実装レポート

## 概要

Phase 3 では SQL パーサーと REPL（対話型シェル）を実装しました。

## 実装ファイル

### パーサー (`internal/parser/`)

| ファイル | 役割 |
|----------|------|
| `token.go` | トークン型定義とキーワードマップ |
| `lexer.go` | 字句解析器（SQL文字列→トークン列） |
| `ast.go` | 抽象構文木のノード定義 |
| `parser.go` | 構文解析器（トークン列→AST） |
| `token_test.go` | トークンのテスト |
| `lexer_test.go` | レキサーのテスト |
| `parser_test.go` | パーサーのテスト |

### REPL (`pkg/repl/`, `cmd/godb/`)

| ファイル | 役割 |
|----------|------|
| `pkg/repl/repl.go` | 対話型シェル本体 |
| `cmd/godb/main.go` | CLI エントリポイント |

## Q&A・議論まとめ

### Q1: token.go の実装方針は？

**回答:**

- `TokenType` を `int` の enum として定義
- 特殊トークン（ILLEGAL, EOF）
- キーワード（SELECT, INSERT, UPDATE, DELETE, FROM, WHERE など）
- 演算子（=, <, >, <=, >=, !=）
- セパレータ（カンマ, セミコロン, 括弧, アスタリスク）
- `keywords` マップで文字列→TokenType の変換
- `LookupIdent()` で識別子がキーワードかどうか判定（大文字小文字を無視）

### Q2: EXPLAIN も追加したい

**回答:**

- `TOKEN_EXPLAIN` を追加
- keywords マップに `"EXPLAIN": TOKEN_EXPLAIN` を追加
- `explainStatement` AST ノードを定義

### Q3: token は何をしているのか？

**回答:**
Token はよく入力されるコマンド（SQL キーワード）を変換して読み込めるようにしています。
字句解析の最小単位で、SQL 文を意味のある単位に分割するために使用します。

例: `SELECT * FROM users` → `[SELECT] [*] [FROM] [users]`

### Q4: lexer.go の実装方針は？

**回答:**

- `lexer` 構造体: input, position, readPosition, ch
- `NewLexer()`: 初期化
- `nextToken()`: 次のトークンを返す
- `readChar()`: 1文字読み進める
- `peekChar()`: 次の文字を覗き見
- `readIdentifier()`: 識別子を読む
- `readNumber()`: 数値を読む
- `readString()`: 文字列リテラルを読む
- `skipWhitespace()`: 空白をスキップ

### Q5: ast.go の実装方針は？

**回答:**

- `Node` インターフェース: 全ノードの基底
- `Statement` インターフェース: 文（SELECT, INSERT など）
- `Expression` インターフェース: 式（識別子, リテラル, 二項演算）

Statement 型:

- `selectStatement`: columns, from, where, orderBy, limit
- `insertStatement`: tableName, columns, values
- `updateStatement`: tableName, setExpressions, where
- `deleteStatement`: tableName, where
- `createTableStatement`: tableName, columns
- `explainStatement`: statement

Expression 型:

- `identifier`: カラム名など
- `stringLiteral`: 文字列リテラル
- `integerLiteral`: 整数リテラル
- `binaryExpression`: 二項演算（left, operator, right）
- `asterisk`: *

### Q6: parser.go の実装方針は？

**回答:**

- `parser` 構造体: lexer, currentToken, peekToken, errors
- `NewParser()`: 初期化
- `Parse()`: メインのパース処理（文の種類に応じて分岐）
- `parseSelectStatement()`: SELECT 文をパース
- `parseInsertStatement()`: INSERT 文をパース
- `parseUpdateStatement()`: UPDATE 文をパース
- `parseDeleteStatement()`: DELETE 文をパース
- `parseCreateTableStatement()`: CREATE TABLE 文をパース
- `parseExplainStatement()`: EXPLAIN 文をパース
- `parseExpression()`: 式をパース（AND/OR の論理式）
- `parseComparisonExpression()`: 比較式をパース（=, <, > など）
- `parsePrimaryExpression()`: 基本式をパース（識別子, リテラル）

ヘルパー関数:

- `nextToken()`: 次のトークンへ進む
- `currentTokenIs()`: 現在のトークンが指定の型か
- `peekTokenIs()`: 次のトークンが指定の型か
- `expectPeek()`: 次のトークンを期待して進む

### Q7: テストを先に実装してほしい

**回答:**

テストファイルを作成:

- `token_test.go`: `LookupIdent()` のテスト
- `lexer_test.go`: 各 SQL 文のトークン化テスト
- `parser_test.go`: 各 SQL 文のパーステスト

### Q8: repl.go の実装方針は？

**回答:**

- `Repl` 構造体: input (io.Reader), output (io.Writer)
- `NewRepl()`: 初期化
- `Run()`: メインループ
- `printWelcome()`: 起動メッセージ
- `printPrompt()`: プロンプト表示
- `executeCommand()`: コマンド実行
- `handleCommand()`: 特殊コマンド処理（.help, .exit, .quit）
- `eval()`: SQL パース・表示
- `printHelp()`: ヘルプ表示
- `printGoodBye()`: 終了メッセージ（ランダム）
- `exit()`: 終了処理

終了方法:

- `.exit` / `.quit` コマンド
- `Ctrl+C` (SIGINT)
- `Ctrl+D` (EOF)

## バグ修正

### 1. lexer.go: readString() のバグ

**問題:** 開始のシングルクォートをスキップしていなかった

**修正:**

```go
func (l *lexer) readString() string {
    l.readChar() // 開始の ' をスキップ ← 追加
    position := l.position
    for l.ch != '\'' && l.ch != 0 {
        l.readChar()
    }
    return l.input[position:l.position]
}
```

### 2. parser.go: トークン進行ロジックのバグ

**問題:** `expectPeek()` と `nextToken()` の重複呼び出しでトークンが飛ばされていた

**修正:** 各 `parseXxxStatement()` から余分な `p.nextToken()` を削除

### 3. parser.go: parseSelectColumns() のバグ

**問題:** `peekTokenIs(TOKEN_ASTERISK)` で次のトークンを見ていたが、現在のトークンを見るべきだった

**修正:** `currentTokenIs(TOKEN_ASTERISK)` に変更

### 4. parser.go: parseColumnDefinition() のバグ

**問題:** データ型（INT, VARCHAR など）がキーワードではなく識別子として認識されるため、TOKEN_INT などにマッチしなかった

**修正:** `currentTokenIs(TOKEN_IDENT)` でチェックし、`strings.ToUpper(p.currentToken.literal)` で判定

### 5. parser.go: 演算子優先順位のバグ

**問題:** `id = 1 AND name = 'Alice'` が `((id = 1) AND name) = 'Alice'` として解析されていた

**修正:** `parseExpression()` と `parseComparisonExpression()` に分離し、比較演算子より AND/OR の優先度を低くした

### 6. token.go: EXPLAIN キーワードの追加漏れ

**問題:** `TOKEN_EXPLAIN` は定義されていたが、`keywords` マップに追加されていなかった

**修正:** `"EXPLAIN": TOKEN_EXPLAIN` を追加

## PR

- [#4 feat(parser): SQL パーサーの実装](https://github.com/takeuchi-shogo/go-example-database/pull/4)
- [#5 feat(repl): REPL（対話型シェル）の実装](https://github.com/takeuchi-shogo/go-example-database/pull/5)

## 次のフェーズ

Phase 4: トランザクション

- wal.go - Write-Ahead Log
- transaction.go - BEGIN / COMMIT / ROLLBACK
- recovery.go - クラッシュリカバリ
