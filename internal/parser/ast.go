package parser

// Node は抽象構文木のノードを表す
type Node any

// Statement は文を表す
type Statement interface {
	Node
}

// Expression は式を表す
type Expression interface {
	Node
}

// SelectStatement はSELECT文を表す
type SelectStatement struct {
	Columns []Expression   // 選択するカラム
	From    string         // テーブル名
	Where   Expression     // 条件
	OrderBy []OrderByClause // ソート条件
	Limit   *int           // 最大行数
	Offset  *int           // オフセット
}

// OrderByClause はソート条件を表す
type OrderByClause struct {
	Column string // ソートするカラム
	Asc    bool   // 昇順か降順か
}

// InsertStatement はINSERT文を表す
type InsertStatement struct {
	TableName string       // テーブル名
	Columns   []string     // 挿入するカラム
	Values    []Expression // 挿入する値
}

// UpdateStatement はUPDATE文を表す
type UpdateStatement struct {
	TableName      string                // テーブル名
	SetExpressions map[string]Expression // 更新するカラムと値
	Where          Expression            // 条件
}

// DeleteStatement はDELETE文を表す
type DeleteStatement struct {
	TableName string     // テーブル名
	Where     Expression // 条件
}

// CreateTableStatement はCREATE TABLE文を表す
type CreateTableStatement struct {
	TableName string             // テーブル名
	Columns   []ColumnDefinition // カラム定義
}

// ColumnDefinition はカラム定義を表す
type ColumnDefinition struct {
	Name       string // カラム名
	ColumnType string // カラム型
	PrimaryKey bool   // 主キーかどうか
	Nullable   bool   // NULLかどうか
}

// ExplainStatement はEXPLAIN文を表す
type ExplainStatement struct {
	Statement Statement // 説明する文
}

// Identifier はカラム名やテーブル名
type Identifier struct {
	Value string // 値
}

// StringLiteral は文字列リテラルを表す
type StringLiteral struct {
	Value string // 値
}

// IntegerLiteral は整数リテラルを表す
type IntegerLiteral struct {
	Value int // 値
}

// BooleanLiteral は真偽リテラルを表す
type BooleanLiteral struct {
	Value bool // 値
}

// BinaryExpression は二項演算子を表す
type BinaryExpression struct {
	Left     Expression // 左辺
	Operator string     // 演算子
	Right    Expression // 右辺
}

// Asterisk は*を表す
type Asterisk struct {
}
