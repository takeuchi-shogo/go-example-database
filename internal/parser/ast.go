package parser

// Node は抽象構文木のノードを表す
type Node interface{}

// Statement は文を表す
type Statement interface {
	Node
}

// Expression は式を表す
type Expression interface {
	Node
}

// SelectStatement はSELECT文を表す
type selectStatement struct {
	columns []Expression    // 選択するカラム
	from    string          // テーブル名
	where   Expression      // 条件
	orderBy []orderByClause // ソート条件
	limit   *int            // 最大行数
	offset  *int            // オフセット
}

// orderByClause はソート条件を表す
type orderByClause struct {
	column string // ソートするカラム
	asc    bool   // 昇順か降順か
}

// insertStatement はINSERT文を表す
type insertStatement struct {
	tableName string       // テーブル名
	columns   []string     // 挿入するカラム
	values    []Expression // 挿入する値
}

// updateStatement はUPDATE文を表す
type updateStatement struct {
	tableName      string                // テーブル名
	setExpressions map[string]Expression // 更新するカラムと値
	where          Expression            // 条件
}

// deleteStatement はDELETE文を表す
type deleteStatement struct {
	tableName string     // テーブル名
	where     Expression // 条件
}

// createTableStatement はCREATE TABLE文を表す
type createTableStatement struct {
	tableName string             // テーブル名
	columns   []columnDefinition // カラム定義
}

// columnDefinition はカラム定義を表す
type columnDefinition struct {
	name       string // カラム名
	columnType string // カラム型
	primaryKey bool   // 主キーかどうか
	nullable   bool   // NULLかどうか
}

// explainStatement はEXPLAIN文を表す
type explainStatement struct {
	statement Statement // 説明する文
}

// identifier はカラム名やテーブル名
type identifier struct {
	value string // 値
}

// stringLiteral は文字列リテラルを表す
type stringLiteral struct {
	value string // 値
}

// integerLiteral は整数リテラルを表す
type integerLiteral struct {
	value int // 値
}

// booleanLiteral は真偽リテラルを表す
type booleanLiteral struct {
	value bool // 値
}

// binaryExpression は二項演算子を表す
type binaryExpression struct {
	left     Expression // 左辺
	operator string     // 演算子
	right    Expression // 右辺
}

// asterisk は*を表す
type asterisk struct {
}
