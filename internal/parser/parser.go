package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type parser struct {
	lexer        *lexer
	currentToken *token // 現在のトークン
	peekToken    *token // 次のトークン
	errors       []string
}

func NewParser(lexer *lexer) *parser {
	p := &parser{lexer: lexer}
	// 最初の2つのトークンを読み込む
	p.nextToken()
	p.nextToken()
	return p
}

// nextToken は次のトークンを読み込む
func (p *parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.lexer.nextToken()
}

// currentTokenIs は現在のトークンが指定されたトークンかどうかを返す
func (p *parser) currentTokenIs(t TokenType) bool {
	return p.currentToken.tokenType == t
}

// peekTokenIs は次のトークンが指定されたトークンかどうかを返す
func (p *parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.tokenType == t
}

// errors はエラーを追加する
func (p *parser) addError(msg string) {
	p.errors = append(p.errors, msg)
}

func (p *parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %d, got %d instead", t, p.peekToken.tokenType)
	p.addError(msg)
}

func (p *parser) Parse() (Statement, error) {
	switch p.currentToken.tokenType {
	case TOKEN_SELECT:
		return p.parseSelectStatement()
	case TOKEN_INSERT:
		return p.parseInsertStatement()
	case TOKEN_UPDATE:
		return p.parseUpdateStatement()
	case TOKEN_DELETE:
		return p.parseDeleteStatement()
	case TOKEN_CREATE:
		return p.parseCreateTableStatement()
	case TOKEN_EXPLAIN:
		return p.parseExplainStatement()
	default:
		return nil, fmt.Errorf("unexpected token: %d", p.currentToken.tokenType)
	}
}

func (p *parser) parseSelectStatement() (*selectStatement, error) {
	stmt := &selectStatement{}
	// SELECT の次へ進む
	p.nextToken()
	// カラムリストをパース
	columns, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.columns = columns
	// FROM を期待
	if !p.expectPeek(TOKEN_FROM) {
		return nil, fmt.Errorf("expected FROM token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.from = p.currentToken.literal
	// Where句をパース
	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken() // WHERE へ
		p.nextToken() // 条件式へ
		stmt.where, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}
	// ORDER BY（オプション）
	if p.peekTokenIs(TOKEN_ORDER) {
		p.nextToken() // ORDER へ
		if !p.expectPeek(TOKEN_BY) {
			return nil, fmt.Errorf("expected BY after ORDER")
		}
		stmt.orderBy, err = p.parseOrderBy()
		if err != nil {
			return nil, err
		}
	}

	// LIMIT（オプション）
	if p.peekTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		p.nextToken()
		limit, _ := strconv.Atoi(p.currentToken.literal)
		stmt.limit = &limit
	}
	return stmt, nil
}

func (p *parser) parseSelectColumns() ([]Expression, error) {
	columns := []Expression{}
	// カラムリストをパース
	// * の場合は全カラムを選択
	if p.currentTokenIs(TOKEN_ASTERISK) {
		return []Expression{&asterisk{}}, nil
	}
	// カラム名のリスト
	for {
		if !p.currentTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name")
		}
		columns = append(columns, &identifier{value: p.currentToken.literal})
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
		p.nextToken() // 次のカラム名へ
	}
	return columns, nil
}

func (p *parser) parseExpression() (Expression, error) {
	// まず比較式を解析
	left, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}
	// AND/OR があれば論理式を作成
	for p.peekTokenIs(TOKEN_AND) || p.peekTokenIs(TOKEN_OR) {
		p.nextToken() // 演算子へ
		operator := p.currentToken.literal
		p.nextToken() // 右辺へ
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		left = &binaryExpression{left: left, operator: operator, right: right}
	}
	return left, nil
}

func (p *parser) parseComparisonExpression() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}
	// 比較演算子があれば binaryExpression を作成
	if p.peekTokenIs(TOKEN_EQ) || p.peekTokenIs(TOKEN_NEQ) ||
		p.peekTokenIs(TOKEN_LT) || p.peekTokenIs(TOKEN_GT) ||
		p.peekTokenIs(TOKEN_LTE) || p.peekTokenIs(TOKEN_GTE) {

		p.nextToken() // 演算子へ
		operator := p.currentToken.literal
		p.nextToken() // 右辺へ
		right, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		left = &binaryExpression{left: left, operator: operator, right: right}
	}
	return left, nil
}

func (p *parser) parsePrimaryExpression() (Expression, error) {
	switch p.currentToken.tokenType {
	case TOKEN_IDENT:
		return &identifier{value: p.currentToken.literal}, nil
	case TOKEN_INT:
		val, _ := strconv.ParseInt(p.currentToken.literal, 10, 64)
		return &integerLiteral{value: int(val)}, nil
	case TOKEN_VARCHAR:
		return &stringLiteral{value: p.currentToken.literal}, nil
	case TOKEN_TEXT:
		return &stringLiteral{value: p.currentToken.literal}, nil
	case TOKEN_BOOL:
		return &booleanLiteral{value: p.currentToken.literal == "true"}, nil
	default:
		return nil, fmt.Errorf("unexpected token: %d", p.currentToken.tokenType)
	}
}

func (p *parser) parseInsertStatement() (*insertStatement, error) {
	stmt := &insertStatement{}
	// INTO を期待
	if !p.expectPeek(TOKEN_INTO) {
		return nil, fmt.Errorf("expected INTO token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.tableName = p.currentToken.literal
	// ()を期待
	if !p.expectPeek(TOKEN_LPAREN) {
		return nil, fmt.Errorf("expected ( after table name")
	}
	// カラムリストをパース
	stmt.columns = p.parseIdentifierList()
	// )を期待
	if !p.expectPeek(TOKEN_RPAREN) {
		return nil, fmt.Errorf("expected ) after columns")
	}
	// VALUESを期待
	if !p.expectPeek(TOKEN_VALUES) {
		return nil, fmt.Errorf("expected VALUES token")
	}
	// ()を期待
	if !p.expectPeek(TOKEN_LPAREN) {
		return nil, fmt.Errorf("expected ( after VALUES")
	}
	// 値のリストをパース
	stmt.values = p.parseExpressionList()
	if !p.expectPeek(TOKEN_RPAREN) {
		return nil, fmt.Errorf("expected ) after values")
	}
	return stmt, nil
}

// カラム名('id', 'name', 'age' など)のリストをパース
func (p *parser) parseIdentifierList() []string {
	list := []string{}
	for {
		p.nextToken() // カラム名へ
		if !p.currentTokenIs(TOKEN_IDENT) {
			break
		}
		list = append(list, p.currentToken.literal)
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	return list
}

// 値のリストをパース(123, 'hello', true など)
func (p *parser) parseExpressionList() []Expression {
	list := []Expression{}
	for {
		p.nextToken() // 値へ
		expr, err := p.parsePrimaryExpression()
		if err != nil {
			break
		}
		list = append(list, expr)
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	return list
}

func (p *parser) parseOrderBy() ([]orderByClause, error) {
	clauses := []orderByClause{}
	for {
		p.nextToken() // カラム名へ
		if !p.currentTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name")
		}
		clause := orderByClause{column: p.currentToken.literal, asc: true}
		if p.peekTokenIs(TOKEN_DESC) {
			p.nextToken() // DESC へ
			clause.asc = false
		} else if p.peekTokenIs(TOKEN_ASC) {
			p.nextToken() // ASC へ
			clause.asc = true
		}
		clauses = append(clauses, clause)
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	return clauses, nil
}

func (p *parser) parseUpdateStatement() (*updateStatement, error) {
	stmt := &updateStatement{
		setExpressions: make(map[string]Expression),
	}
	// テーブル名を期待
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.tableName = p.currentToken.literal
	// SET を期待
	if !p.expectPeek(TOKEN_SET) {
		return nil, fmt.Errorf("expected SET token")
	}

	// SET句をパース
	for {
		p.nextToken() // カラム名へ
		if !p.currentTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name")
		}
		columnName := p.currentToken.literal

		// =を期待
		if !p.expectPeek(TOKEN_EQ) {
			return nil, fmt.Errorf("expected = after column name")
		}
		p.nextToken() // 値へ
		// 値をパース
		value, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		stmt.setExpressions[columnName] = value
		// 次が , でなければ終了
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	// WHERE句をパース
	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken() // WHERE へ
		p.nextToken() // 条件式へ
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.where = whereExpr
	}
	return stmt, nil
}

// DELETE文をパース
func (p *parser) parseDeleteStatement() (*deleteStatement, error) {
	stmt := &deleteStatement{}
	// FROM を期待
	if !p.expectPeek(TOKEN_FROM) {
		return nil, fmt.Errorf("expected FROM token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.tableName = p.currentToken.literal
	// WHERE句をパース
	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken() // WHERE へ
		p.nextToken() // 条件式へ
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.where = whereExpr
	}
	return stmt, nil
}

// CREATE TABLE 文をパース
func (p *parser) parseCreateTableStatement() (*createTableStatement, error) {
	stmt := &createTableStatement{}
	// TABLE を期待
	if !p.expectPeek(TOKEN_TABLE) {
		return nil, fmt.Errorf("expected TABLE token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.tableName = p.currentToken.literal
	// ()を期待
	if !p.expectPeek(TOKEN_LPAREN) {
		return nil, fmt.Errorf("expected ( after TABLE")
	}
	// カラム定義をパース
	for {
		p.nextToken() // カラム定義へ
		colDef, err := p.parseColumnDefinition()
		if err != nil {
			return nil, err
		}
		stmt.columns = append(stmt.columns, *colDef)
		// 次が , でなければ終了
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	// )を期待
	if !p.expectPeek(TOKEN_RPAREN) {
		return nil, fmt.Errorf("expected ) after columns")
	}
	return stmt, nil
}

// カラム定義をパース
func (p *parser) parseColumnDefinition() (*columnDefinition, error) {
	colDef := &columnDefinition{}
	// カラム名をパース（呼び出し元で既にnextToken()済み）
	if !p.currentTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected column name")
	}
	colDef.name = p.currentToken.literal
	p.nextToken() // データ型へ

	// データ型（INT, VARCHAR等は識別子として認識される）
	if !p.currentTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected data type, got token: %d", p.currentToken.tokenType)
	}

	switch strings.ToUpper(p.currentToken.literal) {
	case "INT", "INTEGER":
		colDef.columnType = "INT"
	case "FLOAT", "DOUBLE":
		colDef.columnType = "FLOAT"
	case "VARCHAR":
		colDef.columnType = "VARCHAR"
		// VARCHAR(255) のような形式をパース
		if p.peekTokenIs(TOKEN_LPAREN) {
			p.nextToken() // ( へ
			p.nextToken() // サイズへ
			colDef.columnType = fmt.Sprintf("VARCHAR(%s)", p.currentToken.literal)
			p.nextToken() // ) へ
		}
	case "BOOL", "BOOLEAN":
		colDef.columnType = "BOOL"
	case "TEXT":
		colDef.columnType = "TEXT"
	default:
		return nil, fmt.Errorf("unknown data type: %s", p.currentToken.literal)
	}
	// PRIMARY KEY かどうか
	if p.peekTokenIs(TOKEN_PRIMARY) {
		p.nextToken() // PRIMARY へ
		if !p.expectPeek(TOKEN_KEY) {
			return nil, fmt.Errorf("expected KEY after PRIMARY")
		}
		colDef.primaryKey = true
	}
	// NULL かどうか
	if p.peekTokenIs(TOKEN_NULL) {
		p.nextToken() // NULL へ
		colDef.nullable = true
	}
	return colDef, nil
}

// EXPLAIN文をパース
func (p *parser) parseExplainStatement() (*explainStatement, error) {
	stmt := &explainStatement{}
	// EXPLAIN の次へ進む
	p.nextToken()
	// 文をパース
	innerStmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	stmt.statement = innerStmt
	return stmt, nil
}

func (p *parser) Errors() []string {
	return p.errors
}
