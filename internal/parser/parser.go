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

// NewParser は新しい Parser を作成する
// Lexer を受け取り、最初の2つのトークンを読み込む
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

// expectPeek は次のトークンが指定されたトークンかどうかを期待する
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

func (p *parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}
	// SELECT の次へ進む
	p.nextToken()
	// カラムリストをパース
	columns, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns
	// FROM を期待
	if !p.expectPeek(TOKEN_FROM) {
		return nil, fmt.Errorf("expected FROM token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.From = p.currentToken.literal
	// JOIN を期待
	if p.peekTokenIs(TOKEN_JOIN) {
		p.nextToken() // JOIN へ
		p.nextToken() // テーブル名へ
		joinTable := p.currentToken.literal
		// ON を期待
		if !p.expectPeek(TOKEN_ON) {
			return nil, fmt.Errorf("expected ON token")
		}
		p.nextToken() // ON へ
		// 条件式をパース
		joinOn, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Join = &Join{Table: joinTable, On: joinOn}
	}
	// Where句をパース
	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken() // WHERE へ
		p.nextToken() // 条件式へ
		stmt.Where, err = p.parseExpression()
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
		stmt.OrderBy, err = p.parseOrderBy()
		if err != nil {
			return nil, err
		}
	}

	// LIMIT（オプション）
	if p.peekTokenIs(TOKEN_LIMIT) {
		p.nextToken()
		p.nextToken()
		limit, _ := strconv.Atoi(p.currentToken.literal)
		stmt.Limit = &limit
	}
	return stmt, nil
}

func (p *parser) parseSelectColumns() ([]Expression, error) {
	columns := []Expression{}
	// カラムリストをパース
	// * の場合は全カラムを選択
	if p.currentTokenIs(TOKEN_ASTERISK) {
		return []Expression{&Asterisk{}}, nil
	}
	// カラム名のリスト
	for {
		if !p.currentTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name")
		}
		columns = append(columns, &Identifier{Value: p.currentToken.literal})
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
		left = &BinaryExpression{Left: left, Operator: operator, Right: right}
	}
	return left, nil
}

func (p *parser) parseComparisonExpression() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}
	// 比較演算子があれば BinaryExpression を作成
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
		left = &BinaryExpression{Left: left, Operator: operator, Right: right}
	}
	return left, nil
}

func (p *parser) parsePrimaryExpression() (Expression, error) {
	switch p.currentToken.tokenType {
	case TOKEN_IDENT:
		ident := p.currentToken.literal
		// table.column 形式かチェック
		if p.peekTokenIs(TOKEN_DOT) {
			p.nextToken() // . へ
			p.nextToken() // カラム名へ
			if !p.currentTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name after dot")
			}
			return &QualifiedIdentifier{TableName: ident, ColumnName: p.currentToken.literal}, nil
		}
		return &Identifier{Value: ident}, nil
	case TOKEN_INT:
		val, _ := strconv.ParseInt(p.currentToken.literal, 10, 64)
		return &IntegerLiteral{Value: int(val)}, nil
	case TOKEN_VARCHAR:
		return &StringLiteral{Value: p.currentToken.literal}, nil
	case TOKEN_TEXT:
		return &StringLiteral{Value: p.currentToken.literal}, nil
	case TOKEN_BOOL:
		return &BooleanLiteral{Value: p.currentToken.literal == "true"}, nil
	default:
		return nil, fmt.Errorf("unexpected token: %d", p.currentToken.tokenType)
	}
}

func (p *parser) parseInsertStatement() (*InsertStatement, error) {
	stmt := &InsertStatement{}
	// INTO を期待
	if !p.expectPeek(TOKEN_INTO) {
		return nil, fmt.Errorf("expected INTO token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.currentToken.literal
	// ()を期待
	if !p.expectPeek(TOKEN_LPAREN) {
		return nil, fmt.Errorf("expected ( after table name")
	}
	// カラムリストをパース
	stmt.Columns = p.parseIdentifierList()
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
	stmt.Values = p.parseExpressionList()
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

func (p *parser) parseOrderBy() ([]OrderByClause, error) {
	clauses := []OrderByClause{}
	for {
		p.nextToken() // カラム名へ
		if !p.currentTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name")
		}
		clause := OrderByClause{Column: p.currentToken.literal, Asc: true}
		if p.peekTokenIs(TOKEN_DESC) {
			p.nextToken() // DESC へ
			clause.Asc = false
		} else if p.peekTokenIs(TOKEN_ASC) {
			p.nextToken() // ASC へ
			clause.Asc = true
		}
		clauses = append(clauses, clause)
		if !p.peekTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // COMMA へ
	}
	return clauses, nil
}

func (p *parser) parseUpdateStatement() (*UpdateStatement, error) {
	stmt := &UpdateStatement{
		SetExpressions: make(map[string]Expression),
	}
	// テーブル名を期待
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.currentToken.literal
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
		stmt.SetExpressions[columnName] = value
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
		stmt.Where = whereExpr
	}
	return stmt, nil
}

// DELETE文をパース
func (p *parser) parseDeleteStatement() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}
	// FROM を期待
	if !p.expectPeek(TOKEN_FROM) {
		return nil, fmt.Errorf("expected FROM token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.currentToken.literal
	// WHERE句をパース
	if p.peekTokenIs(TOKEN_WHERE) {
		p.nextToken() // WHERE へ
		p.nextToken() // 条件式へ
		whereExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = whereExpr
	}
	return stmt, nil
}

// CREATE TABLE 文をパース
func (p *parser) parseCreateTableStatement() (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{}
	// TABLE を期待
	if !p.expectPeek(TOKEN_TABLE) {
		return nil, fmt.Errorf("expected TABLE token")
	}
	// テーブル名をパース
	if !p.expectPeek(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.currentToken.literal
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
		stmt.Columns = append(stmt.Columns, *colDef)
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
func (p *parser) parseColumnDefinition() (*ColumnDefinition, error) {
	colDef := &ColumnDefinition{}
	// カラム名をパース（呼び出し元で既にnextToken()済み）
	if !p.currentTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected column name")
	}
	colDef.Name = p.currentToken.literal
	p.nextToken() // データ型へ

	// データ型（INT, VARCHAR等は識別子として認識される）
	if !p.currentTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected data type, got token: %d", p.currentToken.tokenType)
	}

	switch strings.ToUpper(p.currentToken.literal) {
	case "INT", "INTEGER":
		colDef.ColumnType = "INT"
	case "FLOAT", "DOUBLE":
		colDef.ColumnType = "FLOAT"
	case "VARCHAR":
		colDef.ColumnType = "VARCHAR"
		// VARCHAR(255) のような形式をパース
		if p.peekTokenIs(TOKEN_LPAREN) {
			p.nextToken() // ( へ
			p.nextToken() // サイズへ
			colDef.ColumnType = fmt.Sprintf("VARCHAR(%s)", p.currentToken.literal)
			p.nextToken() // ) へ
		}
	case "BOOL", "BOOLEAN":
		colDef.ColumnType = "BOOL"
	case "TEXT":
		colDef.ColumnType = "TEXT"
	default:
		return nil, fmt.Errorf("unknown data type: %s", p.currentToken.literal)
	}
	// PRIMARY KEY かどうか
	if p.peekTokenIs(TOKEN_PRIMARY) {
		p.nextToken() // PRIMARY へ
		if !p.expectPeek(TOKEN_KEY) {
			return nil, fmt.Errorf("expected KEY after PRIMARY")
		}
		colDef.PrimaryKey = true
	}
	// NULL かどうか
	if p.peekTokenIs(TOKEN_NULL) {
		p.nextToken() // NULL へ
		colDef.Nullable = true
	}
	return colDef, nil
}

// EXPLAIN文をパース
func (p *parser) parseExplainStatement() (*ExplainStatement, error) {
	stmt := &ExplainStatement{}
	// EXPLAIN の次へ進む
	p.nextToken()
	// 文をパース
	innerStmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	stmt.Statement = innerStmt
	return stmt, nil
}

func (p *parser) Errors() []string {
	return p.errors
}
