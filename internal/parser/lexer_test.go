package parser

import "testing"

func TestLexer_Select(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_ASTERISK, "*"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "id"},
		{TOKEN_EQ, "="},
		{TOKEN_INT, "1"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_SelectColumns(t *testing.T) {
	input := "SELECT id, name, age FROM users"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_SELECT, "SELECT"},
		{TOKEN_IDENT, "id"},
		{TOKEN_COMMA, ","},
		{TOKEN_IDENT, "name"},
		{TOKEN_COMMA, ","},
		{TOKEN_IDENT, "age"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_Insert(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice')"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_INSERT, "INSERT"},
		{TOKEN_INTO, "INTO"},
		{TOKEN_IDENT, "users"},
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "id"},
		{TOKEN_COMMA, ","},
		{TOKEN_IDENT, "name"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_VALUES, "VALUES"},
		{TOKEN_LPAREN, "("},
		{TOKEN_INT, "1"},
		{TOKEN_COMMA, ","},
		{TOKEN_VARCHAR, "Alice"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_Update(t *testing.T) {
	input := "UPDATE users SET name = 'Bob' WHERE id = 1"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_UPDATE, "UPDATE"},
		{TOKEN_IDENT, "users"},
		{TOKEN_SET, "SET"},
		{TOKEN_IDENT, "name"},
		{TOKEN_EQ, "="},
		{TOKEN_VARCHAR, "Bob"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "id"},
		{TOKEN_EQ, "="},
		{TOKEN_INT, "1"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_Delete(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_DELETE, "DELETE"},
		{TOKEN_FROM, "FROM"},
		{TOKEN_IDENT, "users"},
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "id"},
		{TOKEN_EQ, "="},
		{TOKEN_INT, "1"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_CreateTable(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)"

	tests := []struct {
		expectedType TokenType
	}{
		{TOKEN_CREATE},
		{TOKEN_TABLE},
		{TOKEN_IDENT},  // users
		{TOKEN_LPAREN}, // (
		{TOKEN_IDENT},  // id
		{TOKEN_IDENT},  // INT (識別子として認識される)
		{TOKEN_PRIMARY},
		{TOKEN_KEY},
		{TOKEN_COMMA},
		{TOKEN_IDENT}, // name
		{TOKEN_IDENT}, // VARCHAR (識別子として認識される)
		{TOKEN_RPAREN},
		{TOKEN_EOF},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v (literal=%q)",
				i, tt.expectedType, tok.tokenType, tok.literal)
		}
	}
}

func TestLexer_Operators(t *testing.T) {
	input := "= != < > <= >="

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_EQ, "="},
		{TOKEN_NEQ, "!="},
		{TOKEN_LT, "<"},
		{TOKEN_GT, ">"},
		{TOKEN_LTE, "<="},
		{TOKEN_GTE, ">="},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_WhitespaceHandling(t *testing.T) {
	// タブ、改行、複数スペースがあっても正しくトークン化
	input := "SELECT\t*\n  FROM   users"

	tests := []struct {
		expectedType TokenType
	}{
		{TOKEN_SELECT},
		{TOKEN_ASTERISK},
		{TOKEN_FROM},
		{TOKEN_IDENT},
		{TOKEN_EOF},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()
		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}
	}
}

func TestLexer_Numbers(t *testing.T) {
	input := "123 456 789"

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TOKEN_INT, "123"},
		{TOKEN_INT, "456"},
		{TOKEN_INT, "789"},
		{TOKEN_EOF, ""},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()

		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v",
				i, tt.expectedType, tok.tokenType)
		}

		if tok.literal != tt.expectedLiteral {
			t.Errorf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.literal)
		}
	}
}

func TestLexer_OrderByLimit(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name DESC LIMIT 10"

	tests := []struct {
		expectedType TokenType
	}{
		{TOKEN_SELECT},
		{TOKEN_ASTERISK},
		{TOKEN_FROM},
		{TOKEN_IDENT}, // users
		{TOKEN_ORDER},
		{TOKEN_BY},
		{TOKEN_IDENT}, // name
		{TOKEN_DESC},
		{TOKEN_LIMIT},
		{TOKEN_INT}, // 10
		{TOKEN_EOF},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()
		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v (literal=%q)",
				i, tt.expectedType, tok.tokenType, tok.literal)
		}
	}
}

func TestLexer_AndOr(t *testing.T) {
	input := "WHERE id = 1 AND name = 'test' OR age > 20"

	tests := []struct {
		expectedType TokenType
	}{
		{TOKEN_WHERE},
		{TOKEN_IDENT}, // id
		{TOKEN_EQ},
		{TOKEN_INT},     // 1
		{TOKEN_AND},
		{TOKEN_IDENT},   // name
		{TOKEN_EQ},
		{TOKEN_VARCHAR}, // test
		{TOKEN_OR},
		{TOKEN_IDENT}, // age
		{TOKEN_GT},
		{TOKEN_INT}, // 20
		{TOKEN_EOF},
	}

	lexer := NewLexer(input)

	for i, tt := range tests {
		tok := lexer.nextToken()
		if tok.tokenType != tt.expectedType {
			t.Errorf("tests[%d] - tokenType wrong. expected=%v, got=%v (literal=%q)",
				i, tt.expectedType, tok.tokenType, tok.literal)
		}
	}
}
