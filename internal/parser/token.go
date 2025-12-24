package parser

import "strings"

type TokenType int

const (
	// 特殊トークン
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF

	// 識別子
	TOKEN_IDENT   // id, name, age, etc.
	TOKEN_INT     // 123, 456, etc.
	TOKEN_FLOAT   // 1.23, 4.56, etc.
	TOKEN_VARCHAR // "hello", "world", etc.
	TOKEN_TEXT    // "hello", "world", etc.
	TOKEN_BOOL    // true, false, etc.

	// キーワード(DML)
	TOKEN_SELECT // SELECT
	TOKEN_INSERT // INSERT
	TOKEN_UPDATE // UPDATE
	TOKEN_DELETE // DELETE
	TOKEN_FROM   // FROM
	TOKEN_WHERE  // WHERE
	TOKEN_GROUP  // GROUP
	TOKEN_HAVING // HAVING
	TOKEN_SET    // SET
	TOKEN_VALUES // VALUES
	TOKEN_INTO   // INTO
	// キーワード(トランザクション)
	TOKEN_BEGIN    // BEGIN
	TOKEN_COMMIT   // COMMIT
	TOKEN_ROLLBACK // ROLLBACK
	// キーワード(DDL)
	TOKEN_CREATE  // CREATE
	TOKEN_DROP    // DROP
	TOKEN_ALTER   // ALTER
	TOKEN_TABLE   // TABLE
	TOKEN_EXPLAIN // EXPLAIN
	// 集約関数
	TOKEN_COUNT // COUNT
	TOKEN_SUM   // SUM
	TOKEN_AVG   // AVG
	TOKEN_MAX   // MAX
	TOKEN_MIN   // MIN
	// 修飾子・句
	TOKEN_AND     // AND
	TOKEN_OR      //OR
	TOKEN_NOT     // NOT
	TOKEN_NULL    // NULL
	TOKEN_PRIMARY // PRIMARY KEY
	TOKEN_KEY     // KEY
	TOKEN_ORDER   // ORDER
	TOKEN_BY      // BY
	TOKEN_ASC     // ASC
	TOKEN_DESC    // DESC
	TOKEN_LIMIT   // LIMIT
	TOKEN_OFFSET  // OFFSET
	TOKEN_JOIN    // JOIN
	TOKEN_ON      // ON
	// 演算子
	TOKEN_EQ  // =
	TOKEN_NEQ // != or <>
	TOKEN_LT  // <
	TOKEN_GT  // >
	TOKEN_LTE // <=
	TOKEN_GTE // >=

	// セパレータ
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_ASTERISK  // *
	TOKEN_DOT       // .
)

type token struct {
	tokenType TokenType
	literal   string
}

func newToken(tokenType TokenType, literal string) token {
	return token{tokenType: tokenType, literal: literal}
}

var keywords = map[string]TokenType{
	// DML
	"SELECT": TOKEN_SELECT,
	"INSERT": TOKEN_INSERT,
	"UPDATE": TOKEN_UPDATE,
	"DELETE": TOKEN_DELETE,
	"FROM":   TOKEN_FROM,
	"WHERE":  TOKEN_WHERE,
	"GROUP":  TOKEN_GROUP,
	"HAVING": TOKEN_HAVING,
	"SET":    TOKEN_SET,
	"VALUES": TOKEN_VALUES,
	"INTO":   TOKEN_INTO,
	// transaction
	"BEGIN":    TOKEN_BEGIN,
	"COMMIT":   TOKEN_COMMIT,
	"ROLLBACK": TOKEN_ROLLBACK,
	// DDL
	"CREATE":  TOKEN_CREATE,
	"DROP":    TOKEN_DROP,
	"ALTER":   TOKEN_ALTER,
	"TABLE":   TOKEN_TABLE,
	"EXPLAIN": TOKEN_EXPLAIN,
	// 集約関数
	"COUNT": TOKEN_COUNT,
	"SUM":   TOKEN_SUM,
	"AVG":   TOKEN_AVG,
	"MAX":   TOKEN_MAX,
	"MIN":   TOKEN_MIN,

	// 修飾子・句
	"AND":     TOKEN_AND,
	"OR":      TOKEN_OR,
	"NOT":     TOKEN_NOT,
	"NULL":    TOKEN_NULL,
	"PRIMARY": TOKEN_PRIMARY,
	"KEY":     TOKEN_KEY,
	"ORDER":   TOKEN_ORDER,
	"BY":      TOKEN_BY,
	"ASC":     TOKEN_ASC,
	"DESC":    TOKEN_DESC,
	"LIMIT":   TOKEN_LIMIT,
	"OFFSET":  TOKEN_OFFSET,
	"JOIN":    TOKEN_JOIN,
	"ON":      TOKEN_ON,
	// 演算子
	"EQ":  TOKEN_EQ,
	"NEQ": TOKEN_NEQ,
	"LT":  TOKEN_LT,
	"GT":  TOKEN_GT,
	"LTE": TOKEN_LTE,
	"GTE": TOKEN_GTE,
	// セパレータ
	"COMMA":     TOKEN_COMMA,
	"SEMICOLON": TOKEN_SEMICOLON,
	"LPAREN":    TOKEN_LPAREN,
	"RPAREN":    TOKEN_RPAREN,
	"ASTERISK":  TOKEN_ASTERISK,
}

// LookupIdent は識別子をトークンに変換する
// ident: 識別子
// return: トークン
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENT
}
