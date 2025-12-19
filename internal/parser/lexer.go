package parser

type lexer struct {
	input        string // 入力文字列
	position     int    // 現在の位置
	readPosition int    // 次の位置
	ch           byte   // 現在の文字
}

// NewLexer は新しい Lexer を作成する
// SQL 文字列をトークン（単語）に分割する役割を担う
func NewLexer(input string) *lexer {
	l := &lexer{input: input}
	l.readChar()
	return l
}

// readChar は1文字読み込む
func (l *lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

// peekChar は次の文字を見る（位置は進まない）
func (l *lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// nextToken は次のトークンを読み込む
func (l *lexer) nextToken() *token {
	var tok token

	l.skipWhitespace() // 空白をスキップ

	switch l.ch {
	case '=':
		tok = newToken(TOKEN_EQ, string(l.ch))
	case ',':
		tok = newToken(TOKEN_COMMA, string(l.ch))
	case ';':
		tok = newToken(TOKEN_SEMICOLON, string(l.ch))
	case '(':
		tok = newToken(TOKEN_LPAREN, string(l.ch))
	case ')':
		tok = newToken(TOKEN_RPAREN, string(l.ch))
	case '*':
		tok = newToken(TOKEN_ASTERISK, string(l.ch))
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = newToken(TOKEN_LTE, string(ch)+string(l.ch))
		} else {
			tok = newToken(TOKEN_LT, string(l.ch))
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = newToken(TOKEN_GTE, string(ch)+string(l.ch))
		} else {
			tok = newToken(TOKEN_GT, string(l.ch))
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = newToken(TOKEN_NEQ, string(ch)+string(l.ch))
		} else {
			tok = newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	case '\'':
		tok.tokenType = TOKEN_VARCHAR
		tok.literal = l.readString()
	case 0:
		tok = newToken(TOKEN_EOF, "")
	default:
		if isLetter(l.ch) {
			tok.literal = l.readIdentifier()
			tok.tokenType = LookupIdent(tok.literal)
			return &tok
		} else if isDigit(l.ch) {
			tok.tokenType = TOKEN_INT
			tok.literal = l.readNumber()
			return &tok
		} else {
			tok = newToken(TOKEN_ILLEGAL, string(l.ch))
		}
	}

	l.readChar()
	return &tok
}

// skipWhitespace は空白をスキップする
func (l *lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readIdentifier は識別子を読み込む
func (l *lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber は数字を読み込む
func (l *lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readString は文字列を読み込む
func (l *lexer) readString() string {
	l.readChar() // 開始の ' をスキップ
	position := l.position
	for l.ch != '\'' && l.ch != 0 {
		l.readChar()
	}
	return l.input[position:l.position]
}

// isLetter は文字が英字かアンダースコアかを判定する
// ch: 文字
// return: 英字かアンダースコアか
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit は文字が数字かを判定する
// ch: 文字
// return: 数字か
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
