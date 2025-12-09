package parser

import "testing"

func TestLookupIdent(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		// キーワード（大文字）
		{"SELECT", TOKEN_SELECT},
		{"INSERT", TOKEN_INSERT},
		{"UPDATE", TOKEN_UPDATE},
		{"DELETE", TOKEN_DELETE},
		{"FROM", TOKEN_FROM},
		{"WHERE", TOKEN_WHERE},
		{"INTO", TOKEN_INTO},
		{"VALUES", TOKEN_VALUES},
		{"SET", TOKEN_SET},
		{"CREATE", TOKEN_CREATE},
		{"TABLE", TOKEN_TABLE},
		{"DROP", TOKEN_DROP},
		{"AND", TOKEN_AND},
		{"OR", TOKEN_OR},
		{"NOT", TOKEN_NOT},
		{"NULL", TOKEN_NULL},
		{"PRIMARY", TOKEN_PRIMARY},
		{"KEY", TOKEN_KEY},
		{"ORDER", TOKEN_ORDER},
		{"BY", TOKEN_BY},
		{"ASC", TOKEN_ASC},
		{"DESC", TOKEN_DESC},
		{"LIMIT", TOKEN_LIMIT},
		{"OFFSET", TOKEN_OFFSET},

		// キーワード（小文字 - 大文字小文字を区別しない）
		{"select", TOKEN_SELECT},
		{"from", TOKEN_FROM},
		{"where", TOKEN_WHERE},

		// キーワード（混在）
		{"Select", TOKEN_SELECT},
		{"From", TOKEN_FROM},
		{"WhErE", TOKEN_WHERE},

		// 識別子（キーワードでない文字列）
		{"users", TOKEN_IDENT},
		{"id", TOKEN_IDENT},
		{"name", TOKEN_IDENT},
		{"myTable", TOKEN_IDENT},
		{"column_name", TOKEN_IDENT},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := LookupIdent(tt.input)
			if result != tt.expected {
				t.Errorf("LookupIdent(%q) = %v, want %v",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestNewToken(t *testing.T) {
	tok := newToken(TOKEN_SELECT, "SELECT")

	if tok.tokenType != TOKEN_SELECT {
		t.Errorf("expected tokenType=%v, got %v", TOKEN_SELECT, tok.tokenType)
	}

	if tok.literal != "SELECT" {
		t.Errorf("expected literal=%q, got %q", "SELECT", tok.literal)
	}
}
