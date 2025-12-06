package storage

import (
	"testing"
)

// =============================================================================
// ColumnType Tests
// =============================================================================

func TestColumnTypeValues(t *testing.T) {
	testCases := []struct {
		name     string
		colType  ColumnType
		expected int
	}{
		{"ColumnTypeInt32", ColumnTypeInt32, 1},
		{"ColumnTypeInt64", ColumnTypeInt64, 2},
		{"ColumnTypeFloat32", ColumnTypeFloat32, 3},
		{"ColumnTypeFloat64", ColumnTypeFloat64, 4},
		{"ColumnTypeString", ColumnTypeString, 5},
		{"ColumnTypeBool", ColumnTypeBool, 6},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if int(tc.colType) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.colType, tc.expected)
			}
		})
	}
}

// =============================================================================
// StringValue Tests
// =============================================================================

func TestStringValueType(t *testing.T) {
	v := StringValue("hello")
	if v.Type() != ColumnTypeString {
		t.Errorf("StringValue.Type() = %v, want %v", v.Type(), ColumnTypeString)
	}
}

func TestStringValueSize(t *testing.T) {
	testCases := []struct {
		name     string
		value    StringValue
		expected int
	}{
		{"空文字列", StringValue(""), 2},                        // 長さ2byte + 0文字
		{"hello", StringValue("hello"), 7},                   // 長さ2byte + 5文字
		{"日本語", StringValue("日本語"), 2 + len("日本語")},          // UTF-8で9byte
		{"長い文字列", StringValue("abcdefghij"), 12},             // 長さ2byte + 10文字
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value.Size() != tc.expected {
				t.Errorf("StringValue(%q).Size() = %d, want %d", tc.value, tc.value.Size(), tc.expected)
			}
		})
	}
}

func TestStringValueEncode(t *testing.T) {
	testCases := []struct {
		name     string
		value    StringValue
		expected []byte
	}{
		{
			name:     "空文字列",
			value:    StringValue(""),
			expected: []byte{0x00, 0x00}, // 長さ0をリトルエンディアンで
		},
		{
			name:     "hello",
			value:    StringValue("hello"),
			expected: []byte{0x05, 0x00, 'h', 'e', 'l', 'l', 'o'}, // 長さ5 + 文字列
		},
		{
			name:     "ab",
			value:    StringValue("ab"),
			expected: []byte{0x02, 0x00, 'a', 'b'}, // 長さ2 + 文字列
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.value.Encode()
			if len(result) != len(tc.expected) {
				t.Errorf("StringValue(%q).Encode() length = %d, want %d", tc.value, len(result), len(tc.expected))
				return
			}
			for i, b := range result {
				if b != tc.expected[i] {
					t.Errorf("StringValue(%q).Encode()[%d] = %d, want %d", tc.value, i, b, tc.expected[i])
				}
			}
		})
	}
}

// =============================================================================
// BoolValue Tests
// =============================================================================

func TestBoolValueType(t *testing.T) {
	v := BoolValue(true)
	if v.Type() != ColumnTypeBool {
		t.Errorf("BoolValue.Type() = %v, want %v", v.Type(), ColumnTypeBool)
	}
}

func TestBoolValueSize(t *testing.T) {
	testCases := []struct {
		name     string
		value    BoolValue
		expected int
	}{
		{"true", BoolValue(true), 1},
		{"false", BoolValue(false), 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value.Size() != tc.expected {
				t.Errorf("BoolValue(%v).Size() = %d, want %d", tc.value, tc.value.Size(), tc.expected)
			}
		})
	}
}

func TestBoolValueEncode(t *testing.T) {
	testCases := []struct {
		name     string
		value    BoolValue
		expected []byte
	}{
		{"true", BoolValue(true), []byte{1}},
		{"false", BoolValue(false), []byte{0}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.value.Encode()
			if len(result) != len(tc.expected) {
				t.Errorf("BoolValue(%v).Encode() length = %d, want %d", tc.value, len(result), len(tc.expected))
				return
			}
			for i, b := range result {
				if b != tc.expected[i] {
					t.Errorf("BoolValue(%v).Encode()[%d] = %d, want %d", tc.value, i, b, tc.expected[i])
				}
			}
		})
	}
}

// =============================================================================
// Value Interface Tests
// =============================================================================

func TestValueInterfaceImplementation(t *testing.T) {
	// StringValue が Value インターフェースを実装していることを確認
	var _ Value = StringValue("test")

	// BoolValue が Value インターフェースを実装していることを確認
	var _ Value = BoolValue(true)
}
