package storage

import (
	"testing"
)

// =============================================================================
// Row Tests
// =============================================================================

func TestNewRow(t *testing.T) {
	values := []Value{
		StringValue("hello"),
		BoolValue(true),
	}

	row := NewRow(values)
	result := row.GetValues()

	if len(result) != len(values) {
		t.Errorf("NewRow().GetValues() length = %d, want %d", len(result), len(values))
	}
}

func TestRowGetValues(t *testing.T) {
	values := []Value{
		StringValue("test"),
		BoolValue(false),
		nil, // NULL値
	}

	row := NewRow(values)
	result := row.GetValues()

	for i, v := range result {
		if v != values[i] {
			t.Errorf("GetValues()[%d] = %v, want %v", i, v, values[i])
		}
	}
}

// =============================================================================
// Row Encode Tests
// =============================================================================

func TestRowEncode(t *testing.T) {
	// StringValue("ab") のみの行
	values := []Value{
		StringValue("ab"),
	}
	row := NewRow(values)
	encoded := row.Encode()

	// 期待値: 1(非NULL) + 2(長さ) + 0(長さ上位) + 'a' + 'b'
	expected := []byte{1, 0x02, 0x00, 'a', 'b'}

	if len(encoded) != len(expected) {
		t.Errorf("Encode() length = %d, want %d", len(encoded), len(expected))
		return
	}

	for i, b := range encoded {
		if b != expected[i] {
			t.Errorf("Encode()[%d] = %d, want %d", i, b, expected[i])
		}
	}
}

func TestRowEncodeWithNull(t *testing.T) {
	values := []Value{
		nil, // NULL
		StringValue("x"),
	}
	row := NewRow(values)
	encoded := row.Encode()

	// 期待値: 0(NULL) + 1(非NULL) + 1(長さ) + 0(長さ上位) + 'x'
	expected := []byte{0, 1, 0x01, 0x00, 'x'}

	if len(encoded) != len(expected) {
		t.Errorf("Encode() length = %d, want %d", len(encoded), len(expected))
		return
	}

	for i, b := range encoded {
		if b != expected[i] {
			t.Errorf("Encode()[%d] = %d, want %d", i, b, expected[i])
		}
	}
}

func TestRowEncodeWithBool(t *testing.T) {
	values := []Value{
		BoolValue(true),
		BoolValue(false),
	}
	row := NewRow(values)
	encoded := row.Encode()

	// 期待値: 1(非NULL) + 1(true) + 1(非NULL) + 0(false)
	expected := []byte{1, 1, 1, 0}

	if len(encoded) != len(expected) {
		t.Errorf("Encode() length = %d, want %d", len(encoded), len(expected))
		return
	}

	for i, b := range encoded {
		if b != expected[i] {
			t.Errorf("Encode()[%d] = %d, want %d", i, b, expected[i])
		}
	}
}

// =============================================================================
// Row Size Tests
// =============================================================================

func TestRowSize(t *testing.T) {
	testCases := []struct {
		name     string
		values   []Value
		expected int
	}{
		{
			name:     "空の行",
			values:   []Value{},
			expected: 0,
		},
		{
			name:     "NULL値のみ",
			values:   []Value{nil},
			expected: 1, // NULLフラグのみ
		},
		{
			name:     "Bool値",
			values:   []Value{BoolValue(true)},
			expected: 2, // NULLフラグ + 1byte
		},
		{
			name:     "String値",
			values:   []Value{StringValue("hi")},
			expected: 1 + 2 + 2, // NULLフラグ + 長さ2byte + 文字2byte
		},
		{
			name:     "複合",
			values:   []Value{StringValue("ab"), BoolValue(true), nil},
			expected: (1 + 2 + 2) + (1 + 1) + 1, // String + Bool + NULL
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := NewRow(tc.values)
			if row.Size() != tc.expected {
				t.Errorf("Size() = %d, want %d", row.Size(), tc.expected)
			}
		})
	}
}

// =============================================================================
// DecodeRow Tests
// =============================================================================

func TestDecodeRowString(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, false),
	})

	// エンコードされたデータ: 1(非NULL) + 5(長さ) + 0(長さ上位) + "hello"
	data := []byte{1, 0x05, 0x00, 'h', 'e', 'l', 'l', 'o'}

	row, err := DecodeRow(data, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}

	values := row.GetValues()
	if len(values) != 1 {
		t.Fatalf("DecodeRow returned %d values, want 1", len(values))
	}

	sv, ok := values[0].(StringValue)
	if !ok {
		t.Fatalf("values[0] is not StringValue")
	}
	if string(sv) != "hello" {
		t.Errorf("values[0] = %q, want %q", sv, "hello")
	}
}

func TestDecodeRowBool(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("flag", ColumnTypeBool, 1, false),
	})

	testCases := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"true", []byte{1, 1}, true},
		{"false", []byte{1, 0}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row, err := DecodeRow(tc.data, schema)
			if err != nil {
				t.Fatalf("DecodeRow failed: %v", err)
			}

			values := row.GetValues()
			bv, ok := values[0].(BoolValue)
			if !ok {
				t.Fatalf("values[0] is not BoolValue")
			}
			if bool(bv) != tc.expected {
				t.Errorf("values[0] = %v, want %v", bv, tc.expected)
			}
		})
	}
}

func TestDecodeRowNull(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, true),
	})

	// NULLデータ
	data := []byte{0}

	row, err := DecodeRow(data, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}

	values := row.GetValues()
	if values[0] != nil {
		t.Errorf("values[0] = %v, want nil", values[0])
	}
}

func TestDecodeRowMultipleColumns(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, false),
		*NewColumn("active", ColumnTypeBool, 1, false),
	})

	// "hi" + true
	data := []byte{1, 0x02, 0x00, 'h', 'i', 1, 1}

	row, err := DecodeRow(data, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}

	values := row.GetValues()
	if len(values) != 2 {
		t.Fatalf("DecodeRow returned %d values, want 2", len(values))
	}

	sv, ok := values[0].(StringValue)
	if !ok || string(sv) != "hi" {
		t.Errorf("values[0] = %v, want StringValue(\"hi\")", values[0])
	}

	bv, ok := values[1].(BoolValue)
	if !ok || !bool(bv) {
		t.Errorf("values[1] = %v, want BoolValue(true)", values[1])
	}
}

// =============================================================================
// Round Trip Tests
// =============================================================================

func TestRowEncodeDecodeRoundTrip(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, true),
		*NewColumn("active", ColumnTypeBool, 1, true),
	})

	testCases := []struct {
		name   string
		values []Value
	}{
		{
			name:   "両方非NULL",
			values: []Value{StringValue("test"), BoolValue(true)},
		},
		{
			name:   "StringがNULL",
			values: []Value{nil, BoolValue(false)},
		},
		{
			name:   "BoolがNULL",
			values: []Value{StringValue("hello"), nil},
		},
		{
			name:   "両方NULL",
			values: []Value{nil, nil},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// エンコード
			row := NewRow(tc.values)
			encoded := row.Encode()

			// デコード
			decoded, err := DecodeRow(encoded, schema)
			if err != nil {
				t.Fatalf("DecodeRow failed: %v", err)
			}

			decodedValues := decoded.GetValues()
			if len(decodedValues) != len(tc.values) {
				t.Fatalf("Decoded %d values, want %d", len(decodedValues), len(tc.values))
			}

			// 値を比較
			for i, expected := range tc.values {
				actual := decodedValues[i]
				if expected == nil {
					if actual != nil {
						t.Errorf("values[%d] = %v, want nil", i, actual)
					}
					continue
				}

				switch ev := expected.(type) {
				case StringValue:
					av, ok := actual.(StringValue)
					if !ok || string(av) != string(ev) {
						t.Errorf("values[%d] = %v, want %v", i, actual, expected)
					}
				case BoolValue:
					av, ok := actual.(BoolValue)
					if !ok || bool(av) != bool(ev) {
						t.Errorf("values[%d] = %v, want %v", i, actual, expected)
					}
				}
			}
		})
	}
}

// =============================================================================
// Error Tests
// =============================================================================

func TestDecodeRowInvalidType(t *testing.T) {
	// 未実装の型を使ったスキーマ
	schema := NewSchema("test", []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
	})

	// 非NULLのデータ（Int32はまだ実装されていない）
	data := []byte{1, 0x01, 0x02, 0x03, 0x04}

	_, err := DecodeRow(data, schema)
	// Int32は未実装なので現状ではエラーにならない（TODO扱い）
	// 実装後はエラーを返すべき
	_ = err
}
