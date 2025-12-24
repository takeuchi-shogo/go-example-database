package storage

import (
	"encoding/binary"
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

	// NewRow で作成した場合、RowID は 0
	if row.GetRowID() != 0 {
		t.Errorf("NewRow().GetRowID() = %d, want 0", row.GetRowID())
	}
}

func TestRow_RowID(t *testing.T) {
	values := []Value{
		Int32Value(123),
		StringValue("hello"),
	}
	// not rowID
	row := NewRow(values)
	if row.GetRowID() != 0 {
		t.Errorf("NewRow().GetRowID() = %d, want 0", row.GetRowID())
	}
	// with rowID
	row = NewRowWithID(123, values)
	if row.GetRowID() != 123 {
		t.Errorf("NewRowWithID().GetRowID() = %d, want 123", row.GetRowID())
	}
}

func TestNewRowWithID(t *testing.T) {
	values := []Value{
		StringValue("hello"),
		BoolValue(true),
	}

	row := NewRowWithID(42, values)

	if row.GetRowID() != 42 {
		t.Errorf("NewRowWithID().GetRowID() = %d, want 42", row.GetRowID())
	}

	result := row.GetValues()
	if len(result) != len(values) {
		t.Errorf("NewRowWithID().GetValues() length = %d, want %d", len(result), len(values))
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

func TestRowSetRowID(t *testing.T) {
	row := NewRow([]Value{StringValue("test")})

	if row.GetRowID() != 0 {
		t.Errorf("initial RowID = %d, want 0", row.GetRowID())
	}

	row.SetRowID(123)

	if row.GetRowID() != 123 {
		t.Errorf("after SetRowID(123), RowID = %d, want 123", row.GetRowID())
	}
}

// =============================================================================
// Row Encode Tests
// =============================================================================

// makeRowIDBytes は RowID を 8 bytes の Little Endian バイト列に変換する
func makeRowIDBytes(rowID int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(rowID))
	return buf
}

func TestRowEncode(t *testing.T) {
	// StringValue("ab") のみの行
	values := []Value{
		StringValue("ab"),
	}
	row := NewRow(values) // RowID = 0
	encoded := row.Encode()

	// 期待値: RowID(8bytes) + 1(非NULL) + 2(長さ) + 0(長さ上位) + 'a' + 'b'
	expected := append(makeRowIDBytes(0), 1, 0x02, 0x00, 'a', 'b')

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

func TestRowEncodeWithRowID(t *testing.T) {
	values := []Value{
		StringValue("ab"),
	}
	row := NewRowWithID(12345, values)
	encoded := row.Encode()

	// 期待値: RowID(8bytes, 12345) + 1(非NULL) + 2(長さ) + 0(長さ上位) + 'a' + 'b'
	expected := append(makeRowIDBytes(12345), 1, 0x02, 0x00, 'a', 'b')

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

	// 期待値: RowID(8bytes) + 0(NULL) + 1(非NULL) + 1(長さ) + 0(長さ上位) + 'x'
	expected := append(makeRowIDBytes(0), 0, 1, 0x01, 0x00, 'x')

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

	// 期待値: RowID(8bytes) + 1(非NULL) + 1(true) + 1(非NULL) + 0(false)
	expected := append(makeRowIDBytes(0), 1, 1, 1, 0)

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
			expected: 8, // RowID のみ
		},
		{
			name:     "NULL値のみ",
			values:   []Value{nil},
			expected: 8 + 1, // RowID + NULLフラグ
		},
		{
			name:     "Bool値",
			values:   []Value{BoolValue(true)},
			expected: 8 + 2, // RowID + NULLフラグ + 1byte
		},
		{
			name:     "String値",
			values:   []Value{StringValue("hi")},
			expected: 8 + 1 + 2 + 2, // RowID + NULLフラグ + 長さ2byte + 文字2byte
		},
		{
			name:     "複合",
			values:   []Value{StringValue("ab"), BoolValue(true), nil},
			expected: 8 + (1 + 2 + 2) + (1 + 1) + 1, // RowID + String + Bool + NULL
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

	// エンコードされたデータ: RowID(8bytes) + 1(非NULL) + 5(長さ) + 0(長さ上位) + "hello"
	data := append(makeRowIDBytes(0), 1, 0x05, 0x00, 'h', 'e', 'l', 'l', 'o')

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

	// RowID も確認
	if row.GetRowID() != 0 {
		t.Errorf("RowID = %d, want 0", row.GetRowID())
	}
}

func TestDecodeRowWithRowID(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, false),
	})

	// エンコードされたデータ: RowID=999 + "hello"
	data := append(makeRowIDBytes(999), 1, 0x05, 0x00, 'h', 'e', 'l', 'l', 'o')

	row, err := DecodeRow(data, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}

	if row.GetRowID() != 999 {
		t.Errorf("RowID = %d, want 999", row.GetRowID())
	}

	values := row.GetValues()
	sv, ok := values[0].(StringValue)
	if !ok || string(sv) != "hello" {
		t.Errorf("values[0] = %v, want StringValue(\"hello\")", values[0])
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
		{"true", append(makeRowIDBytes(0), 1, 1), true},
		{"false", append(makeRowIDBytes(0), 1, 0), false},
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

	// NULLデータ: RowID(8bytes) + 0(NULL)
	data := append(makeRowIDBytes(0), 0)

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

	// RowID(8bytes) + "hi" + true
	data := append(makeRowIDBytes(0), 1, 0x02, 0x00, 'h', 'i', 1, 1)

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

func TestDecodeRowTooShort(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("name", ColumnTypeString, 255, false),
	})

	// RowID に満たないデータ（7 bytes）
	data := []byte{1, 2, 3, 4, 5, 6, 7}

	_, err := DecodeRow(data, schema)
	if err == nil {
		t.Error("DecodeRow should fail with data shorter than 8 bytes")
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
		rowID  int64
		values []Value
	}{
		{
			name:   "両方非NULL",
			rowID:  1,
			values: []Value{StringValue("test"), BoolValue(true)},
		},
		{
			name:   "StringがNULL",
			rowID:  2,
			values: []Value{nil, BoolValue(false)},
		},
		{
			name:   "BoolがNULL",
			rowID:  3,
			values: []Value{StringValue("hello"), nil},
		},
		{
			name:   "両方NULL",
			rowID:  4,
			values: []Value{nil, nil},
		},
		{
			name:   "RowID=0",
			rowID:  0,
			values: []Value{StringValue("zero"), BoolValue(true)},
		},
		{
			name:   "大きなRowID",
			rowID:  9223372036854775807, // int64 max
			values: []Value{StringValue("max"), BoolValue(false)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// エンコード
			row := NewRowWithID(tc.rowID, tc.values)
			encoded := row.Encode()

			// デコード
			decoded, err := DecodeRow(encoded, schema)
			if err != nil {
				t.Fatalf("DecodeRow failed: %v", err)
			}

			// RowID を確認
			if decoded.GetRowID() != tc.rowID {
				t.Errorf("RowID = %d, want %d", decoded.GetRowID(), tc.rowID)
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
	// Int32 型のスキーマ
	schema := NewSchema("test", []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
	})

	// RowID(8bytes) + 非NULLのデータ（Int32）
	data := append(makeRowIDBytes(0), 1, 0x01, 0x02, 0x03, 0x04)

	row, err := DecodeRow(data, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}

	values := row.GetValues()
	if len(values) != 1 {
		t.Fatalf("DecodeRow returned %d values, want 1", len(values))
	}

	// Int32Value として取得できるか確認
	iv, ok := values[0].(Int32Value)
	if !ok {
		t.Fatalf("values[0] is not Int32Value, got %T", values[0])
	}

	// 0x04030201 = 67305985 (Little Endian)
	expected := int32(0x04030201)
	if int32(iv) != expected {
		t.Errorf("values[0] = %d, want %d", iv, expected)
	}
}

// =============================================================================
// Serialize/Deserialize Tests (gob)
// =============================================================================

func TestRowSerializeDeserialize(t *testing.T) {
	row := NewRowWithID(42, []Value{
		Int32Value(100),
		StringValue("hello"),
		BoolValue(true),
	})

	// シリアライズ
	data, err := row.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// デシリアライズ
	decoded, err := DeserializeRow(data)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	// RowID は gob でシリアライズされているか確認
	if decoded.GetRowID() != 42 {
		t.Errorf("RowID = %d, want 42", decoded.GetRowID())
	}

	// 値の確認
	values := decoded.GetValues()
	if len(values) != 3 {
		t.Fatalf("values length = %d, want 3", len(values))
	}
}

func TestRow_EncodeDecodeWithRowID(t *testing.T) {
	schema := NewSchema("test", []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
		*NewColumn("name", ColumnTypeString, 255, false),
	})
	row := NewRowWithID(123, []Value{
		Int32Value(100),
		StringValue("hello"),
	})
	encoded := row.Encode()
	decoded, err := DecodeRow(encoded, schema)
	if err != nil {
		t.Fatalf("DecodeRow failed: %v", err)
	}
	if decoded.GetRowID() != 123 {
		t.Errorf("RowID = %d, want 123", decoded.GetRowID())
	}
}
