package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
)

var (
	ErrColumnCountMismatch = errors.New("column count mismatch")
	ErrInvalidType         = errors.New("invalid type")
	ErrInvalidData         = errors.New("invalid data")
)

type Row struct {
	rowID  int64
	values []Value
}

func NewRow(values []Value) *Row {
	return &Row{rowID: 0, values: values}
}

func NewRowWithID(rowID int64, values []Value) *Row {
	return &Row{rowID: rowID, values: values}
}

// GetRowID は行IDを返す
func (r *Row) GetRowID() int64 {
	return r.rowID
}

// SetRowID は行IDを設定する
func (r *Row) SetRowID(rowID int64) {
	r.rowID = rowID
}

func (r *Row) GetValues() []Value {
	return r.values
}

func (r *Row) Encode() []byte {
	var buf []byte
	// 1. 行IDをエンコード
	rowIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(rowIDBytes, uint64(r.rowID))
	buf = append(buf, rowIDBytes...)

	// 2. 各カラムの値をエンコード
	for _, v := range r.values {
		if v == nil {
			// NULLの場合: 1byte目に0を書く
			buf = append(buf, 0)
		} else {
			// 非NULLの場合: 1byte目に1、その後に値
			buf = append(buf, 1)
			buf = append(buf, v.Encode()...)
		}
	}

	return buf
}

// DecodeRow はバイト列から行をデシリアライズ
func DecodeRow(data []byte, schema *Schema) (*Row, error) {
	if len(data) < 8 {
		return nil, ErrInvalidData
	}
	// 1. 行IDをデシリアライズ
	rowID := int64(binary.LittleEndian.Uint64(data[:8]))
	offset := 8

	values := make([]Value, len(schema.GetColumns()))

	for i, col := range schema.GetColumns() {
		if offset >= len(data) {
			return nil, ErrColumnCountMismatch
		}

		// NULLチェック
		isNull := data[offset] == 0
		offset++

		if isNull {
			values[i] = nil
			continue
		}

		// 型に応じてデコード
		switch col.GetColumnType() {
		case ColumnTypeInt32:
			val := int32(binary.LittleEndian.Uint32(data[offset:]))
			values[i] = Int32Value(val)
			offset += 4

		case ColumnTypeInt64:
			val := int64(binary.LittleEndian.Uint64(data[offset:]))
			values[i] = Int64Value(val)
			offset += 8

		case ColumnTypeString:
			// 長さを読む（2byte）
			length := binary.LittleEndian.Uint16(data[offset:])
			offset += 2
			// 文字列本体を読む
			str := string(data[offset : offset+int(length)])
			values[i] = StringValue(str)
			offset += int(length)

		case ColumnTypeBool:
			values[i] = BoolValue(data[offset] == 1)
			offset++

		default:
			return nil, ErrInvalidType
		}
	}

	return &Row{rowID: rowID, values: values}, nil
}

// Size は行のバイトサイズを返す
func (r *Row) Size() int {
	size := 0
	size += 8 // 行ID分
	for _, v := range r.values {
		size++ // NULLフラグ分
		if v != nil {
			size += int(v.Size())
		}
	}
	return size
}

type rowData struct {
	RowID  int64
	Values []Value
}

func (r *Row) Serialize() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, r.Size()))
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(rowData{RowID: r.rowID, Values: r.values}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DeserializeRow(data []byte) (*Row, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var rowData rowData
	if err := dec.Decode(&rowData); err != nil {
		return nil, err
	}
	return &Row{rowID: rowData.RowID, values: rowData.Values}, nil
}
