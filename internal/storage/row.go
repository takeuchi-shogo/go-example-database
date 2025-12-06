package storage

import (
	"encoding/binary"
	"errors"
)

var (
	ErrColumnCountMismatch = errors.New("column count mismatch")
	ErrInvalidType         = errors.New("invalid type")
)

type Row struct {
	values []Value
}

func NewRow(values []Value) *Row {
	return &Row{values: values}
}

func (r *Row) GetValues() []Value {
	return r.values
}

func (r *Row) Encode() []byte {
	var buf []byte
	// 各カラムの値をエンコード
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
	values := make([]Value, len(schema.GetColumns()))
	offset := 0

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
			// TODO: not implemented

		case ColumnTypeInt64:
			// TODO: not implemented

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

	return &Row{values: values}, nil
}

// Size は行のバイトサイズを返す
func (r *Row) Size() int {
	size := 0
	for _, v := range r.values {
		size++ // NULLフラグ分
		if v != nil {
			size += int(v.Size())
		}
	}
	return size
}
