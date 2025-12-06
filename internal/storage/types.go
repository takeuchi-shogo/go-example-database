/*
カラムの型を定義する
*/
package storage

import "encoding/binary"

type ColumnType int

const (
	ColumnTypeInt32 ColumnType = iota + 1
	ColumnTypeInt64
	ColumnTypeFloat32
	ColumnTypeFloat64
	ColumnTypeString
	ColumnTypeBool
)

// TODO: Datum でもいいかも
type Value interface {
	Type() ColumnType
	Encode() []byte
	Size() int
}

// String
type StringValue string

func (v StringValue) Type() ColumnType { return ColumnTypeString }

func (v StringValue) Size() int { return 2 + len(v) } // 長さ2byte + 文字列

func (v StringValue) Encode() []byte {
	buf := make([]byte, 2+len(v))
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(v)))
	copy(buf[2:], v)
	return buf
}

// Bool
type BoolValue bool

func (v BoolValue) Type() ColumnType { return ColumnTypeBool }

func (v BoolValue) Size() int { return 1 }

func (v BoolValue) Encode() []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}
