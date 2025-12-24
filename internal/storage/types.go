/*
カラムの型を定義する
*/
package storage

import (
	"encoding/binary"
	"encoding/gob"
)

func init() {
	// gob でインターフェース型をシリアライズするために型を登録
	gob.Register(Int32Value(0))
	gob.Register(Int64Value(0))
	gob.Register(StringValue(""))
	gob.Register(BoolValue(false))
}

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

// Int32
type Int32Value int32

func (v Int32Value) Type() ColumnType { return ColumnTypeInt32 }

func (v Int32Value) Size() int { return 4 }

func (v Int32Value) Encode() []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(v))
	return buf
}

// Int64
type Int64Value int64

func (v Int64Value) Type() ColumnType { return ColumnTypeInt64 }

func (v Int64Value) Size() int { return 8 }

func (v Int64Value) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v))
	return buf
}
