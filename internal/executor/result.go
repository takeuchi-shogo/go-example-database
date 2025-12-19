package executor

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

type ResultSet interface {
	// AddRow は行を追加する
	AddRow(row *storage.Row)
	// GetSchema はスキーマを取得する
	GetSchema() *storage.Schema
	// GetRows は行を取得する
	GetRows() []*storage.Row
	// GetRowCount は行数を取得する
	GetRowCount() int
	// GetColumnCount はカラム数を取得する
	GetColumnCount() int
	// GetMessage はメッセージを取得する
	GetMessage() string
	// IsEmpty は空かどうかを返す
	IsEmpty() bool
	// String は文字列を返す
	String() string
}

type resultSet struct {
	schema  *storage.Schema
	columns []string
	rows    []*storage.Row
	message string
}

func NewResultSet() ResultSet {
	return &resultSet{}
}

func NewResultSetWithRows(rows []*storage.Row) ResultSet {
	rs := NewResultSet()
	for _, row := range rows {
		rs.AddRow(row)
	}
	return rs
}

func NewResultSetWithRowsAndSchema(schema *storage.Schema, rows []*storage.Row) ResultSet {
	rs := &resultSet{schema: schema}
	for _, row := range rows {
		rs.AddRow(row)
	}
	return rs
}

func NewResultSetWithMessage(message string) ResultSet {
	return &resultSet{message: message}
}

func (r *resultSet) GetSchema() *storage.Schema {
	return r.schema
}

func (r *resultSet) AddRow(row *storage.Row) {
	r.rows = append(r.rows, row)
}

func (r *resultSet) GetColumns() []string {
	return r.columns
}

func (r *resultSet) GetRows() []*storage.Row {
	return r.rows
}

func (r *resultSet) GetMessage() string {
	return r.message
}

// GetRowCount は結果セットの行数を返す
func (r *resultSet) GetRowCount() int {
	return len(r.rows)
}

// GetColumnCount は結果セットのカラム数を返す
func (r *resultSet) GetColumnCount() int {
	return len(r.columns)
}

// IsEmpty は結果セットが空かどうかを返す
func (r *resultSet) IsEmpty() bool {
	return len(r.rows) == 0
}

// String は REPL での表示用の文字列を返す
func (r *resultSet) String() string {
	return fmt.Sprintf("ResultSet{columns: %v, rows: %v, message: %s}", r.columns, r.rows, r.message)
}
