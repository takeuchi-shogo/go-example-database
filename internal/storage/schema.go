package storage

// カラムを定義する
type Column struct {
	name       string
	columnType ColumnType
	size       uint16
	nullable   bool
}

// カラムを作成する
func NewColumn(name string, columnType ColumnType, size uint16, nullable bool) *Column {
	return &Column{name: name, columnType: columnType, size: size, nullable: nullable}
}

// カラムの名前を取得する
func (c *Column) GetName() string {
	return c.name
}

// カラムの型を取得する
func (c *Column) GetColumnType() ColumnType {
	return c.columnType
}

// カラムのサイズを取得する
func (c *Column) GetSize() uint16 {
	return c.size
}

// カラムのnullableを取得する
func (c *Column) GetNullable() bool {
	return c.nullable
}

// スキーマを定義する
type Schema struct {
	tableName string
	columns   []Column
}

// スキーマを作成する
func NewSchema(tableName string, columns []Column) *Schema {
	return &Schema{tableName: tableName, columns: columns}
}

// テーブルの名前を取得する
func (s *Schema) GetTableName() string {
	return s.tableName
}

// カラムの一覧を取得する
func (s *Schema) GetColumns() []Column {
	return s.columns
}

// カラム数を取得する
func (s *Schema) GetColumnCount() int {
	return len(s.columns)
}

func (s *Schema) Merge(other *Schema) *Schema {
	mergedColumns := make([]Column, 0, len(s.columns)+len(other.columns))
	mergedColumns = append(mergedColumns, s.columns...)
	mergedColumns = append(mergedColumns, other.columns...)
	return NewSchema(s.tableName, mergedColumns)
}
