package storage

import (
	"testing"
)

// =============================================================================
// Column Tests
// =============================================================================

func TestNewColumn(t *testing.T) {
	col := NewColumn("id", ColumnTypeInt32, 4, false)

	if col.GetName() != "id" {
		t.Errorf("NewColumn().GetName() = %q, want %q", col.GetName(), "id")
	}
	if col.GetColumnType() != ColumnTypeInt32 {
		t.Errorf("NewColumn().GetColumnType() = %v, want %v", col.GetColumnType(), ColumnTypeInt32)
	}
	if col.GetSize() != 4 {
		t.Errorf("NewColumn().GetSize() = %d, want %d", col.GetSize(), 4)
	}
	if col.GetNullable() != false {
		t.Errorf("NewColumn().GetNullable() = %v, want %v", col.GetNullable(), false)
	}
}

func TestColumnGetters(t *testing.T) {
	testCases := []struct {
		name       string
		columnName string
		columnType ColumnType
		size       uint16
		nullable   bool
	}{
		{"Int32カラム", "id", ColumnTypeInt32, 4, false},
		{"Int64カラム", "bigid", ColumnTypeInt64, 8, false},
		{"Stringカラム", "name", ColumnTypeString, 255, true},
		{"Boolカラム", "active", ColumnTypeBool, 1, true},
		{"Float32カラム", "price", ColumnTypeFloat32, 4, false},
		{"Float64カラム", "amount", ColumnTypeFloat64, 8, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			col := NewColumn(tc.columnName, tc.columnType, tc.size, tc.nullable)

			if col.GetName() != tc.columnName {
				t.Errorf("GetName() = %q, want %q", col.GetName(), tc.columnName)
			}
			if col.GetColumnType() != tc.columnType {
				t.Errorf("GetColumnType() = %v, want %v", col.GetColumnType(), tc.columnType)
			}
			if col.GetSize() != tc.size {
				t.Errorf("GetSize() = %d, want %d", col.GetSize(), tc.size)
			}
			if col.GetNullable() != tc.nullable {
				t.Errorf("GetNullable() = %v, want %v", col.GetNullable(), tc.nullable)
			}
		})
	}
}

// =============================================================================
// Schema Tests
// =============================================================================

func TestNewSchema(t *testing.T) {
	columns := []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
		*NewColumn("name", ColumnTypeString, 255, true),
	}

	schema := NewSchema("users", columns)

	if schema.GetTableName() != "users" {
		t.Errorf("NewSchema().GetTableName() = %q, want %q", schema.GetTableName(), "users")
	}
}

func TestSchemaGetColumns(t *testing.T) {
	columns := []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
		*NewColumn("name", ColumnTypeString, 255, true),
		*NewColumn("active", ColumnTypeBool, 1, false),
	}

	schema := NewSchema("users", columns)
	result := schema.GetColumns()

	if len(result) != len(columns) {
		t.Errorf("GetColumns() length = %d, want %d", len(result), len(columns))
	}

	for i, col := range result {
		if col.GetName() != columns[i].GetName() {
			t.Errorf("GetColumns()[%d].GetName() = %q, want %q", i, col.GetName(), columns[i].GetName())
		}
	}
}

func TestSchemaGetColumnCount(t *testing.T) {
	testCases := []struct {
		name          string
		columns       []Column
		expectedCount int
	}{
		{
			name:          "カラムなし",
			columns:       []Column{},
			expectedCount: 0,
		},
		{
			name: "1カラム",
			columns: []Column{
				*NewColumn("id", ColumnTypeInt32, 4, false),
			},
			expectedCount: 1,
		},
		{
			name: "3カラム",
			columns: []Column{
				*NewColumn("id", ColumnTypeInt32, 4, false),
				*NewColumn("name", ColumnTypeString, 255, true),
				*NewColumn("active", ColumnTypeBool, 1, false),
			},
			expectedCount: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSchema("test", tc.columns)
			if schema.GetColumnCount() != tc.expectedCount {
				t.Errorf("GetColumnCount() = %d, want %d", schema.GetColumnCount(), tc.expectedCount)
			}
		})
	}
}

func TestSchemaWithDifferentTables(t *testing.T) {
	// usersテーブル
	usersColumns := []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
		*NewColumn("username", ColumnTypeString, 50, false),
		*NewColumn("email", ColumnTypeString, 255, true),
	}
	usersSchema := NewSchema("users", usersColumns)

	// productsテーブル
	productsColumns := []Column{
		*NewColumn("id", ColumnTypeInt32, 4, false),
		*NewColumn("name", ColumnTypeString, 100, false),
		*NewColumn("price", ColumnTypeFloat64, 8, false),
		*NewColumn("in_stock", ColumnTypeBool, 1, false),
	}
	productsSchema := NewSchema("products", productsColumns)

	// usersの検証
	if usersSchema.GetTableName() != "users" {
		t.Errorf("users schema GetTableName() = %q, want %q", usersSchema.GetTableName(), "users")
	}
	if usersSchema.GetColumnCount() != 3 {
		t.Errorf("users schema GetColumnCount() = %d, want %d", usersSchema.GetColumnCount(), 3)
	}

	// productsの検証
	if productsSchema.GetTableName() != "products" {
		t.Errorf("products schema GetTableName() = %q, want %q", productsSchema.GetTableName(), "products")
	}
	if productsSchema.GetColumnCount() != 4 {
		t.Errorf("products schema GetColumnCount() = %d, want %d", productsSchema.GetColumnCount(), 4)
	}
}
