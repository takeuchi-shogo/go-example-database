package storage

import (
	"testing"
)

// =============================================================================
// Constants Tests
// =============================================================================

func TestPageLayoutConstants(t *testing.T) {
	// PageHeaderSize が 6 であることを確認
	if PageHeaderSize != 6 {
		t.Errorf("PageHeaderSize = %d, want 6", PageHeaderSize)
	}

	// SlotSize が 4 であることを確認
	if SlotSize != 4 {
		t.Errorf("SlotSize = %d, want 4", SlotSize)
	}

	// DeletedSlotOffset が 0xFFFF であることを確認
	if DeletedSlotOffset != 0xFFFF {
		t.Errorf("DeletedSlotOffset = %d, want %d", DeletedSlotOffset, 0xFFFF)
	}
}

// =============================================================================
// NewSlottedPage Tests
// =============================================================================

func TestNewSlottedPage(t *testing.T) {
	page := NewSlottedPage()

	// rowCount が 0 であることを確認
	if page.rowCount() != 0 {
		t.Errorf("NewSlottedPage().rowCount() = %d, want 0", page.rowCount())
	}

	// freeSpaceStart がヘッダー直後であることを確認
	if page.freeSpaceStart() != PageHeaderSize {
		t.Errorf("NewSlottedPage().freeSpaceStart() = %d, want %d", page.freeSpaceStart(), PageHeaderSize)
	}

	// freeSpaceEnd がページ末尾であることを確認
	if page.freeSpaceEnd() != uint16(pageSize) {
		t.Errorf("NewSlottedPage().freeSpaceEnd() = %d, want %d", page.freeSpaceEnd(), pageSize)
	}
}

// =============================================================================
// LoadSlottedPage Tests
// =============================================================================

func TestLoadSlottedPage(t *testing.T) {
	// 新しいページを作成してデータを取得
	original := NewSlottedPage()
	data := original.Data()

	// データからページをロード
	loaded := LoadSlottedPage(data)

	// 同じ状態であることを確認
	if loaded.rowCount() != original.rowCount() {
		t.Errorf("LoadSlottedPage().rowCount() = %d, want %d", loaded.rowCount(), original.rowCount())
	}
	if loaded.freeSpaceStart() != original.freeSpaceStart() {
		t.Errorf("LoadSlottedPage().freeSpaceStart() = %d, want %d", loaded.freeSpaceStart(), original.freeSpaceStart())
	}
	if loaded.freeSpaceEnd() != original.freeSpaceEnd() {
		t.Errorf("LoadSlottedPage().freeSpaceEnd() = %d, want %d", loaded.freeSpaceEnd(), original.freeSpaceEnd())
	}
}

// =============================================================================
// FreeSpace Tests
// =============================================================================

func TestFreeSpace(t *testing.T) {
	page := NewSlottedPage()

	// 初期状態では freeSpaceEnd - freeSpaceStart
	expected := uint16(pageSize) - PageHeaderSize
	if page.FreeSpace() != expected {
		t.Errorf("NewSlottedPage().FreeSpace() = %d, want %d", page.FreeSpace(), expected)
	}
}

// =============================================================================
// InsertRow Tests
// =============================================================================

func TestInsertRow(t *testing.T) {
	page := NewSlottedPage()
	rowData := []byte("Hello, World!")

	// 行を挿入
	slotID, err := page.InsertRow(rowData)
	if err != nil {
		t.Fatalf("InsertRow failed: %v", err)
	}

	// slotID が 0 であることを確認（最初の行）
	if slotID != 0 {
		t.Errorf("InsertRow returned slotID = %d, want 0", slotID)
	}

	// rowCount が 1 になることを確認
	if page.rowCount() != 1 {
		t.Errorf("After InsertRow, rowCount() = %d, want 1", page.rowCount())
	}
}

func TestInsertRowMultiple(t *testing.T) {
	page := NewSlottedPage()

	// 複数の行を挿入
	for i := 0; i < 5; i++ {
		rowData := []byte("Row data")
		slotID, err := page.InsertRow(rowData)
		if err != nil {
			t.Fatalf("InsertRow(%d) failed: %v", i, err)
		}
		if slotID != uint16(i) {
			t.Errorf("InsertRow returned slotID = %d, want %d", slotID, i)
		}
	}

	// rowCount が 5 であることを確認
	if page.rowCount() != 5 {
		t.Errorf("After 5 InsertRow, rowCount() = %d, want 5", page.rowCount())
	}
}

func TestInsertRowPageFull(t *testing.T) {
	page := NewSlottedPage()

	// ページがいっぱいになるまで行を挿入
	largeRow := make([]byte, 500)
	insertCount := 0
	for {
		_, err := page.InsertRow(largeRow)
		if err != nil {
			if err != ErrPageFull {
				t.Fatalf("Expected ErrPageFull, got: %v", err)
			}
			break
		}
		insertCount++
		if insertCount > 100 {
			t.Fatal("Too many inserts, expected page to fill up")
		}
	}

	// 少なくとも1つは挿入できたことを確認
	if insertCount == 0 {
		t.Error("Should be able to insert at least one row")
	}
}

// =============================================================================
// GetRow Tests
// =============================================================================

func TestGetRow(t *testing.T) {
	page := NewSlottedPage()
	rowData := []byte("Test row data")

	// 行を挿入
	slotID, err := page.InsertRow(rowData)
	if err != nil {
		t.Fatalf("InsertRow failed: %v", err)
	}

	// 行を取得
	result, err := page.GetRow(slotID)
	if err != nil {
		t.Fatalf("GetRow failed: %v", err)
	}

	// データが一致することを確認
	if len(result) != len(rowData) {
		t.Errorf("GetRow returned data length = %d, want %d", len(result), len(rowData))
	}
	for i, b := range result {
		if b != rowData[i] {
			t.Errorf("GetRow returned data[%d] = %d, want %d", i, b, rowData[i])
		}
	}
}

func TestGetRowInvalidSlotID(t *testing.T) {
	page := NewSlottedPage()

	// 存在しないスロットIDで取得を試みる
	_, err := page.GetRow(0)
	if err != ErrInvalidSlotID {
		t.Errorf("GetRow with invalid slotID returned err = %v, want ErrInvalidSlotID", err)
	}
}

func TestGetRowMultiple(t *testing.T) {
	page := NewSlottedPage()

	// 異なるデータを持つ複数の行を挿入
	rows := [][]byte{
		[]byte("First row"),
		[]byte("Second row with more data"),
		[]byte("Third"),
	}

	for _, rowData := range rows {
		_, err := page.InsertRow(rowData)
		if err != nil {
			t.Fatalf("InsertRow failed: %v", err)
		}
	}

	// 各行を取得して検証
	for i, expected := range rows {
		result, err := page.GetRow(uint16(i))
		if err != nil {
			t.Fatalf("GetRow(%d) failed: %v", i, err)
		}
		if string(result) != string(expected) {
			t.Errorf("GetRow(%d) = %q, want %q", i, result, expected)
		}
	}
}

// =============================================================================
// DeleteRow Tests
// =============================================================================

func TestDeleteRow(t *testing.T) {
	page := NewSlottedPage()
	rowData := []byte("Row to delete")

	// 行を挿入
	slotID, err := page.InsertRow(rowData)
	if err != nil {
		t.Fatalf("InsertRow failed: %v", err)
	}

	// 行を削除
	err = page.DeleteRow(slotID)
	if err != nil {
		t.Fatalf("DeleteRow failed: %v", err)
	}

	// 削除された行を取得しようとするとエラー
	_, err = page.GetRow(slotID)
	if err != ErrSlotDeleted {
		t.Errorf("GetRow after DeleteRow returned err = %v, want ErrSlotDeleted", err)
	}
}

func TestDeleteRowInvalidSlotID(t *testing.T) {
	page := NewSlottedPage()

	// 存在しないスロットIDで削除を試みる
	err := page.DeleteRow(0)
	if err != ErrInvalidSlotID {
		t.Errorf("DeleteRow with invalid slotID returned err = %v, want ErrInvalidSlotID", err)
	}
}

func TestDeleteRowDoesNotAffectOthers(t *testing.T) {
	page := NewSlottedPage()

	// 3つの行を挿入
	rows := [][]byte{
		[]byte("First"),
		[]byte("Second"),
		[]byte("Third"),
	}

	for _, rowData := range rows {
		_, err := page.InsertRow(rowData)
		if err != nil {
			t.Fatalf("InsertRow failed: %v", err)
		}
	}

	// 中間の行を削除
	err := page.DeleteRow(1)
	if err != nil {
		t.Fatalf("DeleteRow failed: %v", err)
	}

	// 他の行は取得できることを確認
	result0, err := page.GetRow(0)
	if err != nil {
		t.Fatalf("GetRow(0) after delete failed: %v", err)
	}
	if string(result0) != "First" {
		t.Errorf("GetRow(0) = %q, want %q", result0, "First")
	}

	result2, err := page.GetRow(2)
	if err != nil {
		t.Fatalf("GetRow(2) after delete failed: %v", err)
	}
	if string(result2) != "Third" {
		t.Errorf("GetRow(2) = %q, want %q", result2, "Third")
	}
}

// =============================================================================
// Data and RowCount Tests
// =============================================================================

func TestData(t *testing.T) {
	page := NewSlottedPage()

	// データを取得
	data := page.Data()

	// サイズが pageSize であることを確認
	if len(data) != pageSize {
		t.Errorf("Data() length = %d, want %d", len(data), pageSize)
	}
}

func TestRowCount(t *testing.T) {
	page := NewSlottedPage()

	// 初期状態で 0
	if page.RowCount() != 0 {
		t.Errorf("NewSlottedPage().RowCount() = %d, want 0", page.RowCount())
	}

	// 行を挿入後
	_, _ = page.InsertRow([]byte("test"))
	if page.RowCount() != 1 {
		t.Errorf("After InsertRow, RowCount() = %d, want 1", page.RowCount())
	}
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestSlottedPageRoundTrip(t *testing.T) {
	// ページを作成してデータを追加
	page := NewSlottedPage()
	rows := [][]byte{
		[]byte("First row"),
		[]byte("Second row"),
		[]byte("Third row"),
	}

	for _, rowData := range rows {
		_, err := page.InsertRow(rowData)
		if err != nil {
			t.Fatalf("InsertRow failed: %v", err)
		}
	}

	// データを取得してリロード
	data := page.Data()
	loadedPage := LoadSlottedPage(data)

	// 同じ行数であることを確認
	if loadedPage.RowCount() != uint16(len(rows)) {
		t.Errorf("Loaded page RowCount() = %d, want %d", loadedPage.RowCount(), len(rows))
	}

	// 各行のデータが一致することを確認
	for i, expected := range rows {
		result, err := loadedPage.GetRow(uint16(i))
		if err != nil {
			t.Fatalf("GetRow(%d) from loaded page failed: %v", i, err)
		}
		if string(result) != string(expected) {
			t.Errorf("Loaded page GetRow(%d) = %q, want %q", i, result, expected)
		}
	}
}
