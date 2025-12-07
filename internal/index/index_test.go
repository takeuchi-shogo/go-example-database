package index

import (
	"testing"
)

func TestNewIndex(t *testing.T) {
	idx := NewIndex("test_index", IndexTypePrimary, 0)

	if idx == nil {
		t.Fatal("NewIndex returned nil")
	}

	// 型アサーションで内部構造を確認
	i, ok := idx.(*index)
	if !ok {
		t.Fatal("expected *index type")
	}

	if i.name != "test_index" {
		t.Errorf("expected name 'test_index', got '%s'", i.name)
	}
	if i.indexType != IndexTypePrimary {
		t.Errorf("expected IndexTypePrimary, got %v", i.indexType)
	}
	if i.columnIndex != 0 {
		t.Errorf("expected columnIndex 0, got %d", i.columnIndex)
	}
	if i.tree == nil {
		t.Error("expected tree to be initialized")
	}
}

func TestIndex_GetName(t *testing.T) {
	idx := NewIndex("my_index", IndexTypeUnique, 1).(*index)

	if idx.GetName() != "my_index" {
		t.Errorf("expected 'my_index', got '%s'", idx.GetName())
	}
}

func TestIndex_GetIndexType(t *testing.T) {
	tests := []struct {
		indexType IndexType
		name      string
	}{
		{IndexTypePrimary, "primary"},
		{IndexTypeUnique, "unique"},
		{IndexTypeSecondary, "secondary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := NewIndex("test", tt.indexType, 0).(*index)
			if idx.GetIndexType() != tt.indexType {
				t.Errorf("expected %v, got %v", tt.indexType, idx.GetIndexType())
			}
		})
	}
}

func TestIndex_GetColumnIndex(t *testing.T) {
	idx := NewIndex("test", IndexTypePrimary, 5).(*index)

	if idx.GetColumnIndex() != 5 {
		t.Errorf("expected 5, got %d", idx.GetColumnIndex())
	}
}

func TestIndex_InsertAndFind(t *testing.T) {
	idx := NewIndex("test", IndexTypePrimary, 0).(*index)

	// 挿入
	err := idx.Insert(100, 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 検索
	rowID, err := idx.Find(100)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if rowID != 1 {
		t.Errorf("expected rowID 1, got %d", rowID)
	}
}

func TestIndex_FindNotFound(t *testing.T) {
	idx := NewIndex("test", IndexTypePrimary, 0).(*index)

	_, err := idx.Find(100)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestIndex_PrimaryKeyDuplicateCheck(t *testing.T) {
	idx := NewIndex("pk_test", IndexTypePrimary, 0).(*index)

	// 最初の挿入は成功
	err := idx.Insert(100, 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 同じキーで再度挿入は失敗
	err = idx.Insert(100, 2)
	if err != ErrDuplicateKey {
		t.Errorf("expected ErrDuplicateKey, got %v", err)
	}

	// 元の値が保持されていることを確認
	rowID, _ := idx.Find(100)
	if rowID != 1 {
		t.Errorf("expected original rowID 1, got %d", rowID)
	}
}

func TestIndex_UniqueKeyDuplicateCheck(t *testing.T) {
	idx := NewIndex("uk_test", IndexTypeUnique, 0).(*index)

	// 最初の挿入は成功
	err := idx.Insert(100, 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 同じキーで再度挿入は失敗
	err = idx.Insert(100, 2)
	if err != ErrDuplicateKey {
		t.Errorf("expected ErrDuplicateKey, got %v", err)
	}
}

func TestIndex_SecondaryKeyAllowsDuplicate(t *testing.T) {
	idx := NewIndex("sk_test", IndexTypeSecondary, 0).(*index)

	// 最初の挿入
	err := idx.Insert(100, 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// セカンダリインデックスは重複を許可
	// 注意：現在の実装ではB+Treeが上書きする可能性がある
	// これはセカンダリインデックスの完全な実装ではない
	err = idx.Insert(100, 2)
	if err != nil {
		t.Errorf("secondary index should allow duplicate keys, got error: %v", err)
	}
}

func TestIndex_InsertMultiple(t *testing.T) {
	idx := NewIndex("test", IndexTypePrimary, 0).(*index)

	testData := []struct {
		key   int64
		rowID int64
	}{
		{10, 1},
		{20, 2},
		{5, 3},
		{15, 4},
		{25, 5},
	}

	// 挿入
	for _, d := range testData {
		err := idx.Insert(d.key, d.rowID)
		if err != nil {
			t.Errorf("unexpected error inserting key %d: %v", d.key, err)
		}
	}

	// 検索
	for _, d := range testData {
		rowID, err := idx.Find(d.key)
		if err != nil {
			t.Errorf("unexpected error finding key %d: %v", d.key, err)
		}
		if rowID != d.rowID {
			t.Errorf("key %d: expected rowID %d, got %d", d.key, d.rowID, rowID)
		}
	}
}

func TestIndexType_Constants(t *testing.T) {
	// IndexType の定数が期待通りの値を持っていることを確認
	if IndexTypePrimary != 1 {
		t.Errorf("expected IndexTypePrimary=1, got %d", IndexTypePrimary)
	}
	if IndexTypeUnique != 2 {
		t.Errorf("expected IndexTypeUnique=2, got %d", IndexTypeUnique)
	}
	if IndexTypeSecondary != 3 {
		t.Errorf("expected IndexTypeSecondary=3, got %d", IndexTypeSecondary)
	}
}

func TestIndex_LargeDataset(t *testing.T) {
	idx := NewIndex("large_test", IndexTypePrimary, 0).(*index)

	n := int64(500)
	// 挿入
	for i := int64(1); i <= n; i++ {
		err := idx.Insert(i, i*10)
		if err != nil {
			t.Errorf("unexpected error inserting key %d: %v", i, err)
		}
	}

	// 検索
	for i := int64(1); i <= n; i++ {
		rowID, err := idx.Find(i)
		if err != nil {
			t.Errorf("unexpected error finding key %d: %v", i, err)
		}
		if rowID != i*10 {
			t.Errorf("key %d: expected rowID %d, got %d", i, i*10, rowID)
		}
	}

	// 存在しないキー
	_, err := idx.Find(n + 1)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}
