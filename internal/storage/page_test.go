package storage

import (
	"testing"
)

// =============================================================================
// Page Tests
// =============================================================================

// TestNewPage tests that NewPage creates a Page with the correct ID and data size.
func TestNewPage(t *testing.T) {
	// テストケースを定義
	testCases := []struct {
		name     string
		pageID   PageID
		expected PageID
	}{
		{
			name:     "PageID 0 の場合",
			pageID:   0,
			expected: 0,
		},
		{
			name:     "PageID 1 の場合",
			pageID:   1,
			expected: 1,
		},
		{
			name:     "PageID 100 の場合",
			pageID:   100,
			expected: 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// NewPage を呼び出し
			page := NewPage(tc.pageID, make([]byte, pageSize))

			// ID が正しく設定されているか確認
			if page.id != tc.expected {
				t.Errorf("NewPage(%d).id = %d, want %d", tc.pageID, page.id, tc.expected)
			}

			// data のサイズが pageSize と一致するか確認
			if len(page.data) != pageSize {
				t.Errorf("NewPage(%d).data length = %d, want %d", tc.pageID, len(page.data), pageSize)
			}

			// data がゼロ初期化されているか確認
			for i, b := range page.data {
				if b != 0 {
					t.Errorf("NewPage(%d).data[%d] = %d, want 0", tc.pageID, i, b)
					break
				}
			}
		})
	}
}

// TestGetOffset tests that GetOffset returns the correct offset for the Page.
func TestGetOffset(t *testing.T) {
	// テストケースを定義
	testCases := []struct {
		name           string
		pageID         PageID
		expectedOffset int64
	}{
		{
			name:           "PageID 0 のオフセット",
			pageID:         0,
			expectedOffset: 0,
		},
		{
			name:           "PageID 1 のオフセット",
			pageID:         1,
			expectedOffset: 4096, // 1 * pageSize
		},
		{
			name:           "PageID 2 のオフセット",
			pageID:         2,
			expectedOffset: 8192, // 2 * pageSize
		},
		{
			name:           "PageID 10 のオフセット",
			pageID:         10,
			expectedOffset: 40960, // 10 * pageSize
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Page を作成
			page := NewPage(tc.pageID, make([]byte, pageSize))

			// GetOffset を呼び出し
			offset := page.GetOffset()

			// 期待されるオフセットと一致するか確認
			if offset != tc.expectedOffset {
				t.Errorf("Page{id: %d}.GetOffset() = %d, want %d", tc.pageID, offset, tc.expectedOffset)
			}
		})
	}
}

// =============================================================================
// Page Constants Tests
// =============================================================================

// TestPageSize tests that pageSize constant is 4096 bytes.
func TestPageSize(t *testing.T) {
	// pageSize が 4096 であることを確認
	expectedSize := 4096
	if pageSize != expectedSize {
		t.Errorf("pageSize = %d, want %d", pageSize, expectedSize)
	}
}
