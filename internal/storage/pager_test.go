package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// Pager Tests
// =============================================================================

// TestNewPager tests that NewPager creates a Pager correctly.
func TestNewPager(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// NewPager を呼び出し（新規ファイル作成）
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// ファイルが作成されているか確認
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Errorf("NewPager(%s) did not create the file", testFile)
	}

	// numPages が 0 であることを確認（新規ファイルの場合）
	if pager.numPages != 0 {
		t.Errorf("NewPager(%s).numPages = %d, want 0", testFile, pager.numPages)
	}
}

// TestNewPagerWithExistingFile tests NewPager with an existing file containing pages.
func TestNewPagerWithExistingFile(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// 事前にファイルを作成し、2ページ分のデータを書き込む
	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 2ページ分のゼロデータを書き込む
	data := make([]byte, pageSize*2)
	if _, err := file.Write(data); err != nil {
		file.Close()
		t.Fatalf("Failed to write to test file: %v", err)
	}
	file.Close()

	// NewPager を呼び出し（既存ファイル）
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// numPages が 2 であることを確認
	if pager.numPages != 2 {
		t.Errorf("NewPager(%s).numPages = %d, want 2", testFile, pager.numPages)
	}
}

// TestNewPagerInvalidPath tests NewPager with an invalid directory path.
func TestNewPagerInvalidPath(t *testing.T) {
	// 存在しないディレクトリ内のファイルを指定
	invalidPath := "/nonexistent/directory/test.db"

	// NewPager を呼び出し（エラーが発生するはず）
	pager, err := NewPager(invalidPath)
	if err == nil {
		pager.Close()
		t.Error("NewPager with invalid path should return an error, but got nil")
	}
}

// =============================================================================
// ReadPage Tests
// =============================================================================

// TestReadPage tests that ReadPage reads a page correctly.
func TestReadPage(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// テストデータを作成して書き込み
	page := NewPage(0, make([]byte, pageSize))
	testData := []byte("Hello, Database!")
	copy(page.data, testData)
	if err := pager.WritePage(page); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// ReadPage を呼び出し
	readPage, err := pager.ReadPage(0, make([]byte, pageSize))
	if err != nil {
		t.Fatalf("ReadPage(0) failed: %v", err)
	}

	// 書き込んだデータと読み込んだデータが一致するか確認
	for i := 0; i < len(testData); i++ {
		if readPage.data[i] != testData[i] {
			t.Errorf("ReadPage data[%d] = %d, want %d", i, readPage.data[i], testData[i])
		}
	}
}

// TestReadPageFromEmptyFile tests ReadPage from an empty file.
func TestReadPageFromEmptyFile(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成（空のファイル）
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// 空のファイルからページを読み込もうとするとエラーになるはず
	_, err = pager.ReadPage(0, make([]byte, pageSize))
	if err == nil {
		t.Error("ReadPage from empty file should return an error, but got nil")
	}
}

// =============================================================================
// WritePage Tests
// =============================================================================

// TestWritePage tests that WritePage writes a page correctly.
func TestWritePage(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// テストデータを作成
	page := NewPage(0, make([]byte, pageSize))
	testData := []byte("Test data for WritePage")
	copy(page.data, testData)

	// WritePage を呼び出し
	if err := pager.WritePage(page); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}

	// ファイルサイズを確認（1ページ分のサイズがあるはず）
	fi, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("Failed to stat test file: %v", err)
	}

	expectedSize := int64(pageSize)
	if fi.Size() < expectedSize {
		t.Errorf("File size = %d, want at least %d", fi.Size(), expectedSize)
	}
}

// TestWriteAndReadMultiplePages tests writing and reading multiple pages.
func TestWriteAndReadMultiplePages(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// 複数のページを作成して書き込む
	numPages := 5
	testDataList := make([][]byte, numPages)

	for i := 0; i < numPages; i++ {
		page := NewPage(PageID(i), make([]byte, pageSize))
		// 各ページに異なるデータを設定
		testData := make([]byte, 100)
		for j := 0; j < 100; j++ {
			testData[j] = byte(i*100 + j)
		}
		copy(page.data, testData)
		testDataList[i] = testData

		// ページを書き込み
		if err := pager.WritePage(page); err != nil {
			t.Fatalf("WritePage(page %d) failed: %v", i, err)
		}
	}

	// 各ページを読み込んで検証
	for i := 0; i < numPages; i++ {
		readPage, err := pager.ReadPage(PageID(i), make([]byte, pageSize))
		if err != nil {
			t.Fatalf("ReadPage(%d) failed: %v", i, err)
		}

		// データが一致するか確認
		for j := 0; j < 100; j++ {
			if readPage.data[j] != testDataList[i][j] {
				t.Errorf("ReadPage(%d) data[%d] = %d, want %d", i, j, readPage.data[j], testDataList[i][j])
				break
			}
		}
	}
}

// =============================================================================
// Data Integrity Tests
// =============================================================================

// TestPageDataIntegrity tests that page data maintains integrity after multiple writes.
func TestPageDataIntegrity(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}
	defer pager.Close()

	// 同じページに複数回書き込む
	page := NewPage(0, make([]byte, pageSize))

	// 最初のデータを書き込み
	firstData := []byte("First write")
	copy(page.data, firstData)
	if err := pager.WritePage(page); err != nil {
		t.Fatalf("First WritePage failed: %v", err)
	}

	// 2回目のデータを書き込み（上書き）
	secondData := []byte("Second write - this is longer")
	// ページデータをクリア
	page.data = make([]byte, pageSize)
	copy(page.data, secondData)
	if err := pager.WritePage(page); err != nil {
		t.Fatalf("Second WritePage failed: %v", err)
	}

	// 読み込んで2回目のデータが正しく保存されているか確認
	readPage, err := pager.ReadPage(0, make([]byte, pageSize))
	if err != nil {
		t.Fatalf("ReadPage(0) failed: %v", err)
	}

	// 2回目のデータと一致するか確認
	for i := 0; i < len(secondData); i++ {
		if readPage.data[i] != secondData[i] {
			t.Errorf("ReadPage data[%d] = %d, want %d", i, readPage.data[i], secondData[i])
		}
	}
}

// =============================================================================
// Close Tests
// =============================================================================

// TestPagerClose tests that Close properly closes the file.
func TestPagerClose(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}

	// Close を呼び出し
	if err := pager.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// 閉じた後に書き込みを試みるとエラーになることを確認
	page := NewPage(0, make([]byte, pageSize))
	err = pager.WritePage(page)
	if err == nil {
		t.Error("WritePage after Close() should return an error, but got nil")
	}
}

// TestPagerCloseMultipleTimes tests that Close can be called only once safely.
func TestPagerCloseMultipleTimes(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.db")

	// Pager を作成
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager(%s) failed: %v", testFile, err)
	}

	// 1回目の Close
	if err := pager.Close(); err != nil {
		t.Fatalf("First Close() failed: %v", err)
	}

	// 2回目の Close はエラーになるはず
	err = pager.Close()
	if err == nil {
		t.Log("Note: Close() called twice did not return an error (implementation may vary)")
	}
}
