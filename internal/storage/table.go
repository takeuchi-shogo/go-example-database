package storage

import "errors"

var (
	ErrTableNotFound = errors.New("table not found")
	ErrTableFull     = errors.New("table is full")
	ErrRowNotFound   = errors.New("row not found")
)

type (
	TableName string
	NumPages  uint32
)

func (t TableName) String() string {
	return string(t)
}

func (n NumPages) ToPageID() PageID {
	return PageID(n)
}

// Table はテーブルを表す
type Table struct {
	name   TableName
	schema *Schema
	pager  *Pager
	// 現在のページ数
	numPages  NumPages
	nextRowID int64                 // 次の行ID
	rowIndex  map[int64]RowLocation // 行IDから行位置のインデックス
}

func NewTable(name TableName, schema *Schema, pager *Pager) *Table {
	t := &Table{
		name:      name,
		schema:    schema,
		pager:     pager,
		numPages:  NumPages(pager.GetNumPages()),
		nextRowID: 1,
		rowIndex:  make(map[int64]RowLocation),
	}
	// 既存のデータを読み込んでインデックスを再構築
	t.rebuildIndex()
	return t
}

// rebuildIndex はインデックスを再構築する
func (t *Table) rebuildIndex() error {
	t.rowIndex = make(map[int64]RowLocation)
	maxRowID := int64(0)
	for i := 0; i < int(t.numPages); i++ {
		page, err := t.getPage(PageID(i))
		if err != nil {
			return err
		}
		for j := 0; j < int(page.rowCount()); j++ {
			rowData, err := page.GetRow(uint16(j))
			if err == ErrSlotDeleted {
				continue
			}
			if err != nil {
				return err
			}
			row, err := DecodeRow(rowData, t.schema)
			if err != nil {
				return err
			}
			rowID := row.GetRowID()
			// インデックスに追加
			t.rowIndex[rowID] = RowLocation{
				PageID(i),
				int64(j),
			}
			// 最大行IDを更新
			if rowID > maxRowID {
				maxRowID = rowID
			}
		}
	}
	// 次の行IDを更新
	t.nextRowID = maxRowID + 1
	return nil
}

func (t *Table) GetName() TableName {
	return t.name
}

// GetRowCost はテーブルスキャン時の推定行数（コスト見積もり用）を返す。
// 現状は正確な統計情報を持っていないため、簡易的に全行をスキャンして数える。
// TODO: 将来的にはカタログに統計情報（行数、ヒストグラム等）を保持して高速化する。
func (t *Table) GetRowCost() int {
	rows, err := t.Scan()
	if err != nil {
		// コスト推定が失敗しても実行計画生成を完全に止めないため、0扱いにする。
		// （呼び出し側でエラーとして扱いたい場合はAPI設計を見直す）
		return 0
	}
	return len(rows)
}

func (t *Table) Insert(row *Row) error {
	// 行IDが指定されていない場合は、次の行IDを使用
	if row.GetRowID() == 0 {
		row.SetRowID(t.nextRowID)
		t.nextRowID++
	} else {
		// 行IDが指定されている場合は、その行IDが次の行IDより大きい場合は更新
		if row.GetRowID() >= t.nextRowID {
			t.nextRowID = row.GetRowID() + 1
		}
	}
	rowData := row.Encode()

	pageID := PageID(0)

	if t.numPages > 0 {
		page, err := t.getPage((t.numPages - 1).ToPageID())
		if err != nil {
			return err
		}
		slotID, err := page.InsertRow(rowData)
		if err == nil {
			pageID = (t.numPages - 1).ToPageID()
			if err := t.savePage(pageID, page); err != nil {
				return err
			}
			t.rowIndex[row.GetRowID()] = RowLocation{
				pageID: pageID,
				rowID:  int64(slotID),
			}
			return nil
		}
		if err != ErrPageFull {
			// ErrPageFull 以外のエラー
			return err
		}
		// ErrPageFull の場合は新しいページを作成（下に続く）
	}
	// 新しいページを作成
	page := NewSlottedPage()
	slotID, err := page.InsertRow(rowData)
	if err != nil {
		return err
	}

	// ページを保存
	pageID = t.numPages.ToPageID()
	t.numPages++
	if err := t.savePage(pageID, page); err != nil {
		return err
	}
	t.rowIndex[row.GetRowID()] = RowLocation{
		pageID: pageID,
		rowID:  int64(slotID),
	}
	return nil
}

func (t *Table) getPage(pageID PageID) (*SlottedPage, error) {
	page, err := t.pager.ReadPage(pageID, make([]byte, pageSize))
	if err != nil {
		return nil, err
	}
	// []byte を [pageSize]byte に変換
	var data [pageSize]byte
	copy(data[:], page.data)
	return LoadSlottedPage(data), nil
}

func (t *Table) savePage(pageID PageID, slotted *SlottedPage) error {
	// [pageSize]byte を []byte に変換
	data := slotted.Data()
	return t.pager.WritePage(NewPage(pageID, data[:]))
}

func (t *Table) Scan() ([]*Row, error) {
	var rows []*Row
	for i := 0; i < int(t.numPages); i++ {
		page, err := t.getPage(PageID(i))
		if err != nil {
			return nil, err
		}
		for j := 0; j < int(page.rowCount()); j++ {
			rowData, err := page.GetRow(uint16(j))
			if err == ErrSlotDeleted {
				continue
			}
			if err != nil {
				return nil, err
			}
			row, err := DecodeRow(rowData, t.schema)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// Update は行を更新する
func (t *Table) Update(rowID int64, row *Row) (*Row, error) {
	location, exists := t.rowIndex[rowID]
	if !exists {
		return nil, ErrRowNotFound
	}
	page, err := t.getPage(location.pageID)
	if err != nil {
		return nil, err
	}
	oldRowData, err := page.GetRow(uint16(location.rowID))
	if err != nil {
		return nil, err
	}
	oldRow, err := DecodeRow(oldRowData, t.schema)
	if err != nil {
		return nil, err
	}
	row.SetRowID(rowID)
	newData := row.Encode()
	// スロットを更新
	// 簡易実装: 削除 -> 再挿入
	if err := page.DeleteRow(uint16(location.rowID)); err != nil {
		return nil, err
	}
	// 同じページに再挿入
	newSlotID, err := page.InsertRow(newData)
	if err == ErrPageFull {
		if err := t.savePage(location.pageID, page); err != nil {
			return nil, err
		}
		// 新しいページを作成
		page = NewSlottedPage()
		newSlotID, err = page.InsertRow(newData)
		if err != nil {
			return nil, err
		}
		pageID := t.numPages.ToPageID()
		t.numPages++
		if err := t.savePage(pageID, page); err != nil {
			return nil, err
		}
		t.rowIndex[rowID] = RowLocation{
			pageID: pageID,
			rowID:  int64(newSlotID),
		}
	} else if err != nil {
		return nil, err
	} else {
		if err := t.savePage(location.pageID, page); err != nil {
			return nil, err
		}
		t.rowIndex[rowID] = RowLocation{
			pageID: location.pageID,
			rowID:  int64(newSlotID),
		}
	}
	return oldRow, nil
}

// Delete は行を削除する
func (t *Table) Delete(rowID int64) (*Row, error) {
	location, exists := t.rowIndex[rowID]
	if !exists {
		return nil, ErrRowNotFound
	}
	page, err := t.getPage(location.pageID)
	if err != nil {
		return nil, err
	}
	// 古いデータを取得
	oldRowData, err := page.GetRow(uint16(location.rowID))
	if err != nil {
		return nil, err
	}
	oldRow, err := DecodeRow(oldRowData, t.schema)
	if err != nil {
		return nil, err
	}
	// スロットを削除
	if err := page.DeleteRow(uint16(location.rowID)); err != nil {
		return nil, err
	}
	// ページを保存
	if err := t.savePage(location.pageID, page); err != nil {
		return nil, err
	}
	// インデックスを更新
	delete(t.rowIndex, rowID)
	return oldRow, nil
}

func (t *Table) FindByRowID(rowID int64) (*Row, error) {
	location, exists := t.rowIndex[rowID]
	if !exists {
		return nil, ErrRowNotFound
	}
	page, err := t.getPage(location.pageID)
	if err != nil {
		return nil, err
	}
	rowData, err := page.GetRow(uint16(location.rowID))
	if err != nil {
		return nil, err
	}
	return DecodeRow(rowData, t.schema)
}

// Close はテーブルを閉じる
func (t *Table) Close() error {
	return t.pager.Close()
}

type RowLocation struct {
	pageID PageID
	rowID  int64
}

func (r *RowLocation) GetPageID() PageID {
	return r.pageID
}

func (r *RowLocation) GetRowID() int64 {
	return r.rowID
}

// setter
func (r *RowLocation) SetPageID(pageID PageID) {
	r.pageID = pageID
}

func (r *RowLocation) SetRowID(rowID int64) {
	r.rowID = rowID
}
