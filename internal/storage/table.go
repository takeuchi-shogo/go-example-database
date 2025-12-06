package storage

import "errors"

var (
	ErrTableNotFound = errors.New("table not found")
	ErrTableFull     = errors.New("table is full")
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
	numPages NumPages
}

func NewTable(name TableName, schema *Schema, pager *Pager) *Table {
	return &Table{name: name, schema: schema, pager: pager, numPages: NumPages(pager.GetNumPages())}
}

func (t *Table) GetName() TableName {
	return t.name
}

func (t *Table) Insert(row *Row) error {
	rowData := row.Encode()

	if t.numPages > 0 {
		page, err := t.getPage((t.numPages - 1).ToPageID())
		if err != nil {
			return err
		}
		_, err = page.InsertRow(rowData)
		// ErrPageFull なら新しいページへ
		if err != ErrPageFull {
			return err
		}
		if err == nil {
			// 入った！ページを保存
			return t.savePage((t.numPages - 1).ToPageID(), page)
		}
	}
	// 新しいページを作成
	page := NewSlottedPage()
	_, err := page.InsertRow(rowData)
	if err != nil {
		return err
	}

	// ページを保存
	pageID := t.numPages
	t.numPages++
	return t.savePage(pageID.ToPageID(), page)
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

// Close はテーブルを閉じる
func (t *Table) Close() error {
	return t.pager.Close()
}
