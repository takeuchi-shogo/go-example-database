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
	rowData := row.Encode()

	if t.numPages > 0 {
		page, err := t.getPage((t.numPages - 1).ToPageID())
		if err != nil {
			return err
		}
		_, err = page.InsertRow(rowData)
		if err == nil {
			// 挿入成功、ページを保存
			return t.savePage((t.numPages - 1).ToPageID(), page)
		}
		if err != ErrPageFull {
			// ErrPageFull 以外のエラー
			return err
		}
		// ErrPageFull の場合は新しいページを作成（下に続く）
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

func (t *Table) Scan() ([]*Row, error) {
	var rows []*Row
	for i := 0; i < int(t.numPages); i++ {
		page, err := t.getPage(PageID(i))
		if err != nil {
			return nil, err
		}
		for j := 0; j < int(page.rowCount()); j++ {
			rowData, err := page.GetRow(uint16(j))
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

// Close はテーブルを閉じる
func (t *Table) Close() error {
	return t.pager.Close()
}
