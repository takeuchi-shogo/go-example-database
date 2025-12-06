package storage

const pageSize = 4096

type PageID int64

type Page struct {
	id   PageID
	data []byte
}

// NewPage creates a new Page with the given ID.
func NewPage(id PageID) *Page {
	return &Page{
		id:   id,
		data: make([]byte, pageSize),
	}
}

// GetOffset returns the offset of the Page in the file.
func (p Page) GetOffset() int64 {
	return int64(p.id) * pageSize
}
