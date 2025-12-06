package storage

import (
	"errors"
	"os"
)

var ErrInvalidPageID = errors.New("invalid page ID")

type Pager struct {
	file     *os.File
	numPages uint32
}

// NewPager creates a new Pager for the given file.
func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	numPages := uint32(fi.Size() / pageSize)

	return &Pager{
		file:     file,
		numPages: numPages,
	}, nil
}

// ReadPage reads the Page with the given ID from the file.
func (p *Pager) ReadPage(id PageID) (*Page, error) {
	page := NewPage(id)

	offset := page.GetOffset()

	_, err := p.file.ReadAt(page.data, offset)
	if err != nil {
		return nil, err
	}

	return page, nil
}

// WritePage writes the Page to the file.
func (p *Pager) WritePage(page *Page) error {
	offset := page.GetOffset()

	_, err := p.file.WriteAt(page.data, offset)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the Pager and the underlying file.
func (p *Pager) Close() error {
	return p.file.Close()
}
