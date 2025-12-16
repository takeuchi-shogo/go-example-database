package executor

import "github.com/takeuchi-shogo/go-example-database/internal/storage"

type Iterator interface {
	Next() (bool, error)
	GetRow() *storage.Row
	Reset()
	Close() error
}

type tableIterator struct {
	table *storage.Table
	rows  []*storage.Row
	index int
}

func NewTableIterator(table *storage.Table) Iterator {
	return &tableIterator{table: table, rows: make([]*storage.Row, 0)}
}

func (i *tableIterator) Next() (bool, error) {
	// すべての行を取得
	if len(i.rows) == 0 {
		rows, err := i.table.Scan()
		if err != nil {
			return false, err
		}
		i.rows = rows
		i.index = -1 // Next() で 0 になるように
	}
	i.index++
	return i.index < len(i.rows), nil
}

func (i *tableIterator) GetRow() *storage.Row {
	if i.index < 0 || i.index >= len(i.rows) {
		return nil
	}
	return i.rows[i.index]
}

func (i *tableIterator) Reset() {
	i.index = 0
}

func (i *tableIterator) Close() error {
	if err := i.table.Close(); err != nil {
		return err
	}
	return nil
}

type filterIterator struct {
	source    Iterator
	predicate func(row *storage.Row) bool
	current   *storage.Row
}

func NewFilterIterator(source Iterator, predicate func(row *storage.Row) bool) Iterator {
	return &filterIterator{source: source, predicate: predicate, current: nil}
}

func (i *filterIterator) Next() (bool, error) {
	for {
		hasNext, err := i.source.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			return false, nil
		}
		row := i.source.GetRow()
		if i.predicate(row) {
			i.current = row
			return true, nil
		}
	}
}

func (i *filterIterator) GetRow() *storage.Row {
	return i.current
}

func (i *filterIterator) Reset() {
	i.source.Reset()
	i.current = nil
}

func (i *filterIterator) Close() error {
	return i.source.Close()
}
