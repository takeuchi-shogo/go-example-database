package index

import "errors"

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrDuplicateKey = errors.New("duplicate key")
)

type IndexType uint8

const (
	IndexTypePrimary   IndexType = 1 // プライマリキーインデックス
	IndexTypeUnique    IndexType = 2 // ユニークインデックス
	IndexTypeSecondary IndexType = 3 // セカンダリキーインデックス
)

var _ Index = (*index)(nil)

type Index interface{}

type index struct {
	name        string
	indexType   IndexType
	columnIndex int
	tree        *bTree
}

func NewIndex(name string, indexType IndexType, columnIndex int) Index {
	return &index{name: name, indexType: indexType, columnIndex: columnIndex, tree: NewBTree()}
}

// Insert はキーと行IDをインデックスに挿入する
// Key: カラムの値
// RowID: 行ID
func (i *index) Insert(key, rowID int64) error {
	// プライマリー、ユニークインデックスの場合：重複チェック
	if i.indexType == IndexTypePrimary || i.indexType == IndexTypeUnique {
		if _, ok := i.tree.Search(key); ok {
			return ErrDuplicateKey
		}
	}
	// 挿入
	i.tree.Insert(key, rowID)
	return nil
}

func (i *index) Find(key int64) (int64, error) {
	rowID, found := i.tree.Search(key)
	if !found {
		return -1, ErrKeyNotFound
	}
	return rowID, nil
}

func (i *index) GetName() string {
	return i.name
}

func (i *index) GetIndexType() IndexType {
	return i.indexType
}

func (i *index) GetColumnIndex() int {
	return i.columnIndex
}
