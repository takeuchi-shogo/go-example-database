/*
catalog.go はデータベースのカタログを管理する
スキーマの管理
既存の internal/storage パッケージとの橋渡し
*/
package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

type Catalog interface {
	// CreateTable はテーブルを作成する
	CreateTable(name string, schema *storage.Schema) error
	// GetTable はテーブルを取得する
	GetTable(name string) (*storage.Table, error)
	// DropTable はテーブルを削除する
	DropTable(name string) error
	// TableExists はテーブルが存在するかどうかを返す
	TableExists(name string) bool
	// ListTables はテーブルの一覧を返す
	ListTables() []*storage.Table
	// GetSchema はスキーマを取得する
	GetSchema(name string) (*storage.Schema, error)
	// Close はカタログを閉じる
	Close() error
}

// catalog はデータベースのカタログを管理する
type catalog struct {
	dataDir string
	tables  map[string]*storage.Table
	schemas map[string]*storage.Schema
	lock    sync.RWMutex
}

func NewCatalog(dataDir string) (Catalog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	return &catalog{
		dataDir: dataDir,
		tables:  make(map[string]*storage.Table),
		schemas: make(map[string]*storage.Schema),
	}, nil
}

// CreateTable はテーブルを作成する
func (c *catalog) CreateTable(name string, schema *storage.Schema) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// すでに存在しているか
	if _, ok := c.tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}
	// テーブル用のファイルパスを作成
	filePath := filepath.Join(c.dataDir, name+".db")
	// pager を作成
	pager, err := storage.NewPager(filePath)
	if err != nil {
		return err
	}
	// テーブルを作成
	table := storage.NewTable(storage.TableName(name), schema, pager)
	c.tables[name] = table
	// スキーマの追加
	c.schemas[name] = schema
	return nil
}

// GetTable はテーブルを取得する
func (c *catalog) GetTable(name string) (*storage.Table, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	table, ok := c.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return table, nil
}

// DropTable はテーブルを削除する
func (c *catalog) DropTable(name string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	table, ok := c.tables[name]
	if !ok {
		return fmt.Errorf("table %s not found", name)
	}
	if err := table.Close(); err != nil {
		return err
	}

	// テーブル用のファイルを削除
	filePath := filepath.Join(c.dataDir, name+".db")
	if err := os.Remove(filePath); err != nil {
		return err
	}
	delete(c.tables, name)
	delete(c.schemas, name)
	return nil
}

// TableExists はテーブルが存在するかどうかを返す
func (c *catalog) TableExists(name string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.tables[name] != nil
}

// ListTables はテーブルの一覧を返す
func (c *catalog) ListTables() []*storage.Table {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tables := make([]*storage.Table, 0, len(c.tables))
	for _, table := range c.tables {
		tables = append(tables, table)
	}
	return tables
}

// GetSchema はスキーマを取得する
func (c *catalog) GetSchema(name string) (*storage.Schema, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	schema, ok := c.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema %s not found", name)
	}
	return schema, nil
}

// Close はカタログを閉じる
func (c *catalog) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, table := range c.tables {
		if err := table.Close(); err != nil {
			return err
		}
	}
	return nil
}
