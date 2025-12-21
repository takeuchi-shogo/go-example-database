package planner

import (
	"os"
	"testing"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func setupTestCatalogWithData(t *testing.T) (catalog.Catalog, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "cost_estimator_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cat, err := catalog.NewCatalog(tempDir)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// テスト用テーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("name", storage.ColumnTypeString, 255, false),
	}
	schema := storage.NewSchema("users", columns)
	err = cat.CreateTable("users", schema)
	if err != nil {
		cat.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create table: %v", err)
	}

	// テストデータを挿入
	table, _ := cat.GetTable("users")
	for i := 0; i < 10; i++ {
		row := storage.NewRow([]storage.Value{
			storage.Int32Value(int32(i)),
			storage.StringValue("user"),
		})
		table.Insert(row)
	}

	cleanup := func() {
		cat.Close()
		os.RemoveAll(tempDir)
	}

	return cat, cleanup
}

func TestCostEstimatorScanNode(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	schema, _ := cat.GetSchema("users")
	scanNode := &ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	cost, err := estimator.EstimateCost(scanNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// 10行挿入したので、コストは10.0のはず
	if cost.GetRowCost() != 10.0 {
		t.Errorf("Expected RowCost 10.0, got %f", cost.GetRowCost())
	}
}

func TestCostEstimatorFilterNode(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	schema, _ := cat.GetSchema("users")
	scanNode := &ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	filterNode := &FilterNode{
		Child:     scanNode,
		Condition: nil, // 条件は省略
	}

	cost, err := estimator.EstimateCost(filterNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// フィルタは子ノードのコストの10%（選択率0.1）
	// 10 * 0.1 = 1.0
	expectedCost := 1.0
	if cost.GetRowCost() != expectedCost {
		t.Errorf("Expected RowCost %f, got %f", expectedCost, cost.GetRowCost())
	}
}

func TestCostEstimatorProjectNode(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	schema, _ := cat.GetSchema("users")
	scanNode := &ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	projectNode := &ProjectNode{
		Child:   scanNode,
		Columns: []string{"name"},
	}

	cost, err := estimator.EstimateCost(projectNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// ProjectNode は子ノードのコストをそのまま返す
	if cost.GetRowCost() != 10.0 {
		t.Errorf("Expected RowCost 10.0, got %f", cost.GetRowCost())
	}
}

func TestCostEstimatorJoinNode(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	// 2つ目のテーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
		*storage.NewColumn("user_id", storage.ColumnTypeInt32, 0, false),
	}
	ordersSchema := storage.NewSchema("orders", columns)
	cat.CreateTable("orders", ordersSchema)

	// ordersテーブルにデータを挿入
	ordersTable, _ := cat.GetTable("orders")
	for i := 0; i < 5; i++ {
		row := storage.NewRow([]storage.Value{
			storage.Int32Value(int32(i)),
			storage.Int32Value(int32(i % 3)),
		})
		ordersTable.Insert(row)
	}

	estimator := NewCostEstimator(cat)

	usersSchema, _ := cat.GetSchema("users")
	leftScan := &ScanNode{
		TableName:   "users",
		TableSchema: usersSchema,
	}

	rightScan := &ScanNode{
		TableName:   "orders",
		TableSchema: ordersSchema,
	}

	joinNode := &JoinNode{
		Left:      leftScan,
		Right:     rightScan,
		JoinType:  JoinTypeInner,
		Condition: nil,
	}

	cost, err := estimator.EstimateCost(joinNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// Nested Loop Join: 左テーブルの行数 × 右テーブルの行数
	// 10 * 5 = 50
	expectedCost := 50.0
	if cost.GetRowCost() != expectedCost {
		t.Errorf("Expected RowCost %f, got %f", expectedCost, cost.GetRowCost())
	}
}

func TestCostEstimatorAggregateNodeWithoutGroupBy(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	schema, _ := cat.GetSchema("users")
	scanNode := &ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	aggregateNode := &AggregateNode{
		Child:   scanNode,
		GroupBy: []string{}, // GROUP BY なし
		Aggregates: []AggregateExpression{
			{Function: "COUNT", Column: ""},
		},
	}

	cost, err := estimator.EstimateCost(aggregateNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// GROUP BY なしの場合は子ノードのコストをそのまま返す
	if cost.GetRowCost() != 10.0 {
		t.Errorf("Expected RowCost 10.0, got %f", cost.GetRowCost())
	}
}

func TestCostEstimatorAggregateNodeWithGroupBy(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	schema, _ := cat.GetSchema("users")
	scanNode := &ScanNode{
		TableName:   "users",
		TableSchema: schema,
	}

	aggregateNode := &AggregateNode{
		Child:   scanNode,
		GroupBy: []string{"name"}, // GROUP BY あり
		Aggregates: []AggregateExpression{
			{Function: "COUNT", Column: ""},
		},
	}

	cost, err := estimator.EstimateCost(aggregateNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// GROUP BY ありの場合も子ノードのコストを返す（現在の実装）
	if cost.GetRowCost() != 10.0 {
		t.Errorf("Expected RowCost 10.0, got %f", cost.GetRowCost())
	}
}

func TestCostEstimatorUnsupportedNode(t *testing.T) {
	cat, cleanup := setupTestCatalogWithData(t)
	defer cleanup()

	estimator := NewCostEstimator(cat)

	// InsertNode はコスト計算未対応
	insertNode := &InsertNode{
		TableName: "users",
	}

	_, err := estimator.EstimateCost(insertNode)
	if err == nil {
		t.Error("Expected error for unsupported node type")
	}
}

func TestCostEstimatorEmptyTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cost_estimator_empty_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cat, err := catalog.NewCatalog(tempDir)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer cat.Close()

	// 空のテーブルを作成
	columns := []storage.Column{
		*storage.NewColumn("id", storage.ColumnTypeInt32, 0, false),
	}
	schema := storage.NewSchema("empty", columns)
	cat.CreateTable("empty", schema)

	estimator := NewCostEstimator(cat)

	scanNode := &ScanNode{
		TableName:   "empty",
		TableSchema: schema,
	}

	cost, err := estimator.EstimateCost(scanNode)
	if err != nil {
		t.Fatalf("EstimateCost failed: %v", err)
	}

	// 空テーブルのコストは0
	if cost.GetRowCost() != 0.0 {
		t.Errorf("Expected RowCost 0.0, got %f", cost.GetRowCost())
	}
}
