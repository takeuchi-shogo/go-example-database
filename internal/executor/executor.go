package executor

import (
	"fmt"

	internalcatalog "github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/planner"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

type Executor interface {
	Execute(plan planner.PlanNode) (ResultSet, error)
}

type executor struct {
	catalog internalcatalog.Catalog
}

func NewExecutor(c internalcatalog.Catalog) Executor {
	return &executor{catalog: c}
}

// Execute は PlanNode を実行して結果を返す
func (e *executor) Execute(plan planner.PlanNode) (ResultSet, error) {
	switch node := plan.(type) {
	case *planner.ScanNode:
		return e.executeScan(node)
	case *planner.FilterNode:
		return e.executeFilter(node)
	case *planner.ProjectNode:
		return e.executeProject(node)
	case *planner.InsertNode:
		return e.executeInsert(node)
	case *planner.UpdateNode:
		return e.executeUpdate(node)
	case *planner.DeleteNode:
		return e.executeDelete(node)
	case *planner.CreateTableNode:
		return e.executeCreateTable(node)
	case *planner.JoinNode:
		return e.executeJoin(node)
	case *planner.AggregateNode:
		return e.executeAggregate(node)
	default:
		return NewResultSetWithMessage(fmt.Sprintf("unsupported plan node type: %T", node)), nil
	}
}

func (e *executor) executeScan(node *planner.ScanNode) (ResultSet, error) {
	table, err := e.catalog.GetTable(node.TableName)
	if err != nil {
		return NewResultSetWithMessage(fmt.Sprintf("table not found: %s", node.TableName)), err
	}
	rows, err := table.Scan()
	if err != nil {
		return NewResultSetWithMessage(fmt.Sprintf("error scanning table: %s", err.Error())), err
	}
	return NewResultSetWithRowsAndSchema(node.TableSchema, rows), nil
}

func (e *executor) executeFilter(node *planner.FilterNode) (ResultSet, error) {
	// 子ノードを先に実行
	childResult, err := e.Execute(node.Child)
	if err != nil {
		return nil, err
	}
	// 子ノードの結果をフィルタリング
	filteredRows := make([]*storage.Row, 0)
	for _, row := range childResult.GetRows() {
		result, err := node.Condition.Evaluate(row, childResult.GetSchema())
		if err != nil {
			return nil, err
		}
		if match, ok := result.(bool); ok && match {
			filteredRows = append(filteredRows, row)
		}
	}
	return NewResultSetWithRowsAndSchema(childResult.GetSchema(), filteredRows), nil
}

func (e *executor) executeProject(node *planner.ProjectNode) (ResultSet, error) {
	// 子ノードを先に実行
	childResult, err := e.Execute(node.Child)
	if err != nil {
		return nil, err
	}
	// 指定されたカラムのインデックスを取得
	schema := childResult.GetSchema()
	columns := schema.GetColumns()
	// カラム名 -> インデックスのマップを作成
	columnIndexMap := make(map[string]int)
	for i, col := range columns {
		columnIndexMap[col.GetName()] = i
	}
	// 指定されたカラムのインデックスを取得
	projectedRows := make([]*storage.Row, 0)
	for _, row := range childResult.GetRows() {
		values := row.GetValues()
		projectedValues := make([]storage.Value, len(node.Columns))
		for i, col := range node.Columns {
			if index, ok := columnIndexMap[col]; ok {
				projectedValues[i] = values[index]
			}
		}
		projectedRows = append(projectedRows, storage.NewRow(projectedValues))
	}
	return NewResultSetWithRowsAndSchema(schema, projectedRows), nil
}

func (e *executor) executeInsert(node *planner.InsertNode) (ResultSet, error) {
	table, err := e.catalog.GetTable(node.TableName)
	if err != nil {
		return nil, err
	}
	// 値を評価して storage.Value に変換
	values := make([]storage.Value, len(node.Values))
	for i, value := range node.Values {
		evaluated, err := value.Evaluate(nil, nil)
		if err != nil {
			return nil, err
		}
		values[i], err = toStorageValue(evaluated)
		if err != nil {
			return nil, err
		}
	}
	row := storage.NewRow(values)
	err = table.Insert(row)
	if err != nil {
		return NewResultSetWithMessage(fmt.Sprintf("error inserting into table: %s", err.Error())), err
	}
	return NewResultSetWithMessage(fmt.Sprintf("row inserted: %s", node.TableName)), nil
}

// executeUpdate は UPDATE 文を実行して結果を返す
// TODO: 未実装
func (e *executor) executeUpdate(node *planner.UpdateNode) (ResultSet, error) {
	return NewResultSetWithMessage(fmt.Sprintf("update not implemented: %s", node.TableName)), nil
}

// executeDelete は DELETE 文を実行して結果を返す
// TODO: 未実装
func (e *executor) executeDelete(node *planner.DeleteNode) (ResultSet, error) {
	return NewResultSetWithMessage(fmt.Sprintf("delete not implemented: %s", node.TableName)), nil
}

// executeCreateTable は CREATE TABLE 文を実行して結果を返す
func (e *executor) executeCreateTable(node *planner.CreateTableNode) (ResultSet, error) {
	if err := e.catalog.CreateTable(node.TableName, node.TableSchema); err != nil {
		return NewResultSetWithMessage(fmt.Sprintf("error creating table: %s", err.Error())), err
	}
	return NewResultSetWithMessage(fmt.Sprintf("table created: %s", node.TableName)), nil
}

func toStorageValue(value any) (storage.Value, error) {
	switch v := value.(type) {
	case string:
		return storage.StringValue(v), nil
	case bool:
		return storage.BoolValue(v), nil
	case int:
		return storage.Int32Value(int32(v)), nil
	case int32:
		return storage.Int32Value(v), nil
	case int64:
		return storage.Int64Value(v), nil
	default:
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}
}

func (e *executor) executeJoin(node *planner.JoinNode) (ResultSet, error) {
	// 左テーブルを実行
	leftResult, err := e.Execute(node.Left)
	if err != nil {
		return nil, err
	}
	// 右テーブルを実行
	rightResult, err := e.Execute(node.Right)
	if err != nil {
		return nil, err
	}
	// 結合スキーマを作成
	joinSchema := node.Schema()
	// 結合条件を評価
	var joinedRows []*storage.Row
	for _, leftRow := range leftResult.GetRows() {
		for _, rightRow := range rightResult.GetRows() {
			// 左右の行を結合して結合条件を評価
			mergedRow := mergeRows(leftRow, rightRow)
			result, err := node.Condition.Evaluate(mergedRow, joinSchema)
			if err != nil {
				return nil, err
			}
			if match, ok := result.(bool); ok && match {
				joinedRows = append(joinedRows, mergedRow)
			}
		}
	}
	return NewResultSetWithRowsAndSchema(joinSchema, joinedRows), nil
}

// mergeRows は左右の行を結合して新しい行を作成する
func mergeRows(leftRow, rightRow *storage.Row) *storage.Row {
	leftValues := leftRow.GetValues()
	rightValues := rightRow.GetValues()
	mergedValues := make([]storage.Value, len(leftValues)+len(rightValues))
	copy(mergedValues, leftValues)
	copy(mergedValues[len(leftValues):], rightValues)
	return storage.NewRow(mergedValues)
}
