package planner

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

// PlanNode は実行計画ノードのインターフェース
type PlanNode interface {
	// Schema はプランノードのスキーマを返す
	Schema() *storage.Schema
	// Children はプランノードの子ノードを返す
	Children() []PlanNode
	// String はデバック用の文字列を返す
	String() string
}

// ScanNode はテーブルスキャンを表す
type ScanNode struct {
	TableName   string
	TableSchema *storage.Schema
}

func (n *ScanNode) Schema() *storage.Schema { return n.TableSchema }
func (n *ScanNode) Children() []PlanNode    { return nil }
func (n *ScanNode) String() string          { return fmt.Sprintf("Scan(%s)", n.TableName) }

// FilterNode は WHERE 句を表す
type FilterNode struct {
	Condition Expression
	Child     PlanNode
}

func (n *FilterNode) Schema() *storage.Schema { return n.Child.Schema() }
func (n *FilterNode) Children() []PlanNode    { return []PlanNode{n.Child} }
func (n *FilterNode) String() string          { return fmt.Sprintf("Filter(%s)", n.Condition.String()) }

// ProjectNode は SELECT 列を表す
type ProjectNode struct {
	Columns []string
	Child   PlanNode
}

func (n *ProjectNode) Schema() *storage.Schema { return n.Child.Schema() } // TODO: 選択した列だけのスキーマを返す
func (n *ProjectNode) Children() []PlanNode    { return []PlanNode{n.Child} }
func (n *ProjectNode) String() string          { return fmt.Sprintf("Project(%v)", n.Columns) }

// InsertNode は INSERT 文を表す
type InsertNode struct {
	TableName string
	Columns   []string
	Values    []Expression
}

func (n *InsertNode) Schema() *storage.Schema { return nil }
func (n *InsertNode) Children() []PlanNode    { return nil }
func (n *InsertNode) String() string {
	return fmt.Sprintf("Insert(%s, %v)", n.TableName, n.Columns)
}

// UpdateNode は UPDATE 文を表す
type UpdateNode struct {
	TableName string
	Sets      map[string]Expression
	Child     PlanNode
}

func (n *UpdateNode) Schema() *storage.Schema { return nil }
func (n *UpdateNode) Children() []PlanNode {
	if n.Child != nil {
		return []PlanNode{n.Child}
	}
	return nil
}
func (n *UpdateNode) String() string { return fmt.Sprintf("Update(%s)", n.TableName) }

// DeleteNode は DELETE 文を表す
type DeleteNode struct {
	TableName string
	Child     PlanNode
}

func (n *DeleteNode) Schema() *storage.Schema { return nil }
func (n *DeleteNode) Children() []PlanNode {
	if n.Child != nil {
		return []PlanNode{n.Child}
	}
	return nil
}
func (n *DeleteNode) String() string { return fmt.Sprintf("Delete(%s)", n.TableName) }

// CreateTableNode は CREATE TABLE 文を表す
type CreateTableNode struct {
	TableName   string
	TableSchema *storage.Schema
}

func (n *CreateTableNode) Schema() *storage.Schema { return n.TableSchema }
func (n *CreateTableNode) Children() []PlanNode    { return nil }
func (n *CreateTableNode) String() string          { return fmt.Sprintf("CreateTable(%s)", n.TableName) }

// Expression は式を表す
type Expression interface {
	// Evaluate は式を評価する
	Evaluate(row *storage.Row, schema *storage.Schema) (any, error)
	// String はデバック用の文字列を返す
	String() string
}

// JoinNode は JOIN を表す
type JoinNode struct {
	Left      PlanNode   // 左テーブル
	Right     PlanNode   // 右テーブル
	JoinType  JoinType   // INNER, LEFT, RIGHT, FULL OUTER
	Condition Expression // 結合条件
}

type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
	JoinTypeFull  JoinType = "FULL OUTER"
)

func (n *JoinNode) Schema() *storage.Schema { return n.Left.Schema().Merge(n.Right.Schema()) }
func (n *JoinNode) Children() []PlanNode    { return []PlanNode{n.Left, n.Right} }
func (n *JoinNode) String() string {
	return fmt.Sprintf("Join(%s, %s)", n.Left.String(), n.Right.String())
}

// ColumnRef はカラム参照を表す
type ColumnRef struct {
	TableName string // テーブル名（修飾子、空の場合は未指定）
	Name      string // カラム名
}

func (e *ColumnRef) Evaluate(row *storage.Row, schema *storage.Schema) (any, error) {
	// カラム名からインデックスを取得
	columns := schema.GetColumns()
	for i, col := range columns {
		if col.GetName() == e.Name {
			values := row.GetValues()
			if i < len(values) {
				return extractValue(values[i]), nil
			}
			return nil, fmt.Errorf("column index out of range: %s", e.Name)
		}
	}
	return nil, fmt.Errorf("column not found: %s", e.Name)
}

func (e *ColumnRef) String() string {
	if e.TableName != "" {
		return e.TableName + "." + e.Name
	}
	return e.Name
}

// extractValue は storage.Value から Go の値を取り出す
func extractValue(v storage.Value) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case storage.StringValue:
		return string(val)
	case storage.BoolValue:
		return bool(val)
	case storage.Int32Value:
		return int(val)
	case storage.Int64Value:
		return int64(val)
	default:
		return v
	}
}

// Literal はリテラル値を表す
type Literal struct {
	Value any
}

func (e *Literal) Evaluate(row *storage.Row, schema *storage.Schema) (any, error) {
	return e.Value, nil
}

func (e *Literal) String() string {
	return fmt.Sprintf("%v", e.Value)
}

// BinaryExpr は二項演算を表す
type BinaryExpr struct {
	Left     Expression
	Operator string // =, <, >, <=, >=, !=, AND, OR
	Right    Expression
}

func (e *BinaryExpr) Evaluate(row *storage.Row, schema *storage.Schema) (any, error) {
	leftVal, err := e.Left.Evaluate(row, schema)
	if err != nil {
		return nil, err
	}
	rightVal, err := e.Right.Evaluate(row, schema)
	if err != nil {
		return nil, err
	}

	switch e.Operator {
	case "=":
		return leftVal == rightVal, nil
	case "!=", "<>":
		return leftVal != rightVal, nil
	case "<":
		return compareValues(leftVal, rightVal) < 0, nil
	case ">":
		return compareValues(leftVal, rightVal) > 0, nil
	case "<=":
		return compareValues(leftVal, rightVal) <= 0, nil
	case ">=":
		return compareValues(leftVal, rightVal) >= 0, nil
	case "AND":
		leftBool, ok1 := leftVal.(bool)
		rightBool, ok2 := rightVal.(bool)
		if ok1 && ok2 {
			return leftBool && rightBool, nil
		}
		return false, fmt.Errorf("AND requires boolean operands")
	case "OR":
		leftBool, ok1 := leftVal.(bool)
		rightBool, ok2 := rightVal.(bool)
		if ok1 && ok2 {
			return leftBool || rightBool, nil
		}
		return false, fmt.Errorf("OR requires boolean operands")
	default:
		return nil, fmt.Errorf("unknown operator: %s", e.Operator)
	}
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Operator, e.Right.String())
}

// compareValues は2つの値を比較する
func compareValues(left, right any) int {
	switch l := left.(type) {
	case int:
		if r, ok := right.(int); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
	case int64:
		if r, ok := right.(int64); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
	case string:
		if r, ok := right.(string); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
	}
	return 0
}

type AggregateNode struct {
	Child      PlanNode              // 子ノード
	GroupBy    []string              // GROUP BY 句
	Aggregates []AggregateExpression // 集約関数
	schema     *storage.Schema       // スキーマ
}

func (n *AggregateNode) Schema() *storage.Schema { return n.schema }
func (n *AggregateNode) Children() []PlanNode    { return []PlanNode{n.Child} }
func (n *AggregateNode) String() string {
	return fmt.Sprintf("Aggregate(%v, %v)", n.GroupBy, n.Aggregates)
}

type AggregateExpression struct {
	Function string // COUNT, SUM, AVG, MAX, MIN
	Column   string // カラム名
	Alias    string // AS のエイリアス
}
