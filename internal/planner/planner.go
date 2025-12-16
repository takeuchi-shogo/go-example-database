package planner

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
	"github.com/takeuchi-shogo/go-example-database/internal/parser"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

// Planner は AST を実行計画に変換するインターフェース
type Planner interface {
	Plan(statement parser.Statement) (PlanNode, error)
}

type planner struct {
	catalog catalog.Catalog
}

// NewPlanner は新しい Planner を作成する
func NewPlanner(c catalog.Catalog) Planner {
	return &planner{catalog: c}
}

// Plan は Statement を PlanNode に変換する
func (p *planner) Plan(statement parser.Statement) (PlanNode, error) {
	switch stmt := statement.(type) {
	case *parser.SelectStatement:
		return p.planSelect(stmt)
	case *parser.InsertStatement:
		return p.planInsert(stmt)
	case *parser.UpdateStatement:
		return p.planUpdate(stmt)
	case *parser.DeleteStatement:
		return p.planDelete(stmt)
	case *parser.CreateTableStatement:
		return p.planCreateTable(stmt)
	case *parser.ExplainStatement:
		return p.planExplain(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", statement)
	}
}

// planSelect は SELECT 文を PlanNode に変換する
func (p *planner) planSelect(stmt *parser.SelectStatement) (PlanNode, error) {
	// 1. テーブルスキャン
	schema, err := p.catalog.GetSchema(stmt.From)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", stmt.From)
	}

	var plan PlanNode = &ScanNode{
		TableName:   stmt.From,
		TableSchema: schema,
	}

	// 2. WHERE 句があればフィルタノードを追加
	if stmt.Where != nil {
		condition, err := p.planExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		plan = &FilterNode{
			Condition: condition,
			Child:     plan,
		}
	}

	// 3. SELECT 列が * でなければプロジェクションノードを追加
	if !isSelectAll(stmt.Columns) {
		columns := extractColumnNames(stmt.Columns)
		plan = &ProjectNode{
			Columns: columns,
			Child:   plan,
		}
	}

	return plan, nil
}

// planInsert は INSERT 文を PlanNode に変換する
func (p *planner) planInsert(stmt *parser.InsertStatement) (PlanNode, error) {
	// テーブルの存在確認
	if !p.catalog.TableExists(stmt.TableName) {
		return nil, fmt.Errorf("table not found: %s", stmt.TableName)
	}

	// 値の式を変換
	values := make([]Expression, len(stmt.Values))
	for i, v := range stmt.Values {
		expr, err := p.planExpression(v)
		if err != nil {
			return nil, err
		}
		values[i] = expr
	}

	return &InsertNode{
		TableName: stmt.TableName,
		Columns:   stmt.Columns,
		Values:    values,
	}, nil
}

// planUpdate は UPDATE 文を PlanNode に変換する
func (p *planner) planUpdate(stmt *parser.UpdateStatement) (PlanNode, error) {
	schema, err := p.catalog.GetSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", stmt.TableName)
	}

	// SET 式を変換
	sets := make(map[string]Expression)
	for col, expr := range stmt.SetExpressions {
		planExpr, err := p.planExpression(expr)
		if err != nil {
			return nil, err
		}
		sets[col] = planExpr
	}

	// 子ノード（WHERE 句があれば）
	var child PlanNode = &ScanNode{
		TableName:   stmt.TableName,
		TableSchema: schema,
	}

	if stmt.Where != nil {
		condition, err := p.planExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		child = &FilterNode{
			Condition: condition,
			Child:     child,
		}
	}

	return &UpdateNode{
		TableName: stmt.TableName,
		Sets:      sets,
		Child:     child,
	}, nil
}

// planDelete は DELETE 文を PlanNode に変換する
func (p *planner) planDelete(stmt *parser.DeleteStatement) (PlanNode, error) {
	schema, err := p.catalog.GetSchema(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table not found: %s", stmt.TableName)
	}

	// 子ノード
	var child PlanNode = &ScanNode{
		TableName:   stmt.TableName,
		TableSchema: schema,
	}

	if stmt.Where != nil {
		condition, err := p.planExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		child = &FilterNode{
			Condition: condition,
			Child:     child,
		}
	}

	return &DeleteNode{
		TableName: stmt.TableName,
		Child:     child,
	}, nil
}

// planCreateTable は CREATE TABLE 文を PlanNode に変換する
func (p *planner) planCreateTable(stmt *parser.CreateTableStatement) (PlanNode, error) {
	// カラム定義を storage.Column に変換
	columns := make([]storage.Column, len(stmt.Columns))
	for i, col := range stmt.Columns {
		colType := parseColumnType(col.ColumnType)
		columns[i] = *storage.NewColumn(col.Name, colType, 0, col.Nullable)
	}

	schema := storage.NewSchema(stmt.TableName, columns)

	return &CreateTableNode{
		TableName:   stmt.TableName,
		TableSchema: schema,
	}, nil
}

// planExplain は EXPLAIN 文を PlanNode に変換する
func (p *planner) planExplain(stmt *parser.ExplainStatement) (PlanNode, error) {
	// 内部の文をプランニングして返す（EXPLAIN 用の特別なノードは不要）
	return p.Plan(stmt.Statement)
}

// planExpression は parser.Expression を planner.Expression に変換する
func (p *planner) planExpression(expr parser.Expression) (Expression, error) {
	switch e := expr.(type) {
	case *parser.Identifier:
		return &ColumnRef{Name: e.Value}, nil

	case *parser.IntegerLiteral:
		return &Literal{Value: e.Value}, nil

	case *parser.StringLiteral:
		return &Literal{Value: e.Value}, nil

	case *parser.BooleanLiteral:
		return &Literal{Value: e.Value}, nil

	case *parser.BinaryExpression:
		left, err := p.planExpression(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.planExpression(e.Right)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:     left,
			Operator: e.Operator,
			Right:    right,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// isSelectAll は SELECT * かどうかを判定する
func isSelectAll(columns []parser.Expression) bool {
	if len(columns) == 1 {
		_, ok := columns[0].(*parser.Asterisk)
		return ok
	}
	return false
}

// extractColumnNames は SELECT 列からカラム名を抽出する
func extractColumnNames(columns []parser.Expression) []string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		if ident, ok := col.(*parser.Identifier); ok {
			names = append(names, ident.Value)
		}
	}
	return names
}

// parseColumnType は文字列を ColumnType に変換する
func parseColumnType(typeStr string) storage.ColumnType {
	switch typeStr {
	case "INT":
		return storage.ColumnTypeInt64
	case "BOOL":
		return storage.ColumnTypeBool
	case "TEXT":
		return storage.ColumnTypeString
	default:
		// VARCHAR(n) など
		if len(typeStr) >= 7 && typeStr[:7] == "VARCHAR" {
			return storage.ColumnTypeString
		}
		return storage.ColumnTypeString
	}
}
