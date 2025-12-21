package executor

import (
	"fmt"
	"strings"

	"github.com/takeuchi-shogo/go-example-database/internal/planner"
	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

func (e *executor) executeAggregate(node *planner.AggregateNode) (ResultSet, error) {
	// NOTE:
	// 集約（GROUP BY / COUNT / SUM ...）はまだ実装途中。
	// ここで未定義変数を返してコンパイルを壊さないよう、明示的に未実装として扱う。
	childResult, err := e.Execute(node.Child)
	if err != nil {
		return nil, err
	}
	rows := childResult.GetRows()

	// GROUP BY がない場合、全行を1グループとして集約
	if len(node.GroupBy) == 0 {
		values := make([]storage.Value, len(node.Aggregates))
		for i, agg := range node.Aggregates {
			result, err := e.calculateAggregate(agg, rows, childResult.GetSchema())
			if err != nil {
				return nil, err
			}
			values[i] = result
		}
		resultRow := storage.NewRow(values)
		return NewResultSetWithRowsAndSchema(node.Schema(), []*storage.Row{resultRow}), nil
	}
	// TODO: GROUP BY 句を評価
	return NewResultSetWithMessage("group by not implemented"), nil
}

func (e *executor) aggreateAll(rows []*storage.Row, node *planner.AggregateNode) (ResultSet, error) {
	values := make([]storage.Value, len(node.Aggregates))

	for i, agg := range node.Aggregates {
		result, err := e.calculateAggregate(agg, rows, node.Child.Schema())
		if err != nil {
			return nil, err
		}
		values[i] = result
	}

	resultRow := storage.NewRow(values)
	return NewResultSetWithRowsAndSchema(node.Schema(), []*storage.Row{resultRow}), nil
}

func (e *executor) calculateAggregate(agg planner.AggregateExpression, rows []*storage.Row, schema *storage.Schema) (storage.Value, error) {
	funcName := strings.ToUpper(agg.Function)
	switch funcName {
	case "COUNT":
		return storage.Int64Value(int64(len(rows))), nil
	case "SUM":
		var sum int64
		colIdx := schema.GetColumnIndex(agg.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column not found: %s", agg.Column)
		}
		for _, row := range rows {
			val := row.GetValues()[colIdx]
			sum += int64(val.(storage.Int32Value))
		}
		return storage.Int64Value(sum), nil
	case "AVG":
		if len(rows) == 0 {
			return nil, fmt.Errorf("no rows to calculate average")
		}
		var sum int64
		colIdx := schema.GetColumnIndex(agg.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column not found: %s", agg.Column)
		}
		for _, row := range rows {
			val := row.GetValues()[colIdx]
			sum += int64(val.(storage.Int32Value))
		}
		return storage.Int64Value(sum / int64(len(rows))), nil
	case "MAX":
		if len(rows) == 0 {
			return nil, fmt.Errorf("no rows to calculate max")
		}
		colIdx := schema.GetColumnIndex(agg.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column not found: %s", agg.Column)
		}
		max := int64(rows[0].GetValues()[colIdx].(storage.Int32Value))
		for _, row := range rows[1:] {
			val := int64(row.GetValues()[colIdx].(storage.Int32Value))
			if val > max {
				max = val
			}
		}
		return storage.Int64Value(max), nil
	case "MIN":
		if len(rows) == 0 {
			return nil, fmt.Errorf("no rows to calculate min")
		}
		colIdx := schema.GetColumnIndex(agg.Column)
		if colIdx < 0 {
			return nil, fmt.Errorf("column not found: %s", agg.Column)
		}
		min := int64(rows[0].GetValues()[colIdx].(storage.Int32Value))
		for _, row := range rows[1:] {
			val := int64(row.GetValues()[colIdx].(storage.Int32Value))
			if val < min {
				min = val
			}
		}
		return storage.Int64Value(min), nil
	}
	return nil, fmt.Errorf("unsupported aggregate function: %s", agg.Function)
}
