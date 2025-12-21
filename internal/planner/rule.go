package planner

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/storage"
)

// Rule は最適化ルールのインターフェース
type Rule interface {
	// Name はルールの名前を返す
	Name() string
	// Match はルールが適用できるかどうかを判定する
	Match(plan PlanNode) bool
	// Apply はルールを適用して新しいプランを返す
	Apply(plan PlanNode) (PlanNode, error)
}

type rule struct {
	name    string
	pattern PlanNode
	action  func(plan PlanNode) (PlanNode, error)
}

func NewRule(name string, pattern PlanNode, action func(plan PlanNode) (PlanNode, error)) Rule {
	return &rule{name: name, pattern: pattern, action: action}
}

func (r *rule) Name() string {
	return r.name
}

func (r *rule) Match(plan PlanNode) bool {
	return r.pattern == plan
}

func (r *rule) Apply(plan PlanNode) (PlanNode, error) {
	return r.action(plan)
}

type FilterPushDownRule struct{}

func NewFilterPushDownRule() Rule {
	return &FilterPushDownRule{}
}

func (r *FilterPushDownRule) Name() string {
	return "filter"
}

func (r *FilterPushDownRule) Match(plan PlanNode) bool {
	filter, ok := plan.(*FilterNode)
	if !ok {
		return false
	}
	// 子が JoinNode の場合のみ適用
	_, isJoin := filter.Child.(*JoinNode)
	return isJoin
}

func (r *FilterPushDownRule) Apply(plan PlanNode) (PlanNode, error) {
	filterNode := plan.(*FilterNode)
	join := filterNode.Child.(*JoinNode)
	// 条件がどのテーブルのカラムを参照しているか分析
	referencedTables := r.getReferencedTables(filterNode.Condition)
	leftSchema := join.Left.Schema()
	// 左テーブルのカラムのみ参照している場合
	if r.allColumnsInSchema(referencedTables, leftSchema) {
		return &JoinNode{
			Left: &FilterNode{
				Condition: filterNode.Condition,
				Child:     join.Left,
			},
			Right:     join.Right,
			JoinType:  join.JoinType,
			Condition: join.Condition,
		}, nil
	}
	// 右テーブルのカラムのみ参照している場合
	rightSchema := join.Right.Schema()
	if r.allColumnsInSchema(referencedTables, rightSchema) {
		return &JoinNode{
			Left: join.Left,
			Right: &FilterNode{
				Condition: filterNode.Condition,
				Child:     join.Right,
			},
			JoinType:  join.JoinType,
			Condition: join.Condition,
		}, nil
	}
	// 両方のテーブルを参照している場合
	return plan, nil
}

func (r *FilterPushDownRule) getReferencedTables(expression Expression) []string {
	var columns []string
	r.collectColumnRefs(expression, &columns)
	return columns
}

func (r *FilterPushDownRule) collectColumnRefs(expression Expression, columns *[]string) {
	switch e := expression.(type) {
	case *BinaryExpr:
		r.collectColumnRefs(e.Left, columns)
		r.collectColumnRefs(e.Right, columns)
	case *ColumnRef:
		*columns = append(*columns, e.Name)
	}
}

func (r *FilterPushDownRule) allColumnsInSchema(columns []string, schema *storage.Schema) bool {
	for _, column := range columns {
		if schema.GetColumnIndex(column) < 0 {
			return false
		}
	}
	return true
}

type ConstantFoldingRule struct{}

func NewConstantFoldingRule() Rule {
	return &ConstantFoldingRule{}
}

func (r *ConstantFoldingRule) Name() string {
	return "constant folding"
}

func (r *ConstantFoldingRule) Match(plan PlanNode) bool {
	filter, ok := plan.(*FilterNode)
	if !ok {
		return false
	}
	return r.hasConstantExpression(filter.Condition)
}

func (r *ConstantFoldingRule) Apply(plan PlanNode) (PlanNode, error) {
	filterNode, ok := plan.(*FilterNode)
	if !ok {
		return nil, fmt.Errorf("plan is not a filter node: %T", plan)
	}
	// 条件式を評価して定数に置き換え
	foldedCondition := r.foldConstants(filterNode.Condition)
	// 条件が true なら FilterNode を削除
	if isAlwaysTrue(foldedCondition) {
		return filterNode.Child, nil
	}
	// 条件が false なら からの Node を返す
	if isAlwaysFalse(foldedCondition) {
		return NewEmptyNode(filterNode.Child.Schema()), nil
	}
	// それ以外はたたむ込んだ条件で node を返す
	return &FilterNode{Condition: foldedCondition, Child: filterNode.Child}, nil
}

func isAlwaysTrue(expression Expression) bool {
	literal, ok := expression.(*Literal)
	if !ok {
		return false
	}
	b, ok := literal.Value.(bool)
	return ok && b
}

func isAlwaysFalse(expression Expression) bool {
	literal, ok := expression.(*Literal)
	if !ok {
		return false
	}
	b, ok := literal.Value.(bool)
	return ok && !b
}

func (r *ConstantFoldingRule) foldConstants(expression Expression) Expression {
	switch e := expression.(type) {
	case *BinaryExpr:
		// 左右の式を再帰的に畳み込む（再帰）
		left := r.foldConstants(e.Left)
		right := r.foldConstants(e.Right)
		// 両方が literal なら畳み込める
		leftLiteral, leftOk := left.(*Literal)
		rightLiteral, rightOk := right.(*Literal)
		if leftOk && rightOk {
			// 定数式を評価
			result := r.evaluateConstantExpression(leftLiteral.Value, e.Operator, rightLiteral.Value)
			return &Literal{Value: result}
		}
		return &BinaryExpr{Left: left, Operator: e.Operator, Right: right}
	case *Literal:
		return expression
	default:
		return expression
	}
}

func (r *ConstantFoldingRule) evaluateConstantExpression(left any, operator string, right any) any {
	switch operator {
	case "+":
		return r.addValues(left, right)
	case "-":
		return r.subtractValues(left, right)
	case "*":
		return r.multiplyValues(left, right)
	case "/":
		return r.divideValues(left, right)
	case "=":
		return left == right
	case "!=", "<>":
		return left != right
	case "<":
		return compareValues(left, right) < 0
	case ">":
		return compareValues(left, right) > 0
	case "<=":
		return compareValues(left, right) <= 0
	case ">=":
		return compareValues(left, right) >= 0
	case "AND":
		leftBool, ok1 := left.(bool)
		rightBool, ok2 := right.(bool)
		if ok1 && ok2 {
			return leftBool && rightBool
		}
		return false
	case "OR":
		leftBool, ok1 := left.(bool)
		rightBool, ok2 := right.(bool)
		if ok1 && ok2 {
			return leftBool || rightBool
		}
		return false
	default:
		return false
	}
}

func (r *ConstantFoldingRule) addValues(left any, right any) any {
	switch left.(type) {
	case int:
		if r, ok := right.(int); ok {
			return left.(int) + r
		}
	case int64:
		if r, ok := right.(int64); ok {
			return left.(int64) + r
		}
	case string:
		if r, ok := right.(string); ok {
			return left.(string) + r
		}
	case float32:
		if r, ok := right.(float32); ok {
			return left.(float32) + r
		}
	case float64:
		if r, ok := right.(float64); ok {
			return left.(float64) + r
		}
	case nil:
		return right
	default:
		return fmt.Sprintf("%v + %v", left, right)
	}
	return nil
}

func (r *ConstantFoldingRule) subtractValues(left any, right any) any {
	switch left.(type) {
	case int:
		if r, ok := right.(int); ok {
			return left.(int) - r
		}
	case int64:
		if r, ok := right.(int64); ok {
			return left.(int64) - r
		}
	case float32:
		if r, ok := right.(float32); ok {
			return left.(float32) - r
		}
	case float64:
		if r, ok := right.(float64); ok {
			return left.(float64) - r
		}
	case nil:
		return right
	default:
		return fmt.Sprintf("%v - %v", left, right)
	}
	return nil
}

func (r *ConstantFoldingRule) multiplyValues(left any, right any) any {
	switch left.(type) {
	case int:
		if r, ok := right.(int); ok {
			return left.(int) * r
		}
	case int64:
		if r, ok := right.(int64); ok {
			return left.(int64) * r
		}
	case float32:
		if r, ok := right.(float32); ok {
			return left.(float32) * r
		}
	case float64:
		if r, ok := right.(float64); ok {
			return left.(float64) * r
		}
	}
	return nil
}

func (r *ConstantFoldingRule) divideValues(left any, right any) any {
	switch left.(type) {
	case int:
		if r, ok := right.(int); ok {
			if r == 0 {
				return nil
			}
			return left.(int) / r
		}
	case int64:
		if r, ok := right.(int64); ok {
			if r == 0 {
				return nil
			}
			return left.(int64) / r
		}
	case float32:
		if r, ok := right.(float32); ok {
			if r == 0 {
				return nil
			}
			return left.(float32) / r
		}
	case float64:
		if r, ok := right.(float64); ok {
			if r == 0 {
				return nil
			}
			return left.(float64) / r
		}
	case nil:
		return right
	default:
		return fmt.Sprintf("%v / %v", left, right)
	}
	return nil
}

func (r *ConstantFoldingRule) hasConstantExpression(expression Expression) bool {
	switch e := expression.(type) {
	case *BinaryExpr:
		// 両編が定数なら畳み込めるかも
		_, leftOk := e.Left.(*Literal)
		_, rightOk := e.Right.(*Literal)
		if leftOk && rightOk {
			return true
		}
		return r.hasConstantExpression(e.Left) ||
			r.hasConstantExpression(e.Right)
	}
	return false
}
