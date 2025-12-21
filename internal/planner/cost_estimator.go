package planner

import (
	"fmt"

	"github.com/takeuchi-shogo/go-example-database/internal/catalog"
)

type CostEstimator interface {
	EstimateCost(node PlanNode) (Cost, error)
}

type costEstimator struct {
	catalog catalog.Catalog
}

func NewCostEstimator(c catalog.Catalog) CostEstimator {
	return &costEstimator{catalog: c}
}

func (e *costEstimator) EstimateCost(node PlanNode) (Cost, error) {
	switch node := node.(type) {
	case *ScanNode:
		return e.estimateScanCost(node)
	case *FilterNode:
		return e.estimateFilterCost(node)
	case *ProjectNode:
		return e.estimateProjectCost(node)
	case *JoinNode:
		return e.estimateJoinCost(node)
	case *AggregateNode:
		return e.estimateAggregateCost(node)
	default:
		return nil, fmt.Errorf("unsupported plan node type: %T", node)
	}
}

// estimateScanCost はテーブルスキャンのコストを推定する
func (e *costEstimator) estimateScanCost(node *ScanNode) (Cost, error) {
	table, err := e.catalog.GetTable(node.TableName)
	if err != nil {
		return nil, err
	}
	rowCost := float64(table.GetRowCost())
	return NewCost(rowCost, 1, 1, 1), nil
}

// estimateFilterCost はフィルタのコストを推定する
func (e *costEstimator) estimateFilterCost(node *FilterNode) (Cost, error) {
	childCost, err := e.EstimateCost(node.Child)
	if err != nil {
		return nil, err
	}
	childCost.MultiplyRowCount(0.1) // フィルタのコストは子ノードのコストの10%
	return childCost, nil
}

// estimateProjectCost はプロジェクトのコストを推定する
func (e *costEstimator) estimateProjectCost(node *ProjectNode) (Cost, error) {
	return e.EstimateCost(node.Child)
}

// estimateJoinCost はJOINのコストを推定する
func (e *costEstimator) estimateJoinCost(node *JoinNode) (Cost, error) {
	leftCost, err := e.EstimateCost(node.Left)
	if err != nil {
		return nil, err
	}
	rightCost, err := e.EstimateCost(node.Right)
	if err != nil {
		return nil, err
	}
	return NewCost(leftCost.GetRowCost()*rightCost.GetRowCost(), 1, 1, 1), nil
}

// estimateAggregateCost は集約のコストを推定する
func (e *costEstimator) estimateAggregateCost(node *AggregateNode) (Cost, error) {
	if len(node.GroupBy) == 0 {
		// GROUP BY がなし場合は子ノードのコストを返す
		return e.EstimateCost(node.Child)
	}
	// GROUP BY がある場合は集約結果は1行になる想定
	childCost, err := e.EstimateCost(node.Child)
	if err != nil {
		return nil, err
	}
	return NewCost(childCost.GetRowCost(), 1, 1, 1), nil
}
