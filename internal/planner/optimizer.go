package planner

type Optimizer interface {
	Optimize(plan PlanNode) (PlanNode, error)
}

type optimizer struct {
	rules         []Rule
	costEstimator CostEstimator
}

func NewOptimizer(rules []Rule, costEstimator CostEstimator) Optimizer {
	return &optimizer{
		rules: []Rule{
			NewFilterPushDownRule(),
			NewConstantFoldingRule(),
		},
		costEstimator: costEstimator,
	}
}

func (o *optimizer) Optimize(plan PlanNode) (PlanNode, error) {
	// 1. 子ノードを再帰的に最適化
	optimized, err := o.optimizeChildren(plan)
	if err != nil {
		return nil, err
	}
	// 2. 各ルールを適用して最適化
	for _, rule := range o.rules {
		if rule.Match(optimized) {
			newPlan, err := rule.Apply(optimized)
			if err != nil {
				return nil, err
			}
			optimized = newPlan
		}
	}
	return optimized, nil
}

func (o *optimizer) optimizeChildren(plan PlanNode) (PlanNode, error) {
	switch n := plan.(type) {
	case *FilterNode:
		child, err := o.Optimize(n.Child)
		if err != nil {
			return nil, err
		}
		return &FilterNode{Condition: n.Condition, Child: child}, nil
	case *ProjectNode:
		child, err := o.Optimize(n.Child)
		if err != nil {
			return nil, err
		}
		return &ProjectNode{Columns: n.Columns, Child: child}, nil
	case *JoinNode:
		left, err := o.Optimize(n.Left)
		if err != nil {
			return nil, err
		}
		right, err := o.Optimize(n.Right)
		if err != nil {
			return nil, err
		}
		return &JoinNode{Left: left, Right: right, JoinType: n.JoinType, Condition: n.Condition}, nil
	case *AggregateNode:
		child, err := o.Optimize(n.Child)
		if err != nil {
			return nil, err
		}
		return &AggregateNode{GroupBy: n.GroupBy, Aggregates: n.Aggregates, Child: child}, nil
	case *InsertNode:
		return &InsertNode{TableName: n.TableName, Columns: n.Columns, Values: n.Values}, nil
	case *UpdateNode:
		child, err := o.Optimize(n.Child)
		if err != nil {
			return nil, err
		}
		return &UpdateNode{TableName: n.TableName, Sets: n.Sets, Child: child}, nil
	case *DeleteNode:
		child, err := o.Optimize(n.Child)
		if err != nil {
			return nil, err
		}
		return &DeleteNode{TableName: n.TableName, Child: child}, nil
	case *CreateTableNode:
		return &CreateTableNode{TableName: n.TableName, TableSchema: n.TableSchema}, nil
	case *ScanNode:
		return &ScanNode{TableName: n.TableName, TableSchema: n.TableSchema}, nil
	default:
		return plan, nil
	}
}
