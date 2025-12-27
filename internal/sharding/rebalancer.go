package sharding

import "sort"

// Rebalancer - データの再配置を管理
type Rebalancer struct {
	rangeManager *RangeManager
}

type RebalanceSuggestion struct {
	RangeID      string
	SourceNodeID string
	TargetNodeID string
}

// NewRebalancer - リバランサーを作成
func NewRebalancer(rangeManager *RangeManager) *Rebalancer {
	return &Rebalancer{
		rangeManager: rangeManager,
	}
}

// GetNodeRangeCounts - 各ノードのシャード数を取得
func (r *Rebalancer) GetNodeRangeCounts() map[string]int {
	counts := make(map[string]int)
	for _, rng := range r.rangeManager.GetRanges() {
		counts[rng.NodeID]++
	}
	return counts
}

// GetRangesByNode - ノードに対応する Range 一覧を取得
func (r *Rebalancer) GetRangesByNode() map[string][]*Range {
	result := make(map[string][]*Range)
	for _, rng := range r.rangeManager.GetRanges() {
		result[rng.NodeID] = append(result[rng.NodeID], rng)
	}
	return result
}

// SuggestRebalance - データの再配置を提案
func (r *Rebalancer) SuggestRebalance() []RebalanceSuggestion {
	counts := r.GetNodeRangeCounts()
	if len(counts) == 0 {
		return nil
	}

	// NOTE:
	// map の反復順は不定なので、提案がテスト等で揺れないようノードIDをソートして扱う。
	nodeIDs := make([]string, 0, len(counts))
	for nodeID := range counts {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	rangesByNode := r.GetRangesByNode()
	suggestions := make([]RebalanceSuggestion, 0)

	// 最大と最小の差が 1 以下になるまで移動提案を生成する。
	for {
		// from: 最も Range が多いノード（同数なら nodeID 昇順で固定）
		// to:   最も Range が少ないノード（同数なら nodeID 昇順で固定）
		fromNodeID := nodeIDs[0]
		toNodeID := nodeIDs[0]
		maxCount := counts[fromNodeID]
		minCount := counts[toNodeID]

		for _, nodeID := range nodeIDs[1:] {
			count := counts[nodeID]

			if count > maxCount || (count == maxCount && nodeID < fromNodeID) {
				maxCount = count
				fromNodeID = nodeID
			}
			if count < minCount || (count == minCount && nodeID < toNodeID) {
				minCount = count
				toNodeID = nodeID
			}
		}

		// これ以上改善できない（既に十分バランス）なら終了
		if maxCount-minCount <= 1 {
			break
		}

		fromRanges := rangesByNode[fromNodeID]
		if len(fromRanges) == 0 {
			// counts と rangesByNode が不整合な場合の防御（通常ここには来ない）
			break
		}

		rangeToMove := fromRanges[len(fromRanges)-1]
		suggestions = append(suggestions, RebalanceSuggestion{
			RangeID:      rangeToMove.ID,
			SourceNodeID: fromNodeID,
			TargetNodeID: toNodeID,
		})

		// SuggestRebalance は状態を変更しない前提のため、Range 自体の NodeID は更新しない。
		// ただし次の提案生成に反映するため、内部の仮状態だけ更新する。
		rangesByNode[fromNodeID] = fromRanges[:len(fromRanges)-1]
		rangesByNode[toNodeID] = append(rangesByNode[toNodeID], rangeToMove)
		counts[fromNodeID]--
		counts[toNodeID]++
	}

	return suggestions
}

// ExecuteRebalance - データの再配置を実行
func (r *Rebalancer) ExecuteRebalance(suggestion RebalanceSuggestion) error {
	return r.rangeManager.UpdateNodeID(suggestion.RangeID, suggestion.TargetNodeID)
}
