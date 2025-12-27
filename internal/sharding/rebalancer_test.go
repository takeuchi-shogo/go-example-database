package sharding

import (
	"testing"
)

// =============================================================================
// NewRebalancer テスト
// =============================================================================

func TestNewRebalancer(t *testing.T) {
	rm := NewRangeManager("node1")
	rb := NewRebalancer(rm)

	if rb == nil {
		t.Fatal("NewRebalancer() returned nil")
	}
	if rb.rangeManager == nil {
		t.Error("rangeManager should not be nil")
	}
}

// =============================================================================
// GetNodeRangeCounts テスト
// =============================================================================

func TestRebalancer_GetNodeRangeCounts_SingleNode(t *testing.T) {
	rm := NewRangeManager("node1")
	rb := NewRebalancer(rm)

	counts := rb.GetNodeRangeCounts()

	if len(counts) != 1 {
		t.Fatalf("len(counts) = %d, want 1", len(counts))
	}
	if counts["node1"] != 1 {
		t.Errorf("counts[node1] = %d, want 1", counts["node1"])
	}
}

func TestRebalancer_GetNodeRangeCounts_MultipleNodes(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split: ["","m") → node1, ["m","") → node2
	rm.SplitRange(original.ID, "m", "node2")

	rb := NewRebalancer(rm)
	counts := rb.GetNodeRangeCounts()

	if len(counts) != 2 {
		t.Fatalf("len(counts) = %d, want 2", len(counts))
	}
	if counts["node1"] != 1 {
		t.Errorf("counts[node1] = %d, want 1", counts["node1"])
	}
	if counts["node2"] != 1 {
		t.Errorf("counts[node2] = %d, want 1", counts["node2"])
	}
}

func TestRebalancer_GetNodeRangeCounts_UnbalancedNodes(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 4つの Range を作成、すべて node1
	_, r1, _ := rm.SplitRange(original.ID, "d", "node1")
	_, r2, _ := rm.SplitRange(r1.ID, "h", "node1")
	_, _, _ = rm.SplitRange(r2.ID, "m", "node1")

	rb := NewRebalancer(rm)
	counts := rb.GetNodeRangeCounts()

	if counts["node1"] != 4 {
		t.Errorf("counts[node1] = %d, want 4", counts["node1"])
	}
}

// =============================================================================
// GetRangesByNode テスト
// =============================================================================

func TestRebalancer_GetRangesByNode(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split: ["","m") → node1, ["m","t") → node2, ["t","") → node1
	_, r1, _ := rm.SplitRange(original.ID, "m", "node2")
	rm.SplitRange(r1.ID, "t", "node1")

	rb := NewRebalancer(rm)
	rangesByNode := rb.GetRangesByNode()

	if len(rangesByNode["node1"]) != 2 {
		t.Errorf("len(rangesByNode[node1]) = %d, want 2", len(rangesByNode["node1"]))
	}
	if len(rangesByNode["node2"]) != 1 {
		t.Errorf("len(rangesByNode[node2]) = %d, want 1", len(rangesByNode["node2"]))
	}
}

// =============================================================================
// SuggestRebalance テスト
// =============================================================================

func TestRebalancer_SuggestRebalance_AlreadyBalanced(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 3つの Range を3つのノードに均等配置
	_, r1, _ := rm.SplitRange(original.ID, "m", "node2")
	rm.SplitRange(r1.ID, "t", "node3")

	rb := NewRebalancer(rm)
	suggestions := rb.SuggestRebalance()

	// 既にバランス済みなので提案なし
	if len(suggestions) != 0 {
		t.Errorf("len(suggestions) = %d, want 0 (already balanced)", len(suggestions))
	}
}

func TestRebalancer_SuggestRebalance_Unbalanced(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 4つの Range を作成、すべて node1
	_, r1, _ := rm.SplitRange(original.ID, "d", "node1")
	_, r2, _ := rm.SplitRange(r1.ID, "h", "node1")
	_, _, _ = rm.SplitRange(r2.ID, "m", "node1")

	// node2 を追加（Range なし）
	// UpdateNodeID で1つを node2 に移動して、不均衡状態を作る
	ranges := rm.GetRanges()
	rm.UpdateNodeID(ranges[3].ID, "node2")

	// 現状: node1: 3, node2: 1
	rb := NewRebalancer(rm)
	counts := rb.GetNodeRangeCounts()
	t.Logf("Before: node1=%d, node2=%d", counts["node1"], counts["node2"])

	suggestions := rb.SuggestRebalance()
	t.Logf("Suggestions: %d", len(suggestions))

	// 4 Ranges, 2 nodes → 平均 2
	// node1: 3 > targetMax(2) なので移動提案あり
	// node2: 1 < targetMin(2) なので受け入れ可能
	if len(suggestions) < 1 {
		t.Errorf("len(suggestions) = %d, want >= 1", len(suggestions))
	}
}

func TestRebalancer_SuggestRebalance_SingleNode(t *testing.T) {
	rm := NewRangeManager("node1")
	rb := NewRebalancer(rm)

	suggestions := rb.SuggestRebalance()

	// 1ノードのみなので移動先がない
	if len(suggestions) != 0 {
		t.Errorf("len(suggestions) = %d, want 0 (single node)", len(suggestions))
	}
}

func TestRebalancer_SuggestRebalance_Empty(t *testing.T) {
	rm := &RangeManager{ranges: []*Range{}}
	rb := NewRebalancer(rm)

	suggestions := rb.SuggestRebalance()

	// Go では nil スライスでも len(nil) == 0 が保証されるため、nil チェックは不要。
	if len(suggestions) != 0 {
		t.Errorf("suggestions should be nil or empty for empty ranges")
	}
}

// =============================================================================
// ExecuteRebalance テスト
// =============================================================================

func TestRebalancer_ExecuteRebalance(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split して2つの Range を作成
	_, right, _ := rm.SplitRange(original.ID, "m", "node1")

	rb := NewRebalancer(rm)

	// 手動で移動提案を作成
	suggestion := RebalanceSuggestion{
		RangeID:      right.ID,
		SourceNodeID: "node1",
		TargetNodeID: "node2",
	}

	err := rb.ExecuteRebalance(suggestion)
	if err != nil {
		t.Fatalf("ExecuteRebalance() error = %v", err)
	}

	// 移動が実行されたか確認
	counts := rb.GetNodeRangeCounts()
	if counts["node1"] != 1 {
		t.Errorf("counts[node1] = %d, want 1", counts["node1"])
	}
	if counts["node2"] != 1 {
		t.Errorf("counts[node2] = %d, want 1", counts["node2"])
	}
}

func TestRebalancer_ExecuteRebalance_NotFound(t *testing.T) {
	rm := NewRangeManager("node1")
	rb := NewRebalancer(rm)

	suggestion := RebalanceSuggestion{
		RangeID:      "non-existent",
		SourceNodeID: "node1",
		TargetNodeID: "node2",
	}

	err := rb.ExecuteRebalance(suggestion)
	if err != ErrRangeNotFound {
		t.Errorf("ExecuteRebalance() error = %v, want ErrRangeNotFound", err)
	}
}

// =============================================================================
// 統合テスト
// =============================================================================

func TestRebalancer_FullRebalance(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 6つの Range を作成、すべて node1
	_, r1, _ := rm.SplitRange(original.ID, "c", "node1")
	_, r2, _ := rm.SplitRange(r1.ID, "f", "node1")
	_, r3, _ := rm.SplitRange(r2.ID, "i", "node1")
	_, r4, _ := rm.SplitRange(r3.ID, "l", "node1")
	_, _, _ = rm.SplitRange(r4.ID, "o", "node1")

	// 2つを node2 に、1つを node3 に移動して不均衡を作る
	ranges := rm.GetRanges()
	rm.UpdateNodeID(ranges[4].ID, "node2")
	rm.UpdateNodeID(ranges[5].ID, "node3")

	// 現状: node1: 4, node2: 1, node3: 1
	rb := NewRebalancer(rm)

	countsBefore := rb.GetNodeRangeCounts()
	t.Logf("Before: node1=%d, node2=%d, node3=%d",
		countsBefore["node1"], countsBefore["node2"], countsBefore["node3"])

	// リバランス提案を取得して実行
	suggestions := rb.SuggestRebalance()
	t.Logf("Suggestions count: %d", len(suggestions))

	for _, s := range suggestions {
		t.Logf("  Move %s: %s → %s", s.RangeID, s.SourceNodeID, s.TargetNodeID)
		rb.ExecuteRebalance(s)
	}

	countsAfter := rb.GetNodeRangeCounts()
	t.Logf("After: node1=%d, node2=%d, node3=%d",
		countsAfter["node1"], countsAfter["node2"], countsAfter["node3"])

	// 完全なバランスではなくても、改善されていることを確認
	// 6 Ranges, 3 nodes → 平均 2、各ノード 2±1 が目標
	for nodeID, count := range countsAfter {
		if count < 1 || count > 3 {
			t.Errorf("counts[%s] = %d, want 1-3", nodeID, count)
		}
	}
}
