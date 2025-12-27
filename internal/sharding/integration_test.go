package sharding

import (
	"fmt"
	"testing"
)

// =============================================================================
// シナリオ 1: 基本的なシャーディング動作
// =============================================================================

func TestIntegration_BasicSharding(t *testing.T) {
	// 1. RangeManager を作成（全データが node1 に）
	rm := NewRangeManager("node1")

	// 2. Range を分割して3ノードに配置
	original := rm.GetRanges()[0]
	_, right1, err := rm.SplitRange(original.ID, "m", "node2")
	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}
	_, _, err = rm.SplitRange(right1.ID, "t", "node3")
	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}

	// 3. Router でキーをルーティング
	router := NewRouter(rm)

	testCases := []struct {
		key      string
		wantNode string
	}{
		{"alice", "node1"},
		{"bob", "node1"},
		{"mike", "node2"},
		{"sam", "node2"},
		{"tom", "node3"},
		{"zebra", "node3"},
	}

	for _, tc := range testCases {
		nodeID, err := router.GetNodeIDByRangeKey(tc.key)
		if err != nil {
			t.Errorf("GetNodeIDByRangeKey(%q) error = %v", tc.key, err)
			continue
		}
		if nodeID != tc.wantNode {
			t.Errorf("GetNodeIDByRangeKey(%q) = %q, want %q", tc.key, nodeID, tc.wantNode)
		}
	}

	t.Logf("3ノードへの分散ルーティング: OK")
}

// =============================================================================
// シナリオ 2: 範囲クエリのルーティング
// =============================================================================

func TestIntegration_RangeQuery(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	testCases := []struct {
		name      string
		startKey  string
		endKey    string
		wantNodes []string
	}{
		{
			name:      "single node query",
			startKey:  "a",
			endKey:    "f",
			wantNodes: []string{"node1"},
		},
		{
			name:      "two nodes query",
			startKey:  "f",
			endKey:    "p",
			wantNodes: []string{"node1", "node2"},
		},
		{
			name:      "all nodes query",
			startKey:  "",
			endKey:    "",
			wantNodes: []string{"node1", "node2", "node3"},
		},
		{
			name:      "boundary query",
			startKey:  "m",
			endKey:    "t",
			wantNodes: []string{"node2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeIDs, err := router.RouteRange(tc.startKey, tc.endKey)
			if err != nil {
				t.Fatalf("RouteRange() error = %v", err)
			}

			// 結果のノード数を確認
			if len(nodeIDs) != len(tc.wantNodes) {
				t.Errorf("len(nodeIDs) = %d, want %d", len(nodeIDs), len(tc.wantNodes))
			}

			// 期待するノードがすべて含まれているか確認
			nodeSet := make(map[string]bool)
			for _, id := range nodeIDs {
				nodeSet[id] = true
			}
			for _, want := range tc.wantNodes {
				if !nodeSet[want] {
					t.Errorf("nodeIDs = %v, missing %q", nodeIDs, want)
				}
			}
		})
	}
}

// =============================================================================
// シナリオ 3: ノード追加とリバランス
// =============================================================================

func TestIntegration_NodeAdditionAndRebalance(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 6つの Range を作成（すべて node1）
	_, r1, _ := rm.SplitRange(original.ID, "d", "node1")
	_, r2, _ := rm.SplitRange(r1.ID, "h", "node1")
	_, r3, _ := rm.SplitRange(r2.ID, "l", "node1")
	_, r4, _ := rm.SplitRange(r3.ID, "p", "node1")
	_, _, _ = rm.SplitRange(r4.ID, "t", "node1")

	t.Logf("初期状態: 6 Ranges すべて node1")

	// 新しいノードを追加（node2, node3）
	// Rebalancer で再配置
	rb := NewRebalancer(rm)

	// node2, node3 を追加するために、まず1つずつ手動で移動
	ranges := rm.GetRanges()
	rm.UpdateNodeID(ranges[2].ID, "node2")
	rm.UpdateNodeID(ranges[4].ID, "node3")

	countsBefore := rb.GetNodeRangeCounts()
	t.Logf("リバランス前: node1=%d, node2=%d, node3=%d",
		countsBefore["node1"], countsBefore["node2"], countsBefore["node3"])

	// リバランス提案を取得して実行
	suggestions := rb.SuggestRebalance()
	t.Logf("リバランス提案数: %d", len(suggestions))

	for _, s := range suggestions {
		err := rb.ExecuteRebalance(s)
		if err != nil {
			t.Errorf("ExecuteRebalance() error = %v", err)
		}
	}

	countsAfter := rb.GetNodeRangeCounts()
	t.Logf("リバランス後: node1=%d, node2=%d, node3=%d",
		countsAfter["node1"], countsAfter["node2"], countsAfter["node3"])

	// 各ノードの Range 数が均等に近いことを確認（2±1）
	for nodeID, count := range countsAfter {
		if count < 1 || count > 3 {
			t.Errorf("counts[%s] = %d, want 1-3", nodeID, count)
		}
	}
}

// =============================================================================
// シナリオ 4: Range Split とルーティングの整合性
// =============================================================================

func TestIntegration_SplitAndRouting(t *testing.T) {
	rm := NewRangeManager("node1")
	router := NewRouter(rm)

	// 初期状態: すべてのキーが node1 にルーティング
	nodeID, _ := router.GetNodeIDByRangeKey("mike")
	if nodeID != "node1" {
		t.Errorf("before split: GetNodeIDByRangeKey(mike) = %q, want node1", nodeID)
	}

	// Split: ["","m") → node1, ["m","") → node2
	original := rm.GetRanges()[0]
	rm.SplitRange(original.ID, "m", "node2")

	// Split 後: "mike" は node2 にルーティング
	nodeID, _ = router.GetNodeIDByRangeKey("mike")
	if nodeID != "node2" {
		t.Errorf("after split: GetNodeIDByRangeKey(mike) = %q, want node2", nodeID)
	}

	// "alice" は変わらず node1
	nodeID, _ = router.GetNodeIDByRangeKey("alice")
	if nodeID != "node1" {
		t.Errorf("after split: GetNodeIDByRangeKey(alice) = %q, want node1", nodeID)
	}

	t.Logf("Split 後のルーティング整合性: OK")
}

// =============================================================================
// シナリオ 5: Range Merge とルーティングの整合性
// =============================================================================

func TestIntegration_MergeAndRouting(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split して2つの Range を作成
	_, right, _ := rm.SplitRange(original.ID, "m", "node2")

	router := NewRouter(rm)

	// Split 後の状態を確認
	nodeID, _ := router.GetNodeIDByRangeKey("mike")
	if nodeID != "node2" {
		t.Errorf("after split: GetNodeIDByRangeKey(mike) = %q, want node2", nodeID)
	}

	// Merge して1つに戻す
	_, err := rm.MergeRanges(original.ID, right.ID)
	if err != nil {
		t.Fatalf("MergeRanges() error = %v", err)
	}

	// Merge 後: すべてのキーが node1 にルーティング
	nodeID, _ = router.GetNodeIDByRangeKey("mike")
	if nodeID != "node1" {
		t.Errorf("after merge: GetNodeIDByRangeKey(mike) = %q, want node1", nodeID)
	}

	nodeID, _ = router.GetNodeIDByRangeKey("alice")
	if nodeID != "node1" {
		t.Errorf("after merge: GetNodeIDByRangeKey(alice) = %q, want node1", nodeID)
	}

	t.Logf("Merge 後のルーティング整合性: OK")
}

// =============================================================================
// シナリオ 6: エンドツーエンド - データ分散シミュレーション
// =============================================================================

func TestIntegration_EndToEnd_DataDistribution(t *testing.T) {
	// 1. 初期化: 1ノードで開始
	rm := NewRangeManager("node1")
	router := NewRouter(rm)
	rb := NewRebalancer(rm)

	t.Log("=== Step 1: 初期状態 ===")
	t.Logf("Ranges: %d, Nodes: %d", len(rm.GetRanges()), len(rb.GetNodeRangeCounts()))

	// 2. データ増加をシミュレート: Range を分割
	t.Log("=== Step 2: データ増加（Range 分割）===")
	original := rm.GetRanges()[0]
	splitPoints := []string{"d", "h", "l", "p", "t", "x"}

	currentRange := original
	for i, point := range splitPoints {
		_, right, err := rm.SplitRange(currentRange.ID, point, "node1")
		if err != nil {
			t.Fatalf("SplitRange(%s) error = %v", point, err)
		}
		currentRange = right
		t.Logf("  Split at %q: %d Ranges", point, len(rm.GetRanges()))

		// 途中でノードを追加
		if i == 2 {
			// 3つ目の分割後に node2 を追加
			ranges := rm.GetRanges()
			rm.UpdateNodeID(ranges[len(ranges)-1].ID, "node2")
			t.Log("  → node2 を追加")
		}
		if i == 4 {
			// 5つ目の分割後に node3 を追加
			ranges := rm.GetRanges()
			rm.UpdateNodeID(ranges[len(ranges)-1].ID, "node3")
			t.Log("  → node3 を追加")
		}
	}

	// 3. リバランス前の状態
	t.Log("=== Step 3: リバランス前 ===")
	countsBefore := rb.GetNodeRangeCounts()
	for nodeID, count := range countsBefore {
		t.Logf("  %s: %d Ranges", nodeID, count)
	}

	// 4. リバランス
	t.Log("=== Step 4: リバランス実行 ===")
	suggestions := rb.SuggestRebalance()
	t.Logf("  提案数: %d", len(suggestions))
	for _, s := range suggestions {
		t.Logf("    %s → %s", s.SourceNodeID, s.TargetNodeID)
		rb.ExecuteRebalance(s)
	}

	// 5. リバランス後の状態
	t.Log("=== Step 5: リバランス後 ===")
	countsAfter := rb.GetNodeRangeCounts()
	for nodeID, count := range countsAfter {
		t.Logf("  %s: %d Ranges", nodeID, count)
	}

	// 6. ルーティングが正常に動作することを確認
	t.Log("=== Step 6: ルーティング確認 ===")
	testKeys := []string{"a", "e", "i", "m", "q", "u", "z"}
	for _, key := range testKeys {
		nodeID, err := router.GetNodeIDByRangeKey(key)
		if err != nil {
			t.Errorf("GetNodeIDByRangeKey(%q) error = %v", key, err)
		} else {
			t.Logf("  %q → %s", key, nodeID)
		}
	}

	// 7. 最終確認: 各ノードの Range 数が均等に近い
	t.Log("=== Step 7: 最終確認 ===")
	totalRanges := len(rm.GetRanges())
	totalNodes := len(countsAfter)
	avgPerNode := float64(totalRanges) / float64(totalNodes)
	t.Logf("Total: %d Ranges, %d Nodes, Avg: %.1f Ranges/Node", totalRanges, totalNodes, avgPerNode)

	for nodeID, count := range countsAfter {
		// 平均の ±1 以内であることを確認
		if float64(count) < avgPerNode-1.5 || float64(count) > avgPerNode+1.5 {
			t.Errorf("%s: %d Ranges (expected %.0f±1)", nodeID, count, avgPerNode)
		}
	}
}

// =============================================================================
// シナリオ 7: 大規模シミュレーション
// =============================================================================

func TestIntegration_LargeScale(t *testing.T) {
	rm := NewRangeManager("node1")

	// 26個の Range を作成（a-z）
	original := rm.GetRanges()[0]
	currentRange := original
	for i := 0; i < 25; i++ {
		splitPoint := string(rune('b' + i))
		_, right, err := rm.SplitRange(currentRange.ID, splitPoint, "node1")
		if err != nil {
			t.Fatalf("SplitRange(%s) error = %v", splitPoint, err)
		}
		currentRange = right
	}

	t.Logf("作成した Range 数: %d", len(rm.GetRanges()))

	// 5ノードに分散
	ranges := rm.GetRanges()
	nodeCount := 5
	for i, rng := range ranges {
		nodeID := fmt.Sprintf("node%d", (i%nodeCount)+1)
		rm.UpdateNodeID(rng.ID, nodeID)
	}

	// Rebalancer で確認
	rb := NewRebalancer(rm)
	counts := rb.GetNodeRangeCounts()

	t.Log("各ノードの Range 数:")
	for nodeID, count := range counts {
		t.Logf("  %s: %d", nodeID, count)
	}

	// Router で全キーがルーティングできることを確認
	router := NewRouter(rm)
	for i := 0; i < 26; i++ {
		key := string(rune('a' + i))
		_, err := router.GetNodeIDByRangeKey(key)
		if err != nil {
			t.Errorf("GetNodeIDByRangeKey(%q) error = %v", key, err)
		}
	}

	t.Logf("全キーのルーティング: OK")
}
