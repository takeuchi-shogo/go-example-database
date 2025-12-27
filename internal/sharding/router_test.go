package sharding

import (
	"testing"
)

// =============================================================================
// NewRouter テスト
// =============================================================================

func TestNewRouter(t *testing.T) {
	rm := NewRangeManager("node1")
	router := NewRouter(rm)

	if router == nil {
		t.Fatal("NewRouter() returned nil")
	}
	if router.rangeManager == nil {
		t.Error("rangeManager should not be nil")
	}
}

// =============================================================================
// GetNodeIDByRangeKey テスト
// =============================================================================

func TestRouter_GetNodeIDByRangeKey_SingleRange(t *testing.T) {
	rm := NewRangeManager("node1")
	router := NewRouter(rm)

	nodeID, err := router.GetNodeIDByRangeKey("anykey")
	if err != nil {
		t.Fatalf("GetNodeIDByRangeKey() error = %v", err)
	}
	if nodeID != "node1" {
		t.Errorf("nodeID = %q, want node1", nodeID)
	}
}

func TestRouter_GetNodeIDByRangeKey_MultipleRanges(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split to create: ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	tests := []struct {
		key      string
		wantNode string
	}{
		{"alice", "node1"},
		{"bob", "node1"},
		{"m", "node2"},
		{"mike", "node2"},
		{"sam", "node2"},
		{"t", "node3"},
		{"tom", "node3"},
		{"zebra", "node3"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			nodeID, err := router.GetNodeIDByRangeKey(tt.key)
			if err != nil {
				t.Fatalf("GetNodeIDByRangeKey(%q) error = %v", tt.key, err)
			}
			if nodeID != tt.wantNode {
				t.Errorf("GetNodeIDByRangeKey(%q) = %q, want %q", tt.key, nodeID, tt.wantNode)
			}
		})
	}
}

// =============================================================================
// GetRangeByRangeKey テスト
// =============================================================================

func TestRouter_GetRangeByRangeKey(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]
	rm.SplitRange(original.ID, "m", "node2")

	router := NewRouter(rm)

	// キー "alice" は ["","m") に含まれる
	rng, err := router.GetRangeByRangeKey("alice")
	if err != nil {
		t.Fatalf("GetRangeByRangeKey() error = %v", err)
	}
	if rng.StartKey != "" || rng.EndKey != "m" {
		t.Errorf("Range = [%q,%q), want [,m)", rng.StartKey, rng.EndKey)
	}

	// キー "mike" は ["m","") に含まれる
	rng, err = router.GetRangeByRangeKey("mike")
	if err != nil {
		t.Fatalf("GetRangeByRangeKey() error = %v", err)
	}
	if rng.StartKey != "m" || rng.EndKey != "" {
		t.Errorf("Range = [%q,%q), want [m,)", rng.StartKey, rng.EndKey)
	}
}

// =============================================================================
// RouteRange テスト
// =============================================================================

func TestRouter_RouteRange_SingleNode(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	// ["a", "f") は node1 のみ
	nodeIDs, err := router.RouteRange("a", "f")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 1 {
		t.Fatalf("len(nodeIDs) = %d, want 1", len(nodeIDs))
	}
	if nodeIDs[0] != "node1" {
		t.Errorf("nodeIDs[0] = %q, want node1", nodeIDs[0])
	}
}

func TestRouter_RouteRange_TwoNodes(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	// ["a", "p") は node1 と node2
	nodeIDs, err := router.RouteRange("a", "p")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 2 {
		t.Fatalf("len(nodeIDs) = %d, want 2", len(nodeIDs))
	}

	// 順序は保証されないので、存在チェック
	found := make(map[string]bool)
	for _, id := range nodeIDs {
		found[id] = true
	}
	if !found["node1"] || !found["node2"] {
		t.Errorf("nodeIDs = %v, want [node1, node2]", nodeIDs)
	}
}

func TestRouter_RouteRange_AllNodes(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	// ["", "") は全ノード
	nodeIDs, err := router.RouteRange("", "")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 3 {
		t.Fatalf("len(nodeIDs) = %d, want 3", len(nodeIDs))
	}
}

func TestRouter_RouteRange_ExactBoundary(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	// ["m", "t") は node2 のみ（境界ぴったり）
	nodeIDs, err := router.RouteRange("m", "t")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 1 {
		t.Fatalf("len(nodeIDs) = %d, want 1", len(nodeIDs))
	}
	if nodeIDs[0] != "node2" {
		t.Errorf("nodeIDs[0] = %q, want node2", nodeIDs[0])
	}
}

func TestRouter_RouteRange_UnboundedEnd(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","") → node2
	_, _, _ = rm.SplitRange(original.ID, "m", "node2")

	router := NewRouter(rm)

	// ["l", "") は node1 と node2
	nodeIDs, err := router.RouteRange("l", "")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 2 {
		t.Fatalf("len(nodeIDs) = %d, want 2", len(nodeIDs))
	}
}

// =============================================================================
// GetRangesByRange テスト
// =============================================================================

func TestRouter_GetRangesByRange(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node2, ["t","") → node3
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, _, _ = rm.SplitRange(right1.ID, "t", "node3")

	router := NewRouter(rm)

	// ["a", "s") は Range1 と Range2
	ranges := router.GetRangesByRange("a", "s")
	if len(ranges) != 2 {
		t.Fatalf("len(ranges) = %d, want 2", len(ranges))
	}
}

// =============================================================================
// overlaps テスト
// =============================================================================

func TestRouter_Overlaps(t *testing.T) {
	router := NewRouter(nil) // rangeManager は使わない

	tests := []struct {
		name     string
		rng      *Range
		startKey string
		endKey   string
		want     bool
	}{
		// 完全に内側
		{
			name:     "query inside range",
			rng:      &Range{StartKey: "a", EndKey: "z"},
			startKey: "m",
			endKey:   "n",
			want:     true,
		},
		// 完全に外側（前）
		{
			name:     "query before range",
			rng:      &Range{StartKey: "m", EndKey: "t"},
			startKey: "a",
			endKey:   "f",
			want:     false,
		},
		// 完全に外側（後）
		{
			name:     "query after range",
			rng:      &Range{StartKey: "a", EndKey: "f"},
			startKey: "m",
			endKey:   "t",
			want:     false,
		},
		// 部分的に重なる（左）
		{
			name:     "query overlaps left",
			rng:      &Range{StartKey: "m", EndKey: "t"},
			startKey: "a",
			endKey:   "p",
			want:     true,
		},
		// 部分的に重なる（右）
		{
			name:     "query overlaps right",
			rng:      &Range{StartKey: "a", EndKey: "m"},
			startKey: "f",
			endKey:   "t",
			want:     true,
		},
		// Range が Query を包含
		{
			name:     "range contains query",
			rng:      &Range{StartKey: "a", EndKey: "z"},
			startKey: "m",
			endKey:   "n",
			want:     true,
		},
		// Query が Range を包含
		{
			name:     "query contains range",
			rng:      &Range{StartKey: "m", EndKey: "n"},
			startKey: "a",
			endKey:   "z",
			want:     true,
		},
		// 境界ぴったり（重ならない）
		{
			name:     "boundary exact - no overlap",
			rng:      &Range{StartKey: "a", EndKey: "m"},
			startKey: "m",
			endKey:   "t",
			want:     false,
		},
		// Range が無限大（EndKey = ""）
		{
			name:     "range unbounded end",
			rng:      &Range{StartKey: "m", EndKey: ""},
			startKey: "t",
			endKey:   "z",
			want:     true,
		},
		// Query が無限大（endKey = ""）
		{
			name:     "query unbounded end",
			rng:      &Range{StartKey: "m", EndKey: "t"},
			startKey: "s",
			endKey:   "",
			want:     true,
		},
		// 両方とも無限大
		{
			name:     "both unbounded",
			rng:      &Range{StartKey: "m", EndKey: ""},
			startKey: "a",
			endKey:   "",
			want:     true,
		},
		// 全範囲 Range と全範囲 Query
		{
			name:     "full range and full query",
			rng:      &Range{StartKey: "", EndKey: ""},
			startKey: "",
			endKey:   "",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := router.overlaps(tt.rng, tt.startKey, tt.endKey)
			if got != tt.want {
				t.Errorf("overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// 同一ノードに複数 Range がある場合のテスト
// =============================================================================

func TestRouter_RouteRange_DuplicateNodeIDs(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// ["","m") → node1, ["m","t") → node1 (同じノード), ["t","") → node2
	_, right1, _ := rm.SplitRange(original.ID, "m", "node1") // 同じ node1
	_, _, _ = rm.SplitRange(right1.ID, "t", "node2")

	router := NewRouter(rm)

	// ["a", "s") は node1 の2つの Range にまたがるが、結果は重複除去される
	nodeIDs, err := router.RouteRange("a", "s")
	if err != nil {
		t.Fatalf("RouteRange() error = %v", err)
	}
	if len(nodeIDs) != 1 {
		t.Fatalf("len(nodeIDs) = %d, want 1 (duplicates removed)", len(nodeIDs))
	}
	if nodeIDs[0] != "node1" {
		t.Errorf("nodeIDs[0] = %q, want node1", nodeIDs[0])
	}
}
