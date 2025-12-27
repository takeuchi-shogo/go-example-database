package sharding

import (
	"sync"
	"testing"
)

// =============================================================================
// Range.Contains テスト
// =============================================================================

func TestRange_Contains(t *testing.T) {
	tests := []struct {
		name     string
		r        *Range
		key      string
		expected bool
	}{
		// 通常の Range ["m", "t")
		{
			name:     "key in range",
			r:        &Range{StartKey: "m", EndKey: "t"},
			key:      "mike",
			expected: true,
		},
		{
			name:     "key equals StartKey",
			r:        &Range{StartKey: "m", EndKey: "t"},
			key:      "m",
			expected: true,
		},
		{
			name:     "key before StartKey",
			r:        &Range{StartKey: "m", EndKey: "t"},
			key:      "alice",
			expected: false,
		},
		{
			name:     "key equals EndKey",
			r:        &Range{StartKey: "m", EndKey: "t"},
			key:      "t",
			expected: false,
		},
		{
			name:     "key after EndKey",
			r:        &Range{StartKey: "m", EndKey: "t"},
			key:      "zebra",
			expected: false,
		},
		// EndKey が空（無限大）の Range ["m", "")
		{
			name:     "unbounded range - key in range",
			r:        &Range{StartKey: "m", EndKey: ""},
			key:      "zebra",
			expected: true,
		},
		{
			name:     "unbounded range - key before StartKey",
			r:        &Range{StartKey: "m", EndKey: ""},
			key:      "alice",
			expected: false,
		},
		// 全範囲 ["", "")
		{
			name:     "full range - any key",
			r:        &Range{StartKey: "", EndKey: ""},
			key:      "anything",
			expected: true,
		},
		{
			name:     "full range - empty key",
			r:        &Range{StartKey: "", EndKey: ""},
			key:      "",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.r.Contains(tt.key)
			if got != tt.expected {
				t.Errorf("Contains(%q) = %v, want %v", tt.key, got, tt.expected)
			}
		})
	}
}

// =============================================================================
// NewRangeManager テスト
// =============================================================================

func TestNewRangeManager(t *testing.T) {
	rm := NewRangeManager("node1")

	if rm == nil {
		t.Fatal("NewRangeManager() returned nil")
	}

	ranges := rm.GetRanges()
	if len(ranges) != 1 {
		t.Fatalf("len(ranges) = %d, want 1", len(ranges))
	}

	r := ranges[0]
	if r.StartKey != "" {
		t.Errorf("StartKey = %q, want empty", r.StartKey)
	}
	if r.EndKey != "" {
		t.Errorf("EndKey = %q, want empty", r.EndKey)
	}
	if r.NodeID != "node1" {
		t.Errorf("NodeID = %q, want node1", r.NodeID)
	}
	if r.ID == "" {
		t.Error("ID should not be empty")
	}
}

// =============================================================================
// FindRange テスト
// =============================================================================

func TestRangeManager_FindRange_SingleRange(t *testing.T) {
	rm := NewRangeManager("node1")

	tests := []string{"", "a", "m", "z", "anything"}

	for _, key := range tests {
		r := rm.FindRange(key)
		if r == nil {
			t.Errorf("FindRange(%q) returned nil", key)
		} else if r.NodeID != "node1" {
			t.Errorf("FindRange(%q).NodeID = %q, want node1", key, r.NodeID)
		}
	}
}

func TestRangeManager_FindRange_MultipleRanges(t *testing.T) {
	rm := NewRangeManager("node1")

	// Split to create 3 ranges: ["","m"), ["m","t"), ["t","")
	original := rm.GetRanges()[0]
	_, _, err := rm.SplitRange(original.ID, "m", "node2")
	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}

	ranges := rm.GetRanges()
	rightRangeID := ""
	for _, r := range ranges {
		if r.StartKey == "m" {
			rightRangeID = r.ID
			break
		}
	}

	_, _, err = rm.SplitRange(rightRangeID, "t", "node3")
	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}

	// Now we have 3 ranges
	tests := []struct {
		key      string
		wantNode string
	}{
		{"", "node1"},
		{"alice", "node1"},
		{"lemon", "node1"},
		{"m", "node2"},
		{"mike", "node2"},
		{"sam", "node2"},
		{"t", "node3"},
		{"tom", "node3"},
		{"zebra", "node3"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			r := rm.FindRange(tt.key)
			if r == nil {
				t.Fatalf("FindRange(%q) returned nil", tt.key)
			}
			if r.NodeID != tt.wantNode {
				t.Errorf("FindRange(%q).NodeID = %q, want %q", tt.key, r.NodeID, tt.wantNode)
			}
		})
	}
}

// =============================================================================
// GetRangeByID テスト
// =============================================================================

func TestRangeManager_GetRangeByID(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// 存在する ID
	r := rm.GetRangeByID(original.ID)
	if r == nil {
		t.Error("GetRangeByID() returned nil for existing ID")
	}

	// 存在しない ID
	r = rm.GetRangeByID("non-existent")
	if r != nil {
		t.Error("GetRangeByID() should return nil for non-existent ID")
	}
}

// =============================================================================
// SplitRange テスト
// =============================================================================

func TestRangeManager_SplitRange(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	left, right, err := rm.SplitRange(original.ID, "m", "node2")

	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}

	// 左の Range
	if left.StartKey != "" {
		t.Errorf("left.StartKey = %q, want empty", left.StartKey)
	}
	if left.EndKey != "m" {
		t.Errorf("left.EndKey = %q, want m", left.EndKey)
	}
	if left.NodeID != "node1" {
		t.Errorf("left.NodeID = %q, want node1", left.NodeID)
	}
	if left.ID != original.ID {
		t.Errorf("left.ID should be same as original")
	}

	// 右の Range
	if right.StartKey != "m" {
		t.Errorf("right.StartKey = %q, want m", right.StartKey)
	}
	if right.EndKey != "" {
		t.Errorf("right.EndKey = %q, want empty", right.EndKey)
	}
	if right.NodeID != "node2" {
		t.Errorf("right.NodeID = %q, want node2", right.NodeID)
	}
	if right.ID == original.ID {
		t.Error("right.ID should be different from original")
	}

	// Range の数
	ranges := rm.GetRanges()
	if len(ranges) != 2 {
		t.Errorf("len(ranges) = %d, want 2", len(ranges))
	}
}

func TestRangeManager_SplitRange_NotFound(t *testing.T) {
	rm := NewRangeManager("node1")

	_, _, err := rm.SplitRange("non-existent", "m", "node2")

	if err != ErrRangeNotFound {
		t.Errorf("SplitRange() error = %v, want ErrRangeNotFound", err)
	}
}

func TestRangeManager_SplitRange_InvalidKey_BeforeStart(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split first to create ["", "m")
	rm.SplitRange(original.ID, "m", "node2")

	// Try to split with key before StartKey
	_, _, err := rm.SplitRange(original.ID, "", "node3")

	if err != ErrInvalidSplitKey {
		t.Errorf("SplitRange() error = %v, want ErrInvalidSplitKey", err)
	}
}

func TestRangeManager_SplitRange_InvalidKey_AfterEnd(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split first to create ["", "m")
	rm.SplitRange(original.ID, "m", "node2")

	// Try to split with key >= EndKey
	_, _, err := rm.SplitRange(original.ID, "m", "node3")

	if err != ErrInvalidSplitKey {
		t.Errorf("SplitRange() error = %v, want ErrInvalidSplitKey", err)
	}
}

func TestRangeManager_SplitRange_Multiple(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// First split: ["","") -> ["","m"), ["m","")
	_, right1, err := rm.SplitRange(original.ID, "m", "node2")
	if err != nil {
		t.Fatalf("First SplitRange() error = %v", err)
	}

	// Second split: ["m","") -> ["m","t"), ["t","")
	_, _, err = rm.SplitRange(right1.ID, "t", "node3")
	if err != nil {
		t.Fatalf("Second SplitRange() error = %v", err)
	}

	ranges := rm.GetRanges()
	if len(ranges) != 3 {
		t.Errorf("len(ranges) = %d, want 3", len(ranges))
	}

	// 各 Range の確認
	expected := []struct {
		startKey string
		endKey   string
		nodeID   string
	}{
		{"", "m", "node1"},
		{"m", "t", "node2"},
		{"t", "", "node3"},
	}

	for i, e := range expected {
		if i >= len(ranges) {
			break
		}
		r := ranges[i]
		if r.StartKey != e.startKey {
			t.Errorf("ranges[%d].StartKey = %q, want %q", i, r.StartKey, e.startKey)
		}
		if r.EndKey != e.endKey {
			t.Errorf("ranges[%d].EndKey = %q, want %q", i, r.EndKey, e.endKey)
		}
		if r.NodeID != e.nodeID {
			t.Errorf("ranges[%d].NodeID = %q, want %q", i, r.NodeID, e.nodeID)
		}
	}
}

// =============================================================================
// MergeRanges テスト
// =============================================================================

func TestRangeManager_MergeRanges(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Split first
	_, right, err := rm.SplitRange(original.ID, "m", "node2")
	if err != nil {
		t.Fatalf("SplitRange() error = %v", err)
	}

	// Merge back
	merged, err := rm.MergeRanges(original.ID, right.ID)
	if err != nil {
		t.Fatalf("MergeRanges() error = %v", err)
	}

	if merged.StartKey != "" {
		t.Errorf("merged.StartKey = %q, want empty", merged.StartKey)
	}
	if merged.EndKey != "" {
		t.Errorf("merged.EndKey = %q, want empty", merged.EndKey)
	}
	if merged.NodeID != "node1" {
		t.Errorf("merged.NodeID = %q, want node1", merged.NodeID)
	}

	ranges := rm.GetRanges()
	if len(ranges) != 1 {
		t.Errorf("len(ranges) = %d, want 1", len(ranges))
	}
}

func TestRangeManager_MergeRanges_NotFound(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	_, err := rm.MergeRanges(original.ID, "non-existent")

	if err != ErrRangeNotFound {
		t.Errorf("MergeRanges() error = %v, want ErrRangeNotFound", err)
	}
}

func TestRangeManager_MergeRanges_NotAdjacent(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	// Create 3 ranges
	_, right1, _ := rm.SplitRange(original.ID, "m", "node2")
	_, right2, _ := rm.SplitRange(right1.ID, "t", "node3")

	// Try to merge non-adjacent ranges (first and third)
	_, err := rm.MergeRanges(original.ID, right2.ID)

	if err != ErrNotAdjacent {
		t.Errorf("MergeRanges() error = %v, want ErrNotAdjacent", err)
	}
}

// =============================================================================
// UpdateNodeID テスト
// =============================================================================

func TestRangeManager_UpdateNodeID(t *testing.T) {
	rm := NewRangeManager("node1")
	original := rm.GetRanges()[0]

	err := rm.UpdateNodeID(original.ID, "node2")

	if err != nil {
		t.Fatalf("UpdateNodeID() error = %v", err)
	}

	r := rm.GetRangeByID(original.ID)
	if r.NodeID != "node2" {
		t.Errorf("NodeID = %q, want node2", r.NodeID)
	}
}

func TestRangeManager_UpdateNodeID_NotFound(t *testing.T) {
	rm := NewRangeManager("node1")

	err := rm.UpdateNodeID("non-existent", "node2")

	if err != ErrRangeNotFound {
		t.Errorf("UpdateNodeID() error = %v, want ErrRangeNotFound", err)
	}
}

// =============================================================================
// 並行性テスト
// =============================================================================

func TestRangeManager_Concurrent_FindRange(t *testing.T) {
	rm := NewRangeManager("node1")

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rm.FindRange("key")
			}
		}(i)
	}

	wg.Wait()
	// パニックしなければOK
}

func TestRangeManager_Concurrent_ReadWrite(t *testing.T) {
	rm := NewRangeManager("node1")

	var wg sync.WaitGroup

	// 読み取りゴルーチン
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rm.FindRange("key")
				rm.GetRanges()
			}
		}()
	}

	// 書き込みゴルーチン（UpdateNodeID）
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ranges := rm.GetRanges()
			if len(ranges) > 0 {
				for j := 0; j < 10; j++ {
					rm.UpdateNodeID(ranges[0].ID, "node-updated")
				}
			}
		}()
	}

	wg.Wait()
	// パニックしなければOK
}
