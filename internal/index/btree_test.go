package index

import (
	"testing"
)

func TestNewBTree(t *testing.T) {
	tree := NewBTree()

	if tree == nil {
		t.Fatal("NewBTree returned nil")
	}
	if tree.root == nil {
		t.Fatal("root is nil")
	}
	if tree.root.nodeType != nodeTypeLeaf {
		t.Errorf("expected root to be leaf node, got %v", tree.root.nodeType)
	}
	if len(tree.root.keys) != 0 {
		t.Errorf("expected empty keys, got %d", len(tree.root.keys))
	}
	if len(tree.root.values) != 0 {
		t.Errorf("expected empty values, got %d", len(tree.root.values))
	}
}

func TestBTree_SearchEmpty(t *testing.T) {
	tree := NewBTree()

	_, found := tree.Search(100)
	if found {
		t.Error("expected not found in empty tree")
	}
}

func TestBTree_InsertAndSearch(t *testing.T) {
	tree := NewBTree()

	// 単一の挿入
	tree.Insert(10, 100)

	value, found := tree.Search(10)
	if !found {
		t.Error("expected to find key 10")
	}
	if value != 100 {
		t.Errorf("expected value 100, got %d", value)
	}

	// 存在しないキーの検索
	_, found = tree.Search(20)
	if found {
		t.Error("expected not to find key 20")
	}
}

func TestBTree_InsertMultiple(t *testing.T) {
	tree := NewBTree()

	testData := []struct {
		key   int64
		value int64
	}{
		{10, 100},
		{20, 200},
		{5, 50},
		{15, 150},
		{25, 250},
	}

	// 挿入
	for _, d := range testData {
		tree.Insert(d.key, d.value)
	}

	// 検索
	for _, d := range testData {
		value, found := tree.Search(d.key)
		if !found {
			t.Errorf("expected to find key %d", d.key)
		}
		if value != d.value {
			t.Errorf("key %d: expected value %d, got %d", d.key, d.value, value)
		}
	}
}

func TestBTree_InsertCausesSplit(t *testing.T) {
	tree := NewBTree()

	// order=4 なので maxKeys=3
	// 4つ以上挿入すると分割が発生する
	for i := int64(1); i <= 10; i++ {
		tree.Insert(i*10, i*100)
	}

	// すべてのキーが検索できることを確認
	for i := int64(1); i <= 10; i++ {
		value, found := tree.Search(i * 10)
		if !found {
			t.Errorf("expected to find key %d", i*10)
		}
		if value != i*100 {
			t.Errorf("key %d: expected value %d, got %d", i*10, i*100, value)
		}
	}
}

func TestBTree_InsertReverseOrder(t *testing.T) {
	tree := NewBTree()

	// 逆順で挿入
	for i := int64(10); i >= 1; i-- {
		tree.Insert(i*10, i*100)
	}

	// すべてのキーが検索できることを確認
	for i := int64(1); i <= 10; i++ {
		value, found := tree.Search(i * 10)
		if !found {
			t.Errorf("expected to find key %d", i*10)
		}
		if value != i*100 {
			t.Errorf("key %d: expected value %d, got %d", i*10, i*100, value)
		}
	}
}

func TestBTree_InsertRandomOrder(t *testing.T) {
	tree := NewBTree()

	// ランダムな順序で挿入
	keys := []int64{50, 20, 80, 10, 30, 60, 90, 5, 15, 25, 35, 55, 65, 85, 95}
	for _, key := range keys {
		tree.Insert(key, key*10)
	}

	// すべてのキーが検索できることを確認
	for _, key := range keys {
		value, found := tree.Search(key)
		if !found {
			t.Errorf("expected to find key %d", key)
		}
		if value != key*10 {
			t.Errorf("key %d: expected value %d, got %d", key, key*10, value)
		}
	}
}

func TestBTreeNode_findKeyIndex(t *testing.T) {
	node := &bTreeNode{
		keys: []int64{10, 20, 30},
	}

	tests := []struct {
		key      int64
		expected int
	}{
		{5, 0},   // 10より小さい
		{10, 0},  // 最初のキーと一致
		{15, 1},  // 10と20の間
		{20, 1},  // 2番目のキーと一致
		{25, 2},  // 20と30の間
		{30, 2},  // 最後のキーと一致
		{35, 3},  // 30より大きい
	}

	for _, tt := range tests {
		result := node.findKeyIndex(tt.key)
		if result != tt.expected {
			t.Errorf("findKeyIndex(%d): expected %d, got %d", tt.key, tt.expected, result)
		}
	}
}

func TestBTreeNode_isLeaf(t *testing.T) {
	leaf := &bTreeNode{nodeType: nodeTypeLeaf}
	internal := &bTreeNode{nodeType: nodeTypeInternal}

	if !leaf.isLeaf() {
		t.Error("expected leaf node to return true for isLeaf()")
	}
	if internal.isLeaf() {
		t.Error("expected internal node to return false for isLeaf()")
	}
}

func TestBTreeNode_isFull(t *testing.T) {
	node := &bTreeNode{
		keys: make([]int64, 0, maxKeys),
	}

	if node.isFull() {
		t.Error("empty node should not be full")
	}

	// maxKeys個のキーを追加
	for i := 0; i < maxKeys; i++ {
		node.keys = append(node.keys, int64(i))
	}

	if !node.isFull() {
		t.Error("node with maxKeys should be full")
	}
}

func TestInsertAt(t *testing.T) {
	tests := []struct {
		name     string
		slice    []int64
		index    int
		value    int64
		expected []int64
	}{
		{
			name:     "insert at beginning",
			slice:    []int64{2, 3, 4},
			index:    0,
			value:    1,
			expected: []int64{1, 2, 3, 4},
		},
		{
			name:     "insert in middle",
			slice:    []int64{1, 3, 4},
			index:    1,
			value:    2,
			expected: []int64{1, 2, 3, 4},
		},
		{
			name:     "insert at end",
			slice:    []int64{1, 2, 3},
			index:    3,
			value:    4,
			expected: []int64{1, 2, 3, 4},
		},
		{
			name:     "insert into empty",
			slice:    []int64{},
			index:    0,
			value:    1,
			expected: []int64{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := insertAt(tt.slice, tt.index, tt.value)
			if len(result) != len(tt.expected) {
				t.Errorf("expected length %d, got %d", len(tt.expected), len(result))
			}
			for i, v := range tt.expected {
				if result[i] != v {
					t.Errorf("index %d: expected %d, got %d", i, v, result[i])
				}
			}
		})
	}
}

func TestBTree_LargeDataset(t *testing.T) {
	tree := NewBTree()

	// 1000件のデータを挿入
	n := int64(1000)
	for i := int64(1); i <= n; i++ {
		tree.Insert(i, i*10)
	}

	// すべてのキーが検索できることを確認
	for i := int64(1); i <= n; i++ {
		value, found := tree.Search(i)
		if !found {
			t.Errorf("expected to find key %d", i)
		}
		if value != i*10 {
			t.Errorf("key %d: expected value %d, got %d", i, i*10, value)
		}
	}

	// 存在しないキーの検索
	_, found := tree.Search(n + 1)
	if found {
		t.Errorf("expected not to find key %d", n+1)
	}
}
