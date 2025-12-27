package sharding

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrRangeNotFound   = errors.New("range not found")
	ErrInvalidSplitKey = errors.New("invalid split key")
	ErrNotAdjacent     = errors.New("not adjacent")
)

// Range - キーの範囲（シャードの単位）
type Range struct {
	ID       string // シャードID
	StartKey string // シャードの開始キー
	EndKey   string // シャードの終了キー
	NodeID   string // シャードの担当ノードID
}

// Contains - キーがシャードに含まれるかどうかを判定
func (r *Range) Contains(key string) bool {
	// StartKey より小さいキーは含まれない
	if key < r.StartKey {
		return false
	}
	// EndKey より大きいキーは含まれない
	if r.EndKey != "" && key >= r.EndKey {
		return false
	}
	return true
}

type RangeManager struct {
	mu     sync.RWMutex
	ranges []*Range // StartKey でソートされたシャードリスト
}

func NewRangeManager(initialNodeID string) *RangeManager {
	initialRanges := []*Range{
		{
			ID:       uuid.New().String(),
			StartKey: "",
			EndKey:   "",
			NodeID:   initialNodeID,
		},
	}
	return &RangeManager{
		ranges: initialRanges,
	}
}

// FindRange - キーに対応するシャードを検索
func (rm *RangeManager) FindRange(key string) *Range {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	// 2分検索
	left, right := 0, len(rm.ranges)-1
	for left <= right {
		mid := (left + right) / 2
		r := rm.ranges[mid]
		if r.Contains(key) {
			return r
		}
		// key が r.StartKey より小さい場合は、左半分を検索
		if key < r.StartKey {
			right = mid - 1
		} else {
			// key が r.EndKey より大きい場合は、右半分を検索
			left = mid + 1
		}
	}
	// 見つからない場合は、最後のシャードを返す
	return nil
}

// GetRanges - 全シャードを取得
// シャードリストをコピーして返す
func (rm *RangeManager) GetRanges() []*Range {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make([]*Range, len(rm.ranges))
	copy(result, rm.ranges)
	return result
}

// GetRangeByID - ID でシャードを取得
func (rm *RangeManager) GetRangeByID(id string) *Range {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for _, r := range rm.ranges {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (rm *RangeManager) findRangeIndex(id string) int {
	for i, r := range rm.ranges {
		if r.ID == id {
			return i
		}
	}
	return -1
}

// SplitRange - シャードを分割
// id のシャードを splitKey で分割し、新しいシャードを作成
// newNodeID には新しいシャードの担当ノードIDを設定
func (rm *RangeManager) SplitRange(id string, splitKey string, newNodeID string) (*Range, *Range, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	index := rm.findRangeIndex(id)
	if index == -1 {
		return nil, nil, ErrRangeNotFound
	}

	original := rm.ranges[index]
	// splitKey が original.StartKey より小さい場合は、エラーを返す
	if splitKey <= original.StartKey {
		return nil, nil, ErrInvalidSplitKey
	}
	if original.EndKey != "" && splitKey >= original.EndKey {
		return nil, nil, ErrInvalidSplitKey
	}
	// 新しい2つの Range を作成
	leftRange := &Range{
		ID:       original.ID, // 元の Range の ID をそのまま使用
		StartKey: original.StartKey,
		EndKey:   splitKey,
		NodeID:   original.NodeID,
	}
	rightRange := &Range{
		ID:       uuid.New().String(),
		StartKey: splitKey,
		EndKey:   original.EndKey,
		NodeID:   newNodeID, // 新しいノードIDを設定
	}
	// 配列を更新
	newRanges := make([]*Range, 0, len(rm.ranges)+1)
	newRanges = append(newRanges, rm.ranges[:index]...)
	newRanges = append(newRanges, leftRange, rightRange)
	newRanges = append(newRanges, rm.ranges[index+1:]...)
	rm.ranges = newRanges

	return leftRange, rightRange, nil
}

// MergeRanges - 2つのシャードをマージ
// rangeID1 と rangeID2 が隣接していることを確認してからマージする
func (rm *RangeManager) MergeRanges(id1 string, id2 string) (*Range, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	// 両方の Range を検索
	index1 := rm.findRangeIndex(id1)
	index2 := rm.findRangeIndex(id2)
	if index1 == -1 || index2 == -1 {
		return nil, ErrRangeNotFound
	}
	// 隣接しているかチェック
	if index1+1 != index2 {
		return nil, ErrNotAdjacent
	}
	range1 := rm.ranges[index1]
	range2 := rm.ranges[index2]
	// マージする
	mergedRange := &Range{
		ID:       range1.ID, // 元の Range の ID をそのまま使用
		StartKey: range1.StartKey,
		EndKey:   range2.EndKey, // 右の Range の EndKey を使用
		NodeID:   range1.NodeID, // 元の Range の NodeID を使用
	}
	// 配列を更新
	newRanges := make([]*Range, 0, len(rm.ranges)-1)
	newRanges = append(newRanges, rm.ranges[:index1]...)
	newRanges = append(newRanges, mergedRange)
	newRanges = append(newRanges, rm.ranges[index2+1:]...)
	rm.ranges = newRanges

	return mergedRange, nil
}

// UpdateNodeID - シャードの担当ノードIDを更新
// id のシャードの担当ノードIDを newNodeID に更新
func (rm *RangeManager) UpdateNodeID(id string, newNodeID string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, r := range rm.ranges {
		if r.ID == id {
			r.NodeID = newNodeID
			return nil
		}
	}
	return ErrRangeNotFound
}
