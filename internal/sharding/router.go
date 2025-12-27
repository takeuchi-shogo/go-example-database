package sharding

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Router struct {
	rangeManager *RangeManager
}

func NewRouter(rangeManager *RangeManager) *Router {
	return &Router{
		rangeManager: rangeManager,
	}
}

// GetNodeIDByRangeKey - キーに対応するノードIDを取得
func (r *Router) GetNodeIDByRangeKey(key string) (string, error) {
	rng := r.rangeManager.FindRange(key)
	if rng == nil {
		return "", ErrKeyNotFound
	}
	return rng.NodeID, nil
}

// GetRangeByRangeKey - キーに対応するシャードを取得
func (r *Router) GetRangeByRangeKey(key string) (*Range, error) {
	rng := r.rangeManager.FindRange(key)
	if rng == nil {
		return nil, ErrKeyNotFound
	}
	return rng, nil
}

// RouteRange - キー範囲に対応するノードIDを取得
func (r *Router) RouteRange(startKey, endKey string) ([]string, error) {
	ranges := r.GetRangesByRange(startKey, endKey)
	if len(ranges) == 0 {
		return nil, ErrRangeNotFound
	}
	// 重複を除去してノード ID を返す
	seen := make(map[string]bool)
	var nodeIDs []string
	for _, rng := range ranges {
		if !seen[rng.NodeID] {
			seen[rng.NodeID] = true
			nodeIDs = append(nodeIDs, rng.NodeID)
		}
	}
	return nodeIDs, nil
}

// GetRangesByRange - キー範囲に対応するシャードを取得
func (r *Router) GetRangesByRange(startKey, endKey string) []*Range {
	allRanges := r.rangeManager.GetRanges()
	var result []*Range
	for _, rng := range allRanges {
		if r.overlaps(rng, startKey, endKey) {
			result = append(result, rng)
		}
	}
	return result
}

// overlaps - シャードがクエリの範囲に重なるかどうかを判定
func (r *Router) overlaps(rng *Range, startKey, endKey string) bool {
	// Range: [rng.StartKey, rng.EndKey)
	// Query: [startKey, endKey)
	// 重なる条件: rng.StartKey < endKey && startKey < rng.EndKey

	// rng.StartKey が endKey より大きい場合は重ならない
	// endKey が空 = 無限大なので常に true
	if endKey != "" && rng.StartKey >= endKey {
		return false
	}
	// startKey が rng.EndKey より大きい場合は重ならない
	// rng.EndKey が空 = 無限大なので常に true
	if rng.EndKey != "" && startKey >= rng.EndKey {
		return false
	}
	return true
}
