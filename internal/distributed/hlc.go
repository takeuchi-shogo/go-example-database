package distributed

import (
	"fmt"
	"sync"
	"time"
)

// HybridLogicalClock - 分散システム用の論理時計
type HybridLogicalClock struct {
	mu             sync.Mutex
	physicalTime   int64  // nanoseconds
	logicalCounter uint32 // 論理時計のカウンタ
}

// Timestamp - 分散システム用の論理時計のタイムスタンプ
type Timestamp struct {
	Physical int64
	Logical  uint32
}

func (ts Timestamp) String() string {
	return fmt.Sprintf("Timestamp{Physical: %d, Logical: %d}", ts.Physical, ts.Logical)
}

func (ts Timestamp) IsZero() bool {
	return ts.Physical == 0 && ts.Logical == 0
}

// NewHybridLogicalClock - 新しい論理時計を作成
func NewHybridLogicalClock() *HybridLogicalClock {
	return &HybridLogicalClock{
		physicalTime:   time.Now().UnixNano(),
		logicalCounter: 0,
	}
}

// Now - 現在のタイムスタンプを取得
func (h *HybridLogicalClock) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixNano()
	if now > h.physicalTime {
		// 物理時刻が進んだ → 論理時計をリセット
		h.physicalTime = now
		h.logicalCounter = 0
	} else {
		// 物理時刻が進まなかった → 論理時計をインクリメント
		h.logicalCounter++
	}
	return Timestamp{
		Physical: h.physicalTime,
		Logical:  h.logicalCounter,
	}
}

// Update - タイムスタンプを更新
func (h *HybridLogicalClock) Update(ts Timestamp) Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now().UnixNano()
	// TODO: ここら辺、メソッドに分けて整理できそう
	if now > h.physicalTime && now > ts.Physical {
		h.physicalTime = now
		h.logicalCounter = 0
	} else if h.physicalTime > ts.Physical {
		h.logicalCounter++
	} else if ts.Physical > h.physicalTime {
		h.logicalCounter = ts.Logical + 1
		h.physicalTime = ts.Physical
	} else {
		if ts.Logical > h.logicalCounter {
			h.logicalCounter = ts.Logical + 1
		} else {
			h.logicalCounter++
		}
	}
	return Timestamp{
		Physical: h.physicalTime,
		Logical:  h.logicalCounter,
	}
}

// Compare - タイムスタンプを比較
func (ts Timestamp) Compare(other Timestamp) int {
	// 物理時刻が異なる場合、物理時刻を比較
	if ts.Physical > other.Physical {
		return 1
	}
	if ts.Physical < other.Physical {
		return -1
	}
	// 物理時刻が同じ場合、論理時計を比較
	if ts.Logical > other.Logical {
		return 1
	}
	if ts.Logical < other.Logical {
		return -1
	}
	return 0
}
