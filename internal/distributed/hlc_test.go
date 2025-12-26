package distributed

import (
	"sync"
	"testing"
	"time"
)

func TestNewHybridLogicalClock(t *testing.T) {
	clock := NewHybridLogicalClock()

	if clock == nil {
		t.Fatal("NewHybridLogicalClock() returned nil")
	}
	if clock.physicalTime == 0 {
		t.Error("physicalTime should not be 0")
	}
	if clock.logicalCounter != 0 {
		t.Errorf("logicalCounter = %d, want 0", clock.logicalCounter)
	}
}

func TestHybridLogicalClock_Now(t *testing.T) {
	clock := NewHybridLogicalClock()

	ts1 := clock.Now()
	ts2 := clock.Now()

	// 2回目のタイムスタンプは1回目より大きいこと
	if ts2.Compare(ts1) <= 0 {
		t.Errorf("ts2 should be greater than ts1: ts1=%v, ts2=%v", ts1, ts2)
	}
}

func TestHybridLogicalClock_Now_MonotonicIncrease(t *testing.T) {
	clock := NewHybridLogicalClock()

	var prev Timestamp
	for i := 0; i < 100; i++ {
		current := clock.Now()
		if i > 0 && current.Compare(prev) <= 0 {
			t.Errorf("Timestamp should monotonically increase: prev=%v, current=%v", prev, current)
		}
		prev = current
	}
}

func TestHybridLogicalClock_Now_LogicalCounterIncrement(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 物理時計が同じ場合、論理カウンタがインクリメントされることを確認
	ts1 := clock.Now()
	ts2 := clock.Now()

	// 短時間で呼び出すので、物理時計が同じ可能性が高い
	if ts1.Physical == ts2.Physical {
		if ts2.Logical != ts1.Logical+1 {
			t.Errorf("Logical counter should increment: ts1.Logical=%d, ts2.Logical=%d",
				ts1.Logical, ts2.Logical)
		}
	}
}

func TestHybridLogicalClock_Update_ReceivedIsNewer(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 未来のタイムスタンプを受信
	futureTime := time.Now().Add(time.Hour).UnixNano()
	received := Timestamp{Physical: futureTime, Logical: 5}

	result := clock.Update(received)

	// 受信したタイムスタンプより大きくなること
	if result.Compare(received) <= 0 {
		t.Errorf("Result should be greater than received: received=%v, result=%v", received, result)
	}
	// 物理時間は受信したものを採用
	if result.Physical != futureTime {
		t.Errorf("Physical time should be %d, got %d", futureTime, result.Physical)
	}
	// 論理カウンタは received.Logical + 1
	if result.Logical != 6 {
		t.Errorf("Logical counter should be 6, got %d", result.Logical)
	}
}

func TestHybridLogicalClock_Update_LocalIsNewer(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 過去のタイムスタンプを受信
	pastTime := time.Now().Add(-time.Hour).UnixNano()
	received := Timestamp{Physical: pastTime, Logical: 100}

	ts1 := clock.Now()
	result := clock.Update(received)

	// ローカルの物理時間が維持されること
	if result.Physical < ts1.Physical {
		t.Errorf("Physical time should not decrease: ts1=%v, result=%v", ts1, result)
	}
	// 結果は受信より大きいこと
	if result.Compare(received) <= 0 {
		t.Errorf("Result should be greater than received: received=%v, result=%v", received, result)
	}
}

func TestHybridLogicalClock_Update_SamePhysicalTime(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 現在時刻を取得してローカルの状態を設定
	ts1 := clock.Now()

	// 同じ物理時間で論理カウンタが大きいタイムスタンプを受信
	received := Timestamp{Physical: ts1.Physical, Logical: ts1.Logical + 10}

	result := clock.Update(received)

	// 受信したタイムスタンプより大きくなること
	if result.Compare(received) <= 0 {
		t.Errorf("Result should be greater than received: received=%v, result=%v", received, result)
	}
}

func TestTimestamp_Compare(t *testing.T) {
	tests := []struct {
		name     string
		ts1      Timestamp
		ts2      Timestamp
		expected int
	}{
		{
			name:     "ts1 < ts2 (physical)",
			ts1:      Timestamp{Physical: 100, Logical: 0},
			ts2:      Timestamp{Physical: 200, Logical: 0},
			expected: -1,
		},
		{
			name:     "ts1 > ts2 (physical)",
			ts1:      Timestamp{Physical: 200, Logical: 0},
			ts2:      Timestamp{Physical: 100, Logical: 0},
			expected: 1,
		},
		{
			name:     "ts1 < ts2 (logical)",
			ts1:      Timestamp{Physical: 100, Logical: 5},
			ts2:      Timestamp{Physical: 100, Logical: 10},
			expected: -1,
		},
		{
			name:     "ts1 > ts2 (logical)",
			ts1:      Timestamp{Physical: 100, Logical: 10},
			ts2:      Timestamp{Physical: 100, Logical: 5},
			expected: 1,
		},
		{
			name:     "ts1 == ts2",
			ts1:      Timestamp{Physical: 100, Logical: 5},
			ts2:      Timestamp{Physical: 100, Logical: 5},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts1.Compare(tt.ts2)
			if result != tt.expected {
				t.Errorf("Compare() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestTimestamp_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		ts       Timestamp
		expected bool
	}{
		{
			name:     "zero timestamp",
			ts:       Timestamp{Physical: 0, Logical: 0},
			expected: true,
		},
		{
			name:     "non-zero physical",
			ts:       Timestamp{Physical: 100, Logical: 0},
			expected: false,
		},
		{
			name:     "non-zero logical",
			ts:       Timestamp{Physical: 0, Logical: 1},
			expected: false,
		},
		{
			name:     "non-zero both",
			ts:       Timestamp{Physical: 100, Logical: 1},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts.IsZero()
			if result != tt.expected {
				t.Errorf("IsZero() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestTimestamp_String(t *testing.T) {
	ts := Timestamp{Physical: 12345, Logical: 67}
	expected := "Timestamp{Physical: 12345, Logical: 67}"

	result := ts.String()
	if result != expected {
		t.Errorf("String() = %s, want %s", result, expected)
	}
}

func TestHybridLogicalClock_Concurrent(t *testing.T) {
	clock := NewHybridLogicalClock()

	var wg sync.WaitGroup
	timestamps := make(chan Timestamp, 100)

	// 10個のゴルーチンから同時にNow()を呼び出す
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ts := clock.Now()
				timestamps <- ts
			}
		}()
	}

	wg.Wait()
	close(timestamps)

	// 全てのタイムスタンプがユニークであることを確認
	seen := make(map[Timestamp]bool)
	for ts := range timestamps {
		if seen[ts] {
			t.Errorf("Duplicate timestamp found: %v", ts)
		}
		seen[ts] = true
	}
}

func TestHybridLogicalClock_Update_Concurrent(t *testing.T) {
	clock := NewHybridLogicalClock()

	var wg sync.WaitGroup

	// 複数のゴルーチンから同時にUpdateを呼び出す
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				received := Timestamp{
					Physical: time.Now().UnixNano(),
					Logical:  uint32(id*10 + j),
				}
				clock.Update(received)
			}
		}(i)
	}

	wg.Wait()
	// パニックしなければOK
}
