package distributed

import (
	"bytes"
	"sync"
	"testing"
)

func TestNewMVCCStore(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	if store == nil {
		t.Fatal("NewMVCCStore() returned nil")
	}
	if store.clock == nil {
		t.Error("clock should not be nil")
	}
	if store.data == nil {
		t.Error("data should not be nil")
	}
}

func TestMVCCStore_Put_NewKey(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts := store.Put("key1", []byte("value1"))

	if ts.IsZero() {
		t.Error("Put() should return non-zero timestamp")
	}

	// 値が取得できることを確認
	data, ok := store.Get("key1", ts)
	if !ok {
		t.Error("Get() should return true for existing key")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() = %s, want value1", string(data))
	}
}

func TestMVCCStore_Put_ExistingKey(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts1 := store.Put("key1", []byte("value1"))
	ts2 := store.Put("key1", []byte("value2"))

	// ts2 は ts1 より大きいこと
	if ts2.Compare(ts1) <= 0 {
		t.Errorf("ts2 should be greater than ts1: ts1=%v, ts2=%v", ts1, ts2)
	}

	// ts2 時点では value2 が見える
	data, ok := store.Get("key1", ts2)
	if !ok {
		t.Error("Get() should return true")
	}
	if !bytes.Equal(data, []byte("value2")) {
		t.Errorf("Get() = %s, want value2", string(data))
	}

	// ts1 時点では value1 が見える
	data, ok = store.Get("key1", ts1)
	if !ok {
		t.Error("Get() should return true for ts1")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() at ts1 = %s, want value1", string(data))
	}
}

func TestMVCCStore_Get_NonExistentKey(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts := clock.Now()
	data, ok := store.Get("nonexistent", ts)

	if ok {
		t.Error("Get() should return false for non-existent key")
	}
	if data != nil {
		t.Errorf("Get() data should be nil, got %v", data)
	}
}

func TestMVCCStore_Get_SnapshotRead(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 3つのバージョンを作成
	ts1 := store.Put("key1", []byte("v1"))
	ts2 := store.Put("key1", []byte("v2"))
	ts3 := store.Put("key1", []byte("v3"))

	// 各タイムスタンプ時点の値を確認
	tests := []struct {
		name     string
		readTs   Timestamp
		expected string
	}{
		{"read at ts1", ts1, "v1"},
		{"read at ts2", ts2, "v2"},
		{"read at ts3", ts3, "v3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, ok := store.Get("key1", tt.readTs)
			if !ok {
				t.Error("Get() should return true")
			}
			if !bytes.Equal(data, []byte(tt.expected)) {
				t.Errorf("Get() = %s, want %s", string(data), tt.expected)
			}
		})
	}
}

func TestMVCCStore_Get_FutureTimestamp(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 過去のタイムスタンプで読み取り
	pastTs := Timestamp{Physical: 1, Logical: 0}

	store.Put("key1", []byte("value1"))

	// 過去のタイムスタンプでは何も見えない
	data, ok := store.Get("key1", pastTs)
	if ok {
		t.Error("Get() should return false for past timestamp")
	}
	if data != nil {
		t.Errorf("Get() data should be nil, got %v", data)
	}
}

func TestMVCCStore_Delete(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts1 := store.Put("key1", []byte("value1"))
	tsDelete := store.Delete("key1")

	// 削除後は見えない
	data, ok := store.Get("key1", tsDelete)
	if ok {
		t.Error("Get() should return false after delete")
	}
	if data != nil {
		t.Errorf("Get() data should be nil after delete, got %v", data)
	}

	// 削除前のタイムスタンプでは見える
	data, ok = store.Get("key1", ts1)
	if !ok {
		t.Error("Get() should return true for timestamp before delete")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() = %s, want value1", string(data))
	}
}

func TestMVCCStore_Delete_NonExistentKey(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts := store.Delete("nonexistent")

	if ts.IsZero() {
		t.Error("Delete() should return non-zero timestamp")
	}

	// 削除後は見えない
	data, ok := store.Get("nonexistent", ts)
	if ok {
		t.Error("Get() should return false for deleted key")
	}
	if data != nil {
		t.Errorf("Get() data should be nil, got %v", data)
	}
}

func TestMVCCStore_Delete_ThenPut(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts1 := store.Put("key1", []byte("value1"))
	tsDelete := store.Delete("key1")
	ts2 := store.Put("key1", []byte("value2"))

	// ts1 時点では value1
	data, ok := store.Get("key1", ts1)
	if !ok || !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() at ts1 = %s, want value1", string(data))
	}

	// tsDelete 時点では削除済み
	data, ok = store.Get("key1", tsDelete)
	if ok {
		t.Errorf("Get() at tsDelete should return false, got %s", string(data))
	}

	// ts2 時点では value2
	data, ok = store.Get("key1", ts2)
	if !ok || !bytes.Equal(data, []byte("value2")) {
		t.Errorf("Get() at ts2 = %s, want value2", string(data))
	}
}

func TestMVCCStore_MultipleKeys(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	ts1 := store.Put("key1", []byte("value1"))
	ts2 := store.Put("key2", []byte("value2"))
	ts3 := store.Put("key3", []byte("value3"))

	// 最新のタイムスタンプで全て読める
	latestTs := clock.Now()

	data, ok := store.Get("key1", latestTs)
	if !ok || !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get(key1) = %s, want value1", string(data))
	}

	data, ok = store.Get("key2", latestTs)
	if !ok || !bytes.Equal(data, []byte("value2")) {
		t.Errorf("Get(key2) = %s, want value2", string(data))
	}

	data, ok = store.Get("key3", latestTs)
	if !ok || !bytes.Equal(data, []byte("value3")) {
		t.Errorf("Get(key3) = %s, want value3", string(data))
	}

	// 各タイムスタンプでも読める
	_ = ts1
	_ = ts2
	_ = ts3
}

func TestMVCCStore_Concurrent_Put(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	var wg sync.WaitGroup
	numGoroutines := 10
	numPuts := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPuts; j++ {
				store.Put("key1", []byte("value"))
			}
		}(i)
	}

	wg.Wait()
	// パニックしなければOK
}

func TestMVCCStore_Concurrent_GetPut(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 初期値を設定
	store.Put("key1", []byte("initial"))

	var wg sync.WaitGroup

	// 書き込みゴルーチン
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			store.Put("key1", []byte("updated"))
		}
	}()

	// 読み取りゴルーチン
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts := clock.Now()
			for j := 0; j < 100; j++ {
				store.Get("key1", ts)
			}
		}()
	}

	wg.Wait()
	// パニックしなければOK
}

func TestMVCCStore_VersionChain(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 10個のバージョンを作成
	timestamps := make([]Timestamp, 10)
	for i := 0; i < 10; i++ {
		timestamps[i] = store.Put("key1", []byte{byte(i)})
	}

	// 各タイムスタンプで正しいバージョンが見えること
	for i, ts := range timestamps {
		data, ok := store.Get("key1", ts)
		if !ok {
			t.Errorf("Get() at timestamps[%d] should return true", i)
			continue
		}
		if len(data) != 1 || data[0] != byte(i) {
			t.Errorf("Get() at timestamps[%d] = %v, want [%d]", i, data, i)
		}
	}
}
