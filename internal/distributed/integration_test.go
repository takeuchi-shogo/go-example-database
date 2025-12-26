package distributed

import (
	"bytes"
	"sync"
	"testing"
)

// =============================================================================
// シナリオ 1: 送金成功
// =============================================================================

func TestIntegration_TransferSuccess(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 2つのノード（2つの MVCC ストア）
	storeA := NewMVCCStore(clock) // ユーザーAの残高
	storeB := NewMVCCStore(clock) // ユーザーBの残高

	// 初期残高を設定
	storeA.Put("balance", []byte("1000"))
	storeB.Put("balance", []byte("500"))

	// 参加者を作成
	participantA := NewLocalParticipant("node-A", storeA)
	participantB := NewLocalParticipant("node-B", storeB)

	// コーディネーターを作成
	coordinator := NewCoordinator(
		[]Participant{participantA, participantB},
		clock,
	)

	// トランザクション開始
	txnID := coordinator.BeginTransaction(nil)

	// 送金処理（両ノードに新しい残高を書き込む）
	writes := []WriteIntent{
		{Key: "balance", Value: []byte("900")}, // A: 1000 - 100 = 900
	}

	err := coordinator.Commit(txnID, writes)

	if err != nil {
		t.Errorf("Commit() error = %v, want nil", err)
	}

	// 残高を確認（両ノードに同じ値が書き込まれる）
	ts := clock.Now()
	dataA, okA := storeA.Get("balance", ts)
	if !okA {
		t.Error("storeA.Get() should return true")
	}
	if !bytes.Equal(dataA, []byte("900")) {
		t.Errorf("storeA balance = %s, want 900", string(dataA))
	}

	dataB, okB := storeB.Get("balance", ts)
	if !okB {
		t.Error("storeB.Get() should return true")
	}
	if !bytes.Equal(dataB, []byte("900")) {
		t.Errorf("storeB balance = %s, want 900", string(dataB))
	}
}

// =============================================================================
// シナリオ 2: 送金失敗（1人が No を返す）
// =============================================================================

// RejectingParticipant - 常に No を返す参加者
type RejectingParticipant struct {
	aborted bool
}

func (p *RejectingParticipant) Prepare(request *PrepareRequest) *PrepareResponse {
	return &PrepareResponse{
		TxnID:  request.TxnID,
		Vote:   false,
		Reason: "insufficient balance",
	}
}

func (p *RejectingParticipant) Commit(request *CommitRequest) *CommitResponse {
	return &CommitResponse{TxnID: request.TxnID, Success: true}
}

func (p *RejectingParticipant) Abort(request *AbortRequest) *AbortResponse {
	p.aborted = true
	return &AbortResponse{TxnID: request.TxnID, Success: true}
}

func TestIntegration_TransferFailure(t *testing.T) {
	clock := NewHybridLogicalClock()

	storeA := NewMVCCStore(clock)
	storeA.Put("balance", []byte("1000"))

	participantA := NewLocalParticipant("node-A", storeA)
	participantB := &RejectingParticipant{} // 常に No を返す

	coordinator := NewCoordinator(
		[]Participant{participantA, participantB},
		clock,
	)

	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{
		{Key: "balance", Value: []byte("900")},
	}

	err := coordinator.Commit(txnID, writes)

	// エラーが返ること
	if err == nil {
		t.Error("Commit() should return error when one participant votes No")
	}

	// Abort が呼ばれたこと
	if !participantB.aborted {
		t.Error("participantB.Abort() should be called")
	}

	// storeA のデータは変更されていないこと（初期値のまま）
	ts := clock.Now()
	data, ok := storeA.Get("balance", ts)
	if !ok {
		t.Error("storeA.Get() should return true")
	}
	if !bytes.Equal(data, []byte("1000")) {
		t.Errorf("storeA balance = %s, want 1000 (unchanged)", string(data))
	}
}

// =============================================================================
// シナリオ 3: スナップショット分離
// =============================================================================

func TestIntegration_SnapshotIsolation(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 初期値を書き込み
	store.Put("key", []byte("value1"))

	// スナップショット時点を記録（トランザクション開始時点）
	readTs := clock.Now()

	// 別のトランザクションが書き込み
	store.Put("key", []byte("value2"))
	store.Put("key", []byte("value3"))

	// スナップショット時点では value1 が見える
	data, ok := store.Get("key", readTs)
	if !ok {
		t.Error("Get() should return true")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() at readTs = %s, want value1", string(data))
	}

	// 最新時点では value3 が見える
	latestTs := clock.Now()
	data, ok = store.Get("key", latestTs)
	if !ok {
		t.Error("Get() should return true")
	}
	if !bytes.Equal(data, []byte("value3")) {
		t.Errorf("Get() at latestTs = %s, want value3", string(data))
	}
}

func TestIntegration_SnapshotIsolation_Delete(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 初期値を書き込み
	store.Put("key", []byte("value1"))

	// スナップショット時点を記録
	readTs := clock.Now()

	// 削除
	store.Delete("key")

	// スナップショット時点では value1 が見える
	data, ok := store.Get("key", readTs)
	if !ok {
		t.Error("Get() should return true before delete")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("Get() = %s, want value1", string(data))
	}

	// 最新時点では削除済み
	latestTs := clock.Now()
	_, ok = store.Get("key", latestTs)
	if ok {
		t.Error("Get() should return false after delete")
	}
}

// =============================================================================
// シナリオ 4: 並行トランザクション
// =============================================================================

func TestIntegration_ConcurrentTransactions(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)
	coordinator := NewCoordinator([]Participant{participant}, clock)

	var wg sync.WaitGroup
	numTxns := 100

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			txnID := coordinator.BeginTransaction(nil)
			writes := []WriteIntent{
				{Key: "counter", Value: []byte("value")},
			}
			coordinator.Commit(txnID, writes)
		}(i)
	}

	wg.Wait()
	// パニックせずに完了すればOK
}

func TestIntegration_ConcurrentReadWrite(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 初期値
	store.Put("key", []byte("initial"))

	var wg sync.WaitGroup

	// 読み取りゴルーチン
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readTs := clock.Now()
			for j := 0; j < 100; j++ {
				store.Get("key", readTs)
			}
		}()
	}

	// 書き込みゴルーチン
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.Put("key", []byte("updated"))
			}
		}(i)
	}

	wg.Wait()
	// パニックせずに完了すればOK
}

// =============================================================================
// シナリオ 5: HLC の因果関係保証
// =============================================================================

func TestIntegration_HLC_Causality(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	// 連続した書き込み
	ts1 := store.Put("key", []byte("v1"))
	ts2 := store.Put("key", []byte("v2"))
	ts3 := store.Put("key", []byte("v3"))

	// タイムスタンプは単調増加すること
	if ts2.Compare(ts1) <= 0 {
		t.Errorf("ts2 should be > ts1: ts1=%v, ts2=%v", ts1, ts2)
	}
	if ts3.Compare(ts2) <= 0 {
		t.Errorf("ts3 should be > ts2: ts2=%v, ts3=%v", ts2, ts3)
	}

	// 各時点で正しい値が見えること
	data1, _ := store.Get("key", ts1)
	data2, _ := store.Get("key", ts2)
	data3, _ := store.Get("key", ts3)

	if !bytes.Equal(data1, []byte("v1")) {
		t.Errorf("at ts1: %s, want v1", string(data1))
	}
	if !bytes.Equal(data2, []byte("v2")) {
		t.Errorf("at ts2: %s, want v2", string(data2))
	}
	if !bytes.Equal(data3, []byte("v3")) {
		t.Errorf("at ts3: %s, want v3", string(data3))
	}
}

func TestIntegration_HLC_Update(t *testing.T) {
	clockA := NewHybridLogicalClock()
	clockB := NewHybridLogicalClock()

	// ノードAで書き込み
	tsA := clockA.Now()

	// ノードBがノードAのタイムスタンプを受信
	tsB := clockB.Update(tsA)

	// tsB は tsA より大きいこと（因果関係を保証）
	if tsB.Compare(tsA) <= 0 {
		t.Errorf("tsB should be > tsA: tsA=%v, tsB=%v", tsA, tsB)
	}
}

// =============================================================================
// シナリオ 6: 複数ノードでの一貫性
// =============================================================================

func TestIntegration_MultiNode_Consistency(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 3つのノード
	store1 := NewMVCCStore(clock)
	store2 := NewMVCCStore(clock)
	store3 := NewMVCCStore(clock)

	participant1 := NewLocalParticipant("node-1", store1)
	participant2 := NewLocalParticipant("node-2", store2)
	participant3 := NewLocalParticipant("node-3", store3)

	coordinator := NewCoordinator(
		[]Participant{participant1, participant2, participant3},
		clock,
	)

	// トランザクションを実行
	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{
		{Key: "data", Value: []byte("consistent-value")},
	}

	err := coordinator.Commit(txnID, writes)
	if err != nil {
		t.Errorf("Commit() error = %v", err)
	}

	// 全ノードで同じ値が見えること
	ts := clock.Now()

	data1, ok1 := store1.Get("data", ts)
	data2, ok2 := store2.Get("data", ts)
	data3, ok3 := store3.Get("data", ts)

	if !ok1 || !ok2 || !ok3 {
		t.Error("All stores should have the data")
	}

	expected := []byte("consistent-value")
	if !bytes.Equal(data1, expected) {
		t.Errorf("store1: %s, want %s", string(data1), string(expected))
	}
	if !bytes.Equal(data2, expected) {
		t.Errorf("store2: %s, want %s", string(data2), string(expected))
	}
	if !bytes.Equal(data3, expected) {
		t.Errorf("store3: %s, want %s", string(data3), string(expected))
	}
}

// =============================================================================
// シナリオ 7: エンドツーエンド - 銀行振込
// =============================================================================

func TestIntegration_EndToEnd_BankTransfer(t *testing.T) {
	clock := NewHybridLogicalClock()

	// 銀行のストア
	bankStore := NewMVCCStore(clock)

	// 初期残高
	bankStore.Put("account:alice", []byte("1000"))
	bankStore.Put("account:bob", []byte("500"))

	participant := NewLocalParticipant("bank", bankStore)
	coordinator := NewCoordinator([]Participant{participant}, clock)

	// 振込前の残高を確認
	ts0 := clock.Now()
	aliceBefore, _ := bankStore.Get("account:alice", ts0)
	bobBefore, _ := bankStore.Get("account:bob", ts0)

	t.Logf("Before: Alice=%s, Bob=%s", string(aliceBefore), string(bobBefore))

	// Alice から Bob へ 200円振込
	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{
		{Key: "account:alice", Value: []byte("800")}, // 1000 - 200
		{Key: "account:bob", Value: []byte("700")},   // 500 + 200
	}

	err := coordinator.Commit(txnID, writes)
	if err != nil {
		t.Errorf("Commit() error = %v", err)
	}

	// 振込後の残高を確認
	ts1 := clock.Now()
	aliceAfter, _ := bankStore.Get("account:alice", ts1)
	bobAfter, _ := bankStore.Get("account:bob", ts1)

	t.Logf("After: Alice=%s, Bob=%s", string(aliceAfter), string(bobAfter))

	if !bytes.Equal(aliceAfter, []byte("800")) {
		t.Errorf("Alice after = %s, want 800", string(aliceAfter))
	}
	if !bytes.Equal(bobAfter, []byte("700")) {
		t.Errorf("Bob after = %s, want 700", string(bobAfter))
	}

	// 振込前のスナップショットでは元の値が見える
	aliceSnapshot, _ := bankStore.Get("account:alice", ts0)
	bobSnapshot, _ := bankStore.Get("account:bob", ts0)

	if !bytes.Equal(aliceSnapshot, []byte("1000")) {
		t.Errorf("Alice snapshot = %s, want 1000", string(aliceSnapshot))
	}
	if !bytes.Equal(bobSnapshot, []byte("500")) {
		t.Errorf("Bob snapshot = %s, want 500", string(bobSnapshot))
	}
}
