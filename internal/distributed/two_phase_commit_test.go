package distributed

import (
	"bytes"
	"sync"
	"testing"
)

func TestNewCoordinator(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)
	participants := []Participant{participant}

	coordinator := NewCoordinator(participants, clock)

	if coordinator == nil {
		t.Fatal("NewCoordinator() returned nil")
	}
	if len(coordinator.participants) != 1 {
		t.Errorf("len(participants) = %d, want 1", len(coordinator.participants))
	}
	if coordinator.transactions == nil {
		t.Error("transactions should not be nil")
	}
}

func TestNewLocalParticipant(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)

	participant := NewLocalParticipant("node-1", store)

	if participant == nil {
		t.Fatal("NewLocalParticipant() returned nil")
	}
	if participant.nodeID != "node-1" {
		t.Errorf("nodeID = %s, want node-1", participant.nodeID)
	}
	if participant.store == nil {
		t.Error("store should not be nil")
	}
	if participant.preparedTxn == nil {
		t.Error("preparedTxn should not be nil")
	}
}

func TestCoordinator_BeginTransaction(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)
	coordinator := NewCoordinator([]Participant{participant}, clock)

	txnID := coordinator.BeginTransaction(nil)

	if txnID == "" {
		t.Error("BeginTransaction() should return non-empty TxnID")
	}

	record, ok := coordinator.transactions[txnID]
	if !ok {
		t.Error("transaction should be recorded")
	}
	if record.State != TxnPending {
		t.Errorf("State = %v, want TxnPending", record.State)
	}
}

func TestCoordinator_Commit_SingleParticipant(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)
	coordinator := NewCoordinator([]Participant{participant}, clock)

	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
	}

	err := coordinator.Commit(txnID, writes)

	if err != nil {
		t.Errorf("Commit() error = %v, want nil", err)
	}

	// トランザクション状態を確認
	record := coordinator.transactions[txnID]
	if record.State != TxnCommitted {
		t.Errorf("State = %v, want TxnCommitted", record.State)
	}

	// データが書き込まれたことを確認
	ts := clock.Now()
	data, ok := store.Get("key1", ts)
	if !ok {
		t.Error("key1 should exist")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("key1 = %s, want value1", string(data))
	}

	data, ok = store.Get("key2", ts)
	if !ok {
		t.Error("key2 should exist")
	}
	if !bytes.Equal(data, []byte("value2")) {
		t.Errorf("key2 = %s, want value2", string(data))
	}
}

func TestCoordinator_Commit_MultipleParticipants(t *testing.T) {
	clock := NewHybridLogicalClock()
	store1 := NewMVCCStore(clock)
	store2 := NewMVCCStore(clock)
	participant1 := NewLocalParticipant("node-1", store1)
	participant2 := NewLocalParticipant("node-2", store2)
	coordinator := NewCoordinator([]Participant{participant1, participant2}, clock)

	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{
		{Key: "key1", Value: []byte("value1")},
	}

	err := coordinator.Commit(txnID, writes)

	if err != nil {
		t.Errorf("Commit() error = %v, want nil", err)
	}

	// 両方のストアにデータが書き込まれたことを確認
	ts := clock.Now()

	data, ok := store1.Get("key1", ts)
	if !ok {
		t.Error("key1 should exist in store1")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("store1 key1 = %s, want value1", string(data))
	}

	data, ok = store2.Get("key1", ts)
	if !ok {
		t.Error("key1 should exist in store2")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("store2 key1 = %s, want value1", string(data))
	}
}

func TestLocalParticipant_Prepare(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)

	request := &PrepareRequest{
		TxnID:     "txn-1",
		Writes:    []WriteIntent{{Key: "key1", Value: []byte("value1")}},
		Timestamp: clock.Now(),
	}

	response := participant.Prepare(request)

	if response.TxnID != "txn-1" {
		t.Errorf("TxnID = %s, want txn-1", response.TxnID)
	}
	if !response.Vote {
		t.Error("Vote should be true")
	}

	// preparedTxn に記録されていることを確認
	if _, ok := participant.preparedTxn["txn-1"]; !ok {
		t.Error("transaction should be in preparedTxn")
	}
}

func TestLocalParticipant_Commit(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)

	// まず Prepare
	prepareReq := &PrepareRequest{
		TxnID:     "txn-1",
		Writes:    []WriteIntent{{Key: "key1", Value: []byte("value1")}},
		Timestamp: clock.Now(),
	}
	participant.Prepare(prepareReq)

	// 次に Commit
	commitReq := &CommitRequest{
		TxnID:     "txn-1",
		Timestamp: clock.Now(),
	}
	response := participant.Commit(commitReq)

	if !response.Success {
		t.Error("Success should be true")
	}

	// データが書き込まれたことを確認
	ts := clock.Now()
	data, ok := store.Get("key1", ts)
	if !ok {
		t.Error("key1 should exist")
	}
	if !bytes.Equal(data, []byte("value1")) {
		t.Errorf("key1 = %s, want value1", string(data))
	}

	// preparedTxn から削除されていることを確認
	if _, ok := participant.preparedTxn["txn-1"]; ok {
		t.Error("transaction should be removed from preparedTxn")
	}
}

func TestLocalParticipant_Commit_NotPrepared(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)

	// Prepare せずに Commit
	commitReq := &CommitRequest{
		TxnID:     "txn-unknown",
		Timestamp: clock.Now(),
	}
	response := participant.Commit(commitReq)

	if response.Success {
		t.Error("Success should be false for unprepared transaction")
	}
}

func TestLocalParticipant_Abort(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)

	// まず Prepare
	prepareReq := &PrepareRequest{
		TxnID:     "txn-1",
		Writes:    []WriteIntent{{Key: "key1", Value: []byte("value1")}},
		Timestamp: clock.Now(),
	}
	participant.Prepare(prepareReq)

	// 次に Abort
	abortReq := &AbortRequest{TxnID: "txn-1"}
	response := participant.Abort(abortReq)

	if !response.Success {
		t.Error("Success should be true")
	}

	// データが書き込まれていないことを確認
	ts := clock.Now()
	_, ok := store.Get("key1", ts)
	if ok {
		t.Error("key1 should not exist after abort")
	}

	// preparedTxn から削除されていることを確認
	if _, ok := participant.preparedTxn["txn-1"]; ok {
		t.Error("transaction should be removed from preparedTxn")
	}
}

// MockParticipant - テスト用のモック参加者
type MockParticipant struct {
	voteYes     bool
	prepareCnt  int
	commitCnt   int
	abortCnt    int
}

func (m *MockParticipant) Prepare(request *PrepareRequest) *PrepareResponse {
	m.prepareCnt++
	return &PrepareResponse{
		TxnID: request.TxnID,
		Vote:  m.voteYes,
	}
}

func (m *MockParticipant) Commit(request *CommitRequest) *CommitResponse {
	m.commitCnt++
	return &CommitResponse{
		TxnID:   request.TxnID,
		Success: true,
	}
}

func (m *MockParticipant) Abort(request *AbortRequest) *AbortResponse {
	m.abortCnt++
	return &AbortResponse{
		TxnID:   request.TxnID,
		Success: true,
	}
}

func TestCoordinator_Commit_OneVoteNo(t *testing.T) {
	clock := NewHybridLogicalClock()

	participant1 := &MockParticipant{voteYes: true}
	participant2 := &MockParticipant{voteYes: false} // No を返す
	coordinator := NewCoordinator([]Participant{participant1, participant2}, clock)

	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{{Key: "key1", Value: []byte("value1")}}

	err := coordinator.Commit(txnID, writes)

	if err == nil {
		t.Error("Commit() should return error when one participant votes No")
	}

	// トランザクション状態を確認
	record := coordinator.transactions[txnID]
	if record.State != TxnAborted {
		t.Errorf("State = %v, want TxnAborted", record.State)
	}

	// Abort が呼ばれたことを確認
	if participant1.abortCnt == 0 && participant2.abortCnt == 0 {
		t.Error("Abort should be called on participants")
	}
}

func TestCoordinator_Commit_AllVoteYes(t *testing.T) {
	clock := NewHybridLogicalClock()

	participant1 := &MockParticipant{voteYes: true}
	participant2 := &MockParticipant{voteYes: true}
	coordinator := NewCoordinator([]Participant{participant1, participant2}, clock)

	txnID := coordinator.BeginTransaction(nil)
	writes := []WriteIntent{{Key: "key1", Value: []byte("value1")}}

	err := coordinator.Commit(txnID, writes)

	if err != nil {
		t.Errorf("Commit() error = %v, want nil", err)
	}

	// Commit が呼ばれたことを確認
	if participant1.commitCnt != 1 {
		t.Errorf("participant1.commitCnt = %d, want 1", participant1.commitCnt)
	}
	if participant2.commitCnt != 1 {
		t.Errorf("participant2.commitCnt = %d, want 1", participant2.commitCnt)
	}
}

func TestCoordinator_Concurrent(t *testing.T) {
	clock := NewHybridLogicalClock()
	store := NewMVCCStore(clock)
	participant := NewLocalParticipant("node-1", store)
	coordinator := NewCoordinator([]Participant{participant}, clock)

	var wg sync.WaitGroup
	numTxns := 10

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txnID := coordinator.BeginTransaction(nil)
			writes := []WriteIntent{
				{Key: "key", Value: []byte("value")},
			}
			coordinator.Commit(txnID, writes)
		}(i)
	}

	wg.Wait()
	// パニックしなければOK
}

func TestTxnState(t *testing.T) {
	tests := []struct {
		state    TxnState
		expected int
	}{
		{TxnPending, 0},
		{TxnPrepared, 1},
		{TxnCommitted, 2},
		{TxnAborted, 3},
	}

	for _, tt := range tests {
		if int(tt.state) != tt.expected {
			t.Errorf("TxnState %v = %d, want %d", tt.state, int(tt.state), tt.expected)
		}
	}
}
