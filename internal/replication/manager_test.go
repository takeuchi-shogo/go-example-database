package replication

import (
	"testing"
	"time"

	"github.com/takeuchi-shogo/go-example-database/internal/raft"
)

// MockTransport - テスト用のモック Transport
type MockTransport struct {
	appendEntriesResponse *raft.AppendEntriesResponse
	requestVoteResponse   *raft.RequestVoteResponse
	appendEntriesCalls    int
	requestVoteCalls      int
}

func (t *MockTransport) SendAppendEntries(peer string, request *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
	t.appendEntriesCalls++
	if t.appendEntriesResponse != nil {
		return t.appendEntriesResponse
	}
	return &raft.AppendEntriesResponse{Term: request.Term, Success: true}
}

func (t *MockTransport) SendRequestVote(peer string, request *raft.RequestVoteRequest) *raft.RequestVoteResponse {
	t.requestVoteCalls++
	if t.requestVoteResponse != nil {
		return t.requestVoteResponse
	}
	return &raft.RequestVoteResponse{Term: request.Term, VoteGranted: true}
}

func TestNewReplicationManager(t *testing.T) {
	peers := []string{"node-2", "node-3"}
	transport := &MockTransport{}

	manager := NewReplicationManager("node-1", peers, transport)

	if manager.node == nil {
		t.Error("node should not be nil")
	}
	if manager.log == nil {
		t.Error("log should not be nil")
	}
	if len(manager.peers) != 2 {
		t.Errorf("len(peers) = %d, want 2", len(manager.peers))
	}
}

func TestReplicationManager_StartStop(t *testing.T) {
	peers := []string{"node-2", "node-3"}
	transport := &MockTransport{}

	manager := NewReplicationManager("node-1", peers, transport)

	// Start
	manager.Start()

	if !manager.running {
		t.Error("running should be true after Start()")
	}
	if manager.electionTimer == nil {
		t.Error("electionTimer should not be nil after Start()")
	}
	if manager.heartbeatTimer == nil {
		t.Error("heartbeatTimer should not be nil after Start()")
	}

	// 少し待つ
	time.Sleep(10 * time.Millisecond)

	// Stop
	manager.Stop()

	if manager.running {
		t.Error("running should be false after Stop()")
	}
}

func TestReplicationManager_HandleAppendEntries(t *testing.T) {
	peers := []string{"node-2", "node-3"}
	transport := &MockTransport{}

	manager := NewReplicationManager("node-1", peers, transport)

	request := &raft.AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := manager.HandleAppendEntries(request)

	if !response.Success {
		t.Error("Success should be true for valid heartbeat")
	}
}

func TestReplicationManager_HandleRequestVote(t *testing.T) {
	peers := []string{"node-2", "node-3"}
	transport := &MockTransport{}

	manager := NewReplicationManager("node-1", peers, transport)

	request := &raft.RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := manager.HandleRequestVote(request)

	if !response.VoteGranted {
		t.Error("VoteGranted should be true for valid request")
	}
}

func TestReplicationManager_QuorumCalculation(t *testing.T) {
	tests := []struct {
		peerCount int
		expected  int // 必要な票数
	}{
		{1, 2},  // 2 nodes total, need 2 votes
		{2, 2},  // 3 nodes total, need 2 votes
		{3, 3},  // 4 nodes total, need 3 votes
		{4, 3},  // 5 nodes total, need 3 votes
		{5, 4},  // 6 nodes total, need 4 votes
	}

	for _, tt := range tests {
		peers := make([]string, tt.peerCount)
		for i := 0; i < tt.peerCount; i++ {
			peers[i] = "node"
		}
		needed := (len(peers)+1)/2 + 1
		if needed != tt.expected {
			t.Errorf("peerCount=%d: needed=%d, want %d", tt.peerCount, needed, tt.expected)
		}
	}
}

func TestReplicationManager_NilTransport(t *testing.T) {
	peers := []string{"node-2", "node-3"}

	// transport が nil でもパニックしないこと
	manager := NewReplicationManager("node-1", peers, nil)

	manager.Start()
	time.Sleep(10 * time.Millisecond)
	manager.Stop()

	// パニックしなければ OK
}
