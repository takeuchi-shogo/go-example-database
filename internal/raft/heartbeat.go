package raft

import "time"

const (
	// ハートビートの間隔
	HeartbeatInterval = 100 * time.Millisecond
	// 選挙のタイムアウトの最小値
	ElectionTimeoutMin = 300 * time.Millisecond
	// 選挙のタイムアウトの最大値
	ElectionTimeoutMax = 500 * time.Millisecond
)

// CreateHeartbeat is a method to create a heartbeat request.
func (n *RaftNode) CreateHeartbeat(log *RaftLog) *AppendEntriesRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.nodeID,
		PrevLogIndex: log.LastIndex(),
		PrevLogTerm:  log.LastTerm(),
		Entries:      nil,
		LeaderCommit: log.GetCommitIndex(),
	}
}

// HandleAppendEntriesResponse is a method to handle an append entries response.
func (n *RaftNode) HandleAppendEntriesResponse(response *AppendEntriesResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// 相手の Term が大きい → Follower に戻る
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.votedFor = ""
		n.state = Follower
	}
}
