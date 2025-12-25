package raft

import (
	"testing"
	"time"
)

func TestHeartbeatConstants(t *testing.T) {
	// ハートビート間隔は選挙タイムアウトより短いこと
	if HeartbeatInterval >= ElectionTimeoutMin {
		t.Errorf("HeartbeatInterval (%v) should be less than ElectionTimeoutMin (%v)",
			HeartbeatInterval, ElectionTimeoutMin)
	}

	// 選挙タイムアウトの最小値は最大値より小さいこと
	if ElectionTimeoutMin >= ElectionTimeoutMax {
		t.Errorf("ElectionTimeoutMin (%v) should be less than ElectionTimeoutMax (%v)",
			ElectionTimeoutMin, ElectionTimeoutMax)
	}

	// ハートビート間隔が妥当な範囲であること
	if HeartbeatInterval < 50*time.Millisecond || HeartbeatInterval > 200*time.Millisecond {
		t.Errorf("HeartbeatInterval (%v) should be between 50ms and 200ms", HeartbeatInterval)
	}
}

func TestCreateHeartbeat(t *testing.T) {
	node := NewRaftNode("leader-1")
	node.SetCurrentTerm(5)
	node.SetState(Leader)

	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 3, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 5, Data: []byte("cmd2")})
	log.SetCommitIndex(1)

	heartbeat := node.CreateHeartbeat(log)

	// Term が正しいこと
	if heartbeat.Term != 5 {
		t.Errorf("heartbeat.Term = %d, want 5", heartbeat.Term)
	}

	// LeaderID が正しいこと
	if heartbeat.LeaderID != "leader-1" {
		t.Errorf("heartbeat.LeaderID = %s, want leader-1", heartbeat.LeaderID)
	}

	// PrevLogIndex が正しいこと
	if heartbeat.PrevLogIndex != 2 {
		t.Errorf("heartbeat.PrevLogIndex = %d, want 2", heartbeat.PrevLogIndex)
	}

	// PrevLogTerm が正しいこと
	if heartbeat.PrevLogTerm != 5 {
		t.Errorf("heartbeat.PrevLogTerm = %d, want 5", heartbeat.PrevLogTerm)
	}

	// Entries が空であること（ハートビートの特徴）
	if heartbeat.Entries != nil {
		t.Errorf("heartbeat.Entries should be nil for heartbeat")
	}

	// LeaderCommit が正しいこと
	if heartbeat.LeaderCommit != 1 {
		t.Errorf("heartbeat.LeaderCommit = %d, want 1", heartbeat.LeaderCommit)
	}
}

func TestCreateHeartbeat_EmptyLog(t *testing.T) {
	node := NewRaftNode("leader-1")
	node.SetCurrentTerm(1)
	node.SetState(Leader)

	log := NewRaftLog() // 空のログ

	heartbeat := node.CreateHeartbeat(log)

	if heartbeat.PrevLogIndex != 0 {
		t.Errorf("heartbeat.PrevLogIndex = %d, want 0", heartbeat.PrevLogIndex)
	}
	if heartbeat.PrevLogTerm != 0 {
		t.Errorf("heartbeat.PrevLogTerm = %d, want 0", heartbeat.PrevLogTerm)
	}
	if heartbeat.Entries != nil {
		t.Error("heartbeat.Entries should be nil")
	}
}

func TestHandleAppendEntriesResponse_NewerTerm(t *testing.T) {
	node := NewRaftNode("leader-1")
	node.SetCurrentTerm(3)
	node.SetState(Leader)
	node.SetVotedFor("leader-1")

	response := &AppendEntriesResponse{
		Term:    5, // 自分より新しい
		Success: false,
	}

	node.HandleAppendEntriesResponse(response)

	// Term が更新されること
	if node.GetCurrentTerm() != 5 {
		t.Errorf("GetCurrentTerm() = %d, want 5", node.GetCurrentTerm())
	}

	// Follower に戻ること
	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}

	// votedFor がリセットされること
	if node.GetVotedFor() != "" {
		t.Errorf("GetVotedFor() = %s, want empty", node.GetVotedFor())
	}
}

func TestHandleAppendEntriesResponse_SameTerm(t *testing.T) {
	node := NewRaftNode("leader-1")
	node.SetCurrentTerm(3)
	node.SetState(Leader)

	response := &AppendEntriesResponse{
		Term:    3, // 同じ
		Success: true,
	}

	node.HandleAppendEntriesResponse(response)

	// 状態は変わらないこと
	if node.GetCurrentTerm() != 3 {
		t.Errorf("GetCurrentTerm() = %d, want 3", node.GetCurrentTerm())
	}
	if node.GetState() != Leader {
		t.Errorf("GetState() = %v, want Leader", node.GetState())
	}
}

func TestHandleAppendEntriesResponse_OlderTerm(t *testing.T) {
	node := NewRaftNode("leader-1")
	node.SetCurrentTerm(5)
	node.SetState(Leader)

	response := &AppendEntriesResponse{
		Term:    3, // 自分より古い
		Success: false,
	}

	node.HandleAppendEntriesResponse(response)

	// 状態は変わらないこと
	if node.GetCurrentTerm() != 5 {
		t.Errorf("GetCurrentTerm() = %d, want 5", node.GetCurrentTerm())
	}
	if node.GetState() != Leader {
		t.Errorf("GetState() = %v, want Leader", node.GetState())
	}
}
