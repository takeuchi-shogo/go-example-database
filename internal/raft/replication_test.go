package raft

import (
	"testing"
)

func TestAppendEntries_RejectOldTerm(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(5)
	log := NewRaftLog()

	request := &AppendEntriesRequest{
		Term:         3, // 古い任期
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if response.Success {
		t.Error("Success should be false for old term")
	}
	if response.Term != 5 {
		t.Errorf("response.Term = %d, want 5", response.Term)
	}
}

func TestAppendEntries_AcceptAndBecomeFollower(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(1)
	node.SetState(Candidate)
	log := NewRaftLog()

	request := &AppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Success should be true")
	}
	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}
	if node.GetCurrentTerm() != 2 {
		t.Errorf("GetCurrentTerm() = %d, want 2", node.GetCurrentTerm())
	}
	if node.GetLeaderID() != "leader-1" {
		t.Errorf("GetLeaderID() = %s, want leader-1", node.GetLeaderID())
	}
}

func TestAppendEntries_RejectMismatchPrevLog(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})

	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // Term が一致しない
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if response.Success {
		t.Error("Success should be false for mismatched PrevLogTerm")
	}
}

func TestAppendEntries_RejectMissingPrevLog(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	// ログが空

	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 5, // 存在しない
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if response.Success {
		t.Error("Success should be false for missing PrevLog")
	}
}

func TestAppendEntries_AppendNewEntries(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()

	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Data: []byte("cmd1")},
			{Index: 2, Term: 1, Data: []byte("cmd2")},
		},
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Success should be true")
	}
	if log.LastIndex() != 2 {
		t.Errorf("LastIndex() = %d, want 2", log.LastIndex())
	}
}

func TestAppendEntries_TruncateConflict(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 1, Data: []byte("cmd3")}) // これが競合

	request := &AppendEntriesRequest{
		Term:         2,
		LeaderID:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 3, Term: 2, Data: []byte("new-cmd3")}, // Term が違う
		},
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Success should be true")
	}
	if log.LastIndex() != 3 {
		t.Errorf("LastIndex() = %d, want 3", log.LastIndex())
	}
	entry := log.GetEntry(2) // 0-based index for entry with Index=3
	if entry.Term != 2 {
		t.Errorf("entry.Term = %d, want 2", entry.Term)
	}
}

func TestAppendEntries_UpdateCommitIndex(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 1, Data: []byte("cmd3")})

	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 2, // コミットインデックスを更新
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Success should be true")
	}
	if log.GetCommitIndex() != 2 {
		t.Errorf("GetCommitIndex() = %d, want 2", log.GetCommitIndex())
	}
}

func TestAppendEntries_CommitIndexMinLastIndex(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})

	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 5, // LastIndex より大きい
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Success should be true")
	}
	// min(5, 2) = 2
	if log.GetCommitIndex() != 2 {
		t.Errorf("GetCommitIndex() = %d, want 2", log.GetCommitIndex())
	}
}

func TestAppendEntries_Heartbeat(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetState(Follower)
	log := NewRaftLog()

	// 空のエントリ = ハートビート
	request := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	response := node.AppendEntries(request, log)

	if !response.Success {
		t.Error("Heartbeat should succeed")
	}
	if node.GetLeaderID() != "leader-1" {
		t.Errorf("GetLeaderID() = %s, want leader-1", node.GetLeaderID())
	}
}
