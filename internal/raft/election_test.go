package raft

import (
	"testing"
)

func TestRequestVote_GrantVote(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()

	request := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := node.RequestVote(request, log)

	if !response.VoteGranted {
		t.Error("VoteGranted should be true for valid request")
	}
	if node.GetVotedFor() != "node-2" {
		t.Errorf("GetVotedFor() = %s, want node-2", node.GetVotedFor())
	}
	if node.GetCurrentTerm() != 1 {
		t.Errorf("GetCurrentTerm() = %d, want 1", node.GetCurrentTerm())
	}
}

func TestRequestVote_RejectOldTerm(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(5)
	log := NewRaftLog()

	request := &RequestVoteRequest{
		Term:         3, // 古い任期
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := node.RequestVote(request, log)

	if response.VoteGranted {
		t.Error("VoteGranted should be false for old term")
	}
	if response.Term != 5 {
		t.Errorf("response.Term = %d, want 5", response.Term)
	}
}

func TestRequestVote_AlreadyVoted(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()

	// 最初の投票
	request1 := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	response1 := node.RequestVote(request1, log)
	if !response1.VoteGranted {
		t.Error("First vote should be granted")
	}

	// 別の候補者への投票は拒否
	request2 := &RequestVoteRequest{
		Term:         1, // 同じ任期
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	response2 := node.RequestVote(request2, log)
	if response2.VoteGranted {
		t.Error("Second vote to different candidate should be rejected")
	}
}

func TestRequestVote_SameCandidateAgain(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()

	request := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// 最初の投票
	response1 := node.RequestVote(request, log)
	if !response1.VoteGranted {
		t.Error("First vote should be granted")
	}

	// 同じ候補者への再投票は許可
	response2 := node.RequestVote(request, log)
	if !response2.VoteGranted {
		t.Error("Vote to same candidate should be granted")
	}
}

func TestRequestVote_NewerTermResetsVote(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(1)
	node.SetVotedFor("node-2")
	log := NewRaftLog()

	request := &RequestVoteRequest{
		Term:         2, // 新しい任期
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	response := node.RequestVote(request, log)

	if !response.VoteGranted {
		t.Error("VoteGranted should be true for newer term")
	}
	if node.GetVotedFor() != "node-3" {
		t.Errorf("GetVotedFor() = %s, want node-3", node.GetVotedFor())
	}
	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}
}

func TestRequestVote_RejectOutdatedLog(t *testing.T) {
	node := NewRaftNode("node-1")
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 2, Data: []byte("cmd1")})

	// 候補者のログが古い
	request := &RequestVoteRequest{
		Term:         3,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  1, // 自分より古い Term
	}

	response := node.RequestVote(request, log)

	if response.VoteGranted {
		t.Error("VoteGranted should be false for outdated log")
	}
}

func TestStartElection(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(1)
	log := NewRaftLog()
	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})

	request := node.StartElection(log)

	// 任期がインクリメント
	if request.Term != 2 {
		t.Errorf("request.Term = %d, want 2", request.Term)
	}
	if node.GetCurrentTerm() != 2 {
		t.Errorf("GetCurrentTerm() = %d, want 2", node.GetCurrentTerm())
	}

	// 状態が Candidate
	if node.GetState() != Candidate {
		t.Errorf("GetState() = %v, want Candidate", node.GetState())
	}

	// 自分に投票
	if node.GetVotedFor() != "node-1" {
		t.Errorf("GetVotedFor() = %s, want node-1", node.GetVotedFor())
	}

	// リクエスト内容
	if request.CandidateID != "node-1" {
		t.Errorf("request.CandidateID = %s, want node-1", request.CandidateID)
	}
	if request.LastLogIndex != 1 {
		t.Errorf("request.LastLogIndex = %d, want 1", request.LastLogIndex)
	}
	if request.LastLogTerm != 1 {
		t.Errorf("request.LastLogTerm = %d, want 1", request.LastLogTerm)
	}
}

func TestHandleVoteResponse_NewerTerm(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(1)
	node.SetState(Candidate)
	node.SetVotedFor("node-1")

	response := &RequestVoteResponse{
		Term:        3, // 自分より新しい
		VoteGranted: false,
	}

	node.HandleVoteResponse(response)

	if node.GetCurrentTerm() != 3 {
		t.Errorf("GetCurrentTerm() = %d, want 3", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}
	if node.GetVotedFor() != "" {
		t.Errorf("GetVotedFor() = %s, want empty", node.GetVotedFor())
	}
}

func TestHandleVoteResponse_SameTerm(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetCurrentTerm(2)
	node.SetState(Candidate)
	node.SetVotedFor("node-1")

	response := &RequestVoteResponse{
		Term:        2, // 同じ任期
		VoteGranted: true,
	}

	node.HandleVoteResponse(response)

	// 状態は変わらない
	if node.GetCurrentTerm() != 2 {
		t.Errorf("GetCurrentTerm() = %d, want 2", node.GetCurrentTerm())
	}
	if node.GetState() != Candidate {
		t.Errorf("GetState() = %v, want Candidate", node.GetState())
	}
}
