package raft

import (
	"testing"
)

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{NodeState(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestNewRaftNode(t *testing.T) {
	nodeID := "node-1"
	node := NewRaftNode(nodeID)

	if node.GetNodeID() != nodeID {
		t.Errorf("GetNodeID() = %s, want %s", node.GetNodeID(), nodeID)
	}

	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}

	if node.GetCurrentTerm() != 0 {
		t.Errorf("GetCurrentTerm() = %d, want 0", node.GetCurrentTerm())
	}

	if node.GetVotedFor() != "" {
		t.Errorf("GetVotedFor() = %s, want empty", node.GetVotedFor())
	}

	if node.GetLeaderID() != "" {
		t.Errorf("GetLeaderID() = %s, want empty", node.GetLeaderID())
	}
}

func TestRaftNode_SettersAndGetters(t *testing.T) {
	node := NewRaftNode("node-1")

	// SetState / GetState
	node.SetState(Candidate)
	if node.GetState() != Candidate {
		t.Errorf("GetState() = %v, want Candidate", node.GetState())
	}

	// SetCurrentTerm / GetCurrentTerm
	node.SetCurrentTerm(5)
	if node.GetCurrentTerm() != 5 {
		t.Errorf("GetCurrentTerm() = %d, want 5", node.GetCurrentTerm())
	}

	// SetVotedFor / GetVotedFor
	node.SetVotedFor("node-2")
	if node.GetVotedFor() != "node-2" {
		t.Errorf("GetVotedFor() = %s, want node-2", node.GetVotedFor())
	}

	// SetLeaderID / GetLeaderID
	node.SetLeaderID("leader-1")
	if node.GetLeaderID() != "leader-1" {
		t.Errorf("GetLeaderID() = %s, want leader-1", node.GetLeaderID())
	}
}

func TestRaftNode_BecomeFollower(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetState(Leader)

	node.BecomeFollower("leader-2")

	if node.GetState() != Follower {
		t.Errorf("GetState() = %v, want Follower", node.GetState())
	}
	if node.GetLeaderID() != "leader-2" {
		t.Errorf("GetLeaderID() = %s, want leader-2", node.GetLeaderID())
	}
}

func TestRaftNode_BecomeCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	node.BecomeCandidate()

	if node.GetState() != Candidate {
		t.Errorf("GetState() = %v, want Candidate", node.GetState())
	}
}

func TestRaftNode_BecomeLeader(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetState(Candidate)

	node.BecomeLeader()

	if node.GetState() != Leader {
		t.Errorf("GetState() = %v, want Leader", node.GetState())
	}
}

func TestRaftNode_StateTransitions(t *testing.T) {
	node := NewRaftNode("node-1")

	// Follower → Candidate → Leader → Follower
	if node.GetState() != Follower {
		t.Fatal("Initial state should be Follower")
	}

	node.BecomeCandidate()
	if node.GetState() != Candidate {
		t.Fatal("State should be Candidate after BecomeCandidate()")
	}

	node.BecomeLeader()
	if node.GetState() != Leader {
		t.Fatal("State should be Leader after BecomeLeader()")
	}

	node.BecomeFollower("new-leader")
	if node.GetState() != Follower {
		t.Fatal("State should be Follower after BecomeFollower()")
	}
	if node.GetLeaderID() != "new-leader" {
		t.Fatal("LeaderID should be 'new-leader' after BecomeFollower()")
	}
}
