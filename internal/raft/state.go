package raft

import "sync"

// NodeState is the state of a node in the Raft cluster.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

type RaftNode struct {
	mu sync.Mutex
	// 永続化される状態
	currentTerm uint64 // 現在の任期
	votedFor    string // 投票したノードID
	// 揮発性の状態
	state    NodeState // ノードの状態
	nodeID   string    // ノードID
	leaderID string    // リーダーのノードID
}

// NewRaftNode is a constructor for RaftNode.
func NewRaftNode(nodeID string) *RaftNode {
	return &RaftNode{
		nodeID:      nodeID,
		state:       Follower, // 最初はフォロワー
		currentTerm: 0,
		votedFor:    "",
	}
}

// =============================== Getter/Setter ===============================
func (n *RaftNode) SetState(state NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = state
}

func (n *RaftNode) GetState() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *RaftNode) SetVotedFor(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.votedFor = nodeID
}

func (n *RaftNode) GetVotedFor() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

func (n *RaftNode) SetCurrentTerm(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = term
}

func (n *RaftNode) GetCurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *RaftNode) SetLeaderID(leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderID = leaderID
}

func (n *RaftNode) GetLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *RaftNode) GetNodeID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodeID
}

// =============================== 状態遷移 ===============================
func (n *RaftNode) BecomeFollower(leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Follower
	n.leaderID = leaderID
}

func (n *RaftNode) BecomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Candidate
}

func (n *RaftNode) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Leader
}
