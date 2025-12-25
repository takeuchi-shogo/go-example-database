package raft

// RequestVoteRequest is a request to request a vote from a follower.
type RequestVoteRequest struct {
	Term         uint64 // 候補者の任期
	CandidateID  string // 候補者のID
	LastLogIndex uint64 // 候補者の最後のログインデックス
	LastLogTerm  uint64 // 候補者の最後のログ任期
}

// RequestVoteResponse is a response to a request vote request.
type RequestVoteResponse struct {
	Term        uint64 // リーダーの任期
	VoteGranted bool   // 投票が許可されたかどうか
}

// RequestVote is a method to request a vote from a follower.
func (n *RaftNode) RequestVote(request *RequestVoteRequest, log *RaftLog) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := &RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}
	// 候補者の任期が古い → 候補者になる
	if request.Term < n.currentTerm {
		return response
	}
	// 候補者の任期が新しい → リーダーになる
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.votedFor = ""
		n.state = Follower
	}
	// すでに他の候補者に投票している → 候補者になる
	// （votedFor が 0 = 未投票、または同じ候補者なら OK）
	// votedFor が 0 の場合は、候補者に投票する
	myLastTerm := log.LastTerm()
	myLastIndex := log.LastIndex()
	logOk := (request.LastLogTerm > myLastTerm || (request.LastLogTerm == myLastTerm && request.LastLogIndex >= myLastIndex))
	if (n.votedFor == "" || n.votedFor == request.CandidateID) && logOk {
		n.votedFor = request.CandidateID // 投票を記録
		response.VoteGranted = true
	}
	// 候補者のログが最新
	return response
}

// StartElection is a method to start an election.
func (n *RaftNode) StartElection(log *RaftLog) *RequestVoteRequest {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.nodeID
	return &RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.nodeID,
		LastLogIndex: log.LastIndex(),
		LastLogTerm:  log.LastTerm(),
	}
}

// RequestVoteResponse is a response to a request vote request.
func (n *RaftNode) HandleVoteResponse(response *RequestVoteResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.votedFor = ""
		n.state = Follower
	}
}
