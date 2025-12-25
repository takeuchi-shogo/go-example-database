package raft

// AppendEntriesRequest is a request to append entries to a follower.
type AppendEntriesRequest struct {
	Term         uint64     // リーダーの任期
	LeaderID     string     // LeaderのID
	PrevLogIndex uint64     // 直前のログインデックス
	PrevLogTerm  uint64     // 直前のログ任期
	Entries      []LogEntry // 追加するログエントリー
	LeaderCommit uint64     // リーダーのコミットインデックス
}

// AppendEntriesResponse is a response to an append entries request.
type AppendEntriesResponse struct {
	Term    uint64 // リーダーの任期
	Success bool   // 追加が成功したかどうか
}

// AppendEntries is a method to append entries to a follower.
func (n *RaftNode) AppendEntries(request *AppendEntriesRequest, log *RaftLog) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.currentTerm {
		// 拒否
		return &AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}
	if request.Term >= n.currentTerm {
		// Follower に遷移、任期更新
		n.currentTerm = request.Term
		n.state = Follower
		n.leaderID = request.LeaderID
		n.votedFor = ""
	}
	// PrevLogIndex / PrevLogTerm のチェック
	if request.PrevLogIndex > 0 {
		prevEntry := log.GetEntry(request.PrevLogIndex - 1) // 0-based index
		if prevEntry == nil || prevEntry.Term != request.PrevLogTerm {
			return &AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}
		}
	}
	// ログの競合チェック
	// 既存エントリと Term が異なる → Truncate で削除
	for i, entry := range request.Entries {
		index := int(request.PrevLogIndex) + i
		existingEntry := log.GetEntry(uint64(index))
		if existingEntry != nil && existingEntry.Term != entry.Term {
			// 競合 → 以降を削除
			if index > 0 {
				log.Truncate(uint64(index - 1))
			}
		}
		if existingEntry == nil || existingEntry.Term != entry.Term {
			// 新しいエントリを追加
			log.Append(&entry)
		}
	}
	// commitIndex の更新
	if request.LeaderCommit > log.GetCommitIndex() {
		// min(LeaderCommit, 最後のエントリのインデックス)
		newCommit := request.LeaderCommit
		if log.LastIndex() < newCommit {
			newCommit = log.LastIndex()
		}
		log.SetCommitIndex(newCommit)
	}
	// 成功
	return &AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}
}
