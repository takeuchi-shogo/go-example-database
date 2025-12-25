package replication

import (
	"math/rand"
	"sync"
	"time"

	"github.com/takeuchi-shogo/go-example-database/internal/raft"
)

// Transport is an interface for sending messages to other nodes.
type Transport interface {
	SendAppendEntries(peer string, request *raft.AppendEntriesRequest) *raft.AppendEntriesResponse
	SendRequestVote(peer string, request *raft.RequestVoteRequest) *raft.RequestVoteResponse
}

// ReplicationManager is a manager for the replication process.
type ReplicationManager struct {
	mu sync.Mutex

	node *raft.RaftNode
	log  *raft.RaftLog

	peers     []string  // 他ノードのアドレス
	transport Transport // メッセージ送信用のインターフェース
	// タイマー
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	// 状態
	running bool
	stopCh  chan struct{}
}

// NewReplicationManager is a constructor for ReplicationManager.
func NewReplicationManager(nodeID string, peers []string, transport Transport) *ReplicationManager {
	return &ReplicationManager{
		node:      raft.NewRaftNode(nodeID),
		log:       raft.NewRaftLog(),
		peers:     peers,
		transport: transport,
		stopCh:    make(chan struct{}),
	}
}

func (m *ReplicationManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return
	}
	m.running = true
	// タイマーを設定
	m.resetElectionTimer()
	m.heartbeatTimer = time.NewTicker(raft.HeartbeatInterval)
	// メインループを開始
	go m.run()
}

func (m *ReplicationManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running {
		return
	}
	m.running = false
	// タイマーを停止
	if m.electionTimer != nil {
		m.electionTimer.Stop()
	}
	if m.heartbeatTimer != nil {
		m.heartbeatTimer.Stop()
	}
	close(m.stopCh)
}

// run is the main loop for the replication process.
func (m *ReplicationManager) run() {
	for {
		select {
		case <-m.electionTimer.C:
			// 選挙タイムアウト → 選挙開始
			m.startElection()
		case <-m.heartbeatTimer.C:
			// Leader → Follower へのハートビート送信
			if m.node.GetState() == raft.Leader {
				m.sendHeartbeats()
			}
		case <-m.stopCh:
			return
		}
	}
}

// resetElectionTimer is a method to reset the election timer.
func (m *ReplicationManager) resetElectionTimer() {
	timeout := raft.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(raft.ElectionTimeoutMax-raft.ElectionTimeoutMin)))
	if m.electionTimer == nil {
		m.electionTimer = time.NewTimer(timeout)
	} else {
		m.electionTimer.Reset(timeout)
	}
}

// startElection is a method to start an election.
func (m *ReplicationManager) startElection() {
	// Follower → Candidate に遷移
	state := m.node.GetState()
	// Leader は選挙を開始しない
	if state == raft.Leader {
		return
	}
	// 選挙を開始
	request := m.node.StartElection(m.log)
	// 自分に投票リクエストを送信
	voteCount := 1
	// 自分を含めた過半数の投票を得るために必要な票数
	needed := (len(m.peers)+1)/2 + 1
	// 他のノードに投票リクエストを送信
	for _, peer := range m.peers {
		if m.transport == nil {
			continue
		}
		response := m.transport.SendRequestVote(peer, request)
		// 投票結果を処理
		m.node.HandleVoteResponse(response)
		if response.Term > request.Term {
			// 相手の Term が大きい → Follower に戻る
			return
		}
		if response.VoteGranted {
			voteCount++
		}
		// 状態が変わっていたら中断
		if m.node.GetState() != raft.Candidate {
			return
		}
		// 過半数の投票を得た → Leader に遷移
		if voteCount >= needed {
			m.node.BecomeLeader()
			m.sendHeartbeats()
			return
		}
	}
	// 選挙失敗 → タイムアウトをリセット
	m.resetElectionTimer()
}

// sendHeartbeat is a method to send a heartbeat to the followers.
func (m *ReplicationManager) sendHeartbeats() {
	if m.node.GetState() != raft.Leader {
		return
	}
	request := m.node.CreateHeartbeat(m.log)
	for _, peer := range m.peers {
		if m.transport == nil {
			continue
		}
		response := m.transport.SendAppendEntries(peer, request)
		// レスポンスを処理
		m.node.HandleAppendEntriesResponse(response)
		// 状態が変わっていたら中断
		if m.node.GetState() != raft.Leader {
			return
		}
	}
}

// HandleAppendEntries is a method to handle an append entries response.
func (m *ReplicationManager) HandleAppendEntries(request *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
	response := m.node.AppendEntries(request, m.log)
	// 有効なリクエストを受信した → タイマー継続
	if response.Success || request.Term >= m.node.GetCurrentTerm() {
		m.resetElectionTimer()
	}
	return response
}

// HandleRequestVote is a method to handle a request vote response.
func (m *ReplicationManager) HandleRequestVote(request *raft.RequestVoteRequest) *raft.RequestVoteResponse {
	response := m.node.RequestVote(request, m.log)
	// 投票したらタイマーリセット
	if response.VoteGranted {
		m.resetElectionTimer()
	}
	return response
}
