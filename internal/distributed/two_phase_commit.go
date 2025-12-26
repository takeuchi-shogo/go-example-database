package distributed

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

// TxnState - トランザクションの状態
type TxnState int

const (
	TxnPending   TxnState = iota // 開始前
	TxnPrepared                  // 準備完了
	TxnCommitted                 // コミット完了
	TxnAborted                   // 中止完了
)

// TxnID - トランザクションID
type TxnID string

type PrepareRequest struct {
	TxnID     TxnID
	Writes    []WriteIntent
	Timestamp Timestamp
}

// WriteIntent - 書き込み予定のデータ
type WriteIntent struct {
	Key   string
	Value []byte
}

// PrepareResponse - 準備応答
type PrepareResponse struct {
	TxnID  TxnID
	Vote   bool   // true = 投票する, false = 投票しない
	Reason string // No の場合の理由
}

// CommitRequest - コミット要求
type CommitRequest struct {
	TxnID     TxnID
	Timestamp Timestamp
}

// CommitResponse - コミット応答
type CommitResponse struct {
	TxnID   TxnID
	Success bool
}

// Coordinator - 2PC のコーディネーター
type Coordinator struct {
	mu           sync.Mutex
	participants []Participant        // 参加者のリスト
	transactions map[TxnID]*TxnRecord // トランザクションの記録
	clock        *HybridLogicalClock
}

func (c *Coordinator) BeginTransaction(writes []WriteIntent) TxnID {
	c.mu.Lock()
	defer c.mu.Unlock()
	// ユニークなトランザクションIDを生成
	txnID := TxnID(uuid.New().String())
	c.transactions[txnID] = &TxnRecord{
		State:        TxnPending,
		Timestamp:    c.clock.Now(),
		Participants: []string{},
	}
	return txnID
}

func (c *Coordinator) Commit(txnID TxnID, writes []WriteIntent) error {
	// 1. Prepare フェーズ
	ts := c.clock.Now()
	allYes := true
	for _, participant := range c.participants {
		resp := participant.Prepare(&PrepareRequest{
			TxnID:     txnID,
			Writes:    writes,
			Timestamp: ts,
		})
		if !resp.Vote {
			allYes = false
			break
		}
	}
	// 2. Commit or Abort フェーズ
	if !allYes {
		// 一人でも No → Abort
		for _, participant := range c.participants {
			_ = participant.Abort(&AbortRequest{
				TxnID: txnID,
			})
		}
		c.mu.Lock()
		c.transactions[txnID].State = TxnAborted
		c.mu.Unlock()

		return errors.New("transaction aborted")
	}
	for _, participant := range c.participants {
		_ = participant.Commit(&CommitRequest{
			TxnID:     txnID,
			Timestamp: ts,
		})
	}
	c.mu.Lock()
	c.transactions[txnID].State = TxnCommitted
	c.mu.Unlock()

	return nil
}

// TxnRecord - トランザクションの記録
type TxnRecord struct {
	State        TxnState
	Timestamp    Timestamp
	Participants []string // 参加ノードの ID
}

// Participant - 参加者
type Participant interface {
	Prepare(request *PrepareRequest) *PrepareResponse
	Commit(request *CommitRequest) *CommitResponse
	Abort(request *AbortRequest) *AbortResponse
}

// LocalParticipant - ローカル参加者
type LocalParticipant struct {
	mu          sync.Mutex
	nodeID      string
	store       *MVCCStore
	preparedTxn map[TxnID]*PrepareData // Prepare 済みデータ
}

func NewLocalParticipant(nodeID string, store *MVCCStore) *LocalParticipant {
	return &LocalParticipant{
		nodeID:      nodeID,
		store:       store,
		preparedTxn: make(map[TxnID]*PrepareData),
	}
}

// PrepareData - Prepare 済みデータ
type PrepareData struct {
	Writes    []WriteIntent
	Timestamp Timestamp
}

// Prepare - コミット可能か確認
func (p *LocalParticipant) Prepare(request *PrepareRequest) *PrepareResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 1. 書き込み競合をチェック
	// TODO: 実際には書き込みロックの取得が必要
	// 2. 書き込み予定のデータを保存
	p.preparedTxn[request.TxnID] = &PrepareData{
		Writes:    request.Writes,
		Timestamp: request.Timestamp,
	}
	return &PrepareResponse{
		TxnID: request.TxnID,
		Vote:  true,
	}
}

// Commit - コミット
func (p *LocalParticipant) Commit(request *CommitRequest) *CommitResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 1. 書き込み予定のデータを取得
	prepared, ok := p.preparedTxn[request.TxnID]
	if !ok {
		return &CommitResponse{
			TxnID:   request.TxnID,
			Success: false,
		}
	}
	// 2. MVCC ストアに書き込み
	for _, w := range prepared.Writes {
		p.store.Put(w.Key, w.Value)
	}
	// 3. prepare 済みのデータを削除
	delete(p.preparedTxn, request.TxnID)
	return &CommitResponse{
		TxnID:   request.TxnID,
		Success: true,
	}
}

// AbortRequest - 中止要求
type AbortRequest struct {
	TxnID TxnID
}

// AbortResponse - 中止応答
type AbortResponse struct {
	TxnID   TxnID
	Success bool
}

// Abort - 中止（ロールバック）
func (p *LocalParticipant) Abort(request *AbortRequest) *AbortResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 1. prepare 済みのデータを削除
	delete(p.preparedTxn, request.TxnID)
	return &AbortResponse{
		TxnID:   request.TxnID,
		Success: true,
	}
}

func NewCoordinator(participants []Participant, clock *HybridLogicalClock) *Coordinator {
	return &Coordinator{
		participants: participants,
		transactions: make(map[TxnID]*TxnRecord),
		clock:        clock,
	}
}
