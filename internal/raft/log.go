package raft

import "sync"

// LogEntry is a entry in the Raft log.
type LogEntry struct {
	Index uint64 // ログのインデックス
	Term  uint64 // 任期
	Data  []byte // コマンドデータ
}

type RaftLog struct {
	mu          sync.Mutex
	entries     []LogEntry // ログエントリーのリスト
	commitIndex uint64     // コミットされた最大インデックス
	lastApplied uint64     // 適応済みの最大インデックス
}

// NewRaftLog is a constructor for RaftLog.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries:     make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
	}
}

// Append is a method to append a new entry to the log.
func (l *RaftLog) Append(entry *LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, *entry)
}

// GetEntry is a method to get an entry from the log.
func (l *RaftLog) GetEntry(index uint64) *LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index >= uint64(len(l.entries)) {
		return nil
	}
	return &l.entries[index]
}

// GetEntriesFrom is a method to get the entries from the log.
func (l *RaftLog) GetEntriesFrom(index uint64) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[index:]
}

// GetEntries is a method to get the entries from the log.
func (l *RaftLog) GetEntries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries
}

// LastIndex is a method to get the last index of the log.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return 0
	}
	return uint64(len(l.entries))
}

// LastTerm is a method to get the last term of the log.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// SetCommitIndex is a method to set the commit index of the log.
func (l *RaftLog) SetCommitIndex(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commitIndex = index
}

// GetCommitIndex is a method to get the commit index of the log.
func (l *RaftLog) GetCommitIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.commitIndex
}

// GetCommitIndex is a method to get the commit index of the log.
func (l *RaftLog) GetCommittedEntries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.commitIndex == 0 || len(l.entries) == 0 {
		return nil
	}
	if l.commitIndex >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[:l.commitIndex+1]
}

// MarkApplied is a method to mark the applied index of the log.
func (l *RaftLog) MarkApplied(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastApplied = index
}

// Truncate is a method to truncate the log.
func (l *RaftLog) Truncate(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = l.entries[:index+1]
}
