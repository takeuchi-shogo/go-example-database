package raft

import (
	"testing"
)

func TestNewRaftLog(t *testing.T) {
	log := NewRaftLog()

	if log.LastIndex() != 0 {
		t.Errorf("LastIndex() = %d, want 0", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("LastTerm() = %d, want 0", log.LastTerm())
	}
	if log.GetCommitIndex() != 0 {
		t.Errorf("GetCommitIndex() = %d, want 0", log.GetCommitIndex())
	}
}

func TestRaftLog_Append(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 2, Data: []byte("cmd3")})

	if log.LastIndex() != 3 {
		t.Errorf("LastIndex() = %d, want 3", log.LastIndex())
	}
	if log.LastTerm() != 2 {
		t.Errorf("LastTerm() = %d, want 2", log.LastTerm())
	}

	entries := log.GetEntries()
	if len(entries) != 3 {
		t.Errorf("len(GetEntries()) = %d, want 3", len(entries))
	}
}

func TestRaftLog_GetEntry(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 2, Data: []byte("cmd2")})

	// 存在するエントリ
	entry := log.GetEntry(0)
	if entry == nil {
		t.Fatal("GetEntry(0) should not be nil")
	}
	if entry.Term != 1 {
		t.Errorf("GetEntry(0).Term = %d, want 1", entry.Term)
	}

	entry = log.GetEntry(1)
	if entry == nil {
		t.Fatal("GetEntry(1) should not be nil")
	}
	if entry.Term != 2 {
		t.Errorf("GetEntry(1).Term = %d, want 2", entry.Term)
	}

	// 存在しないエントリ
	entry = log.GetEntry(99)
	if entry != nil {
		t.Error("GetEntry(99) should be nil")
	}
}

func TestRaftLog_GetEntriesFrom(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 2, Data: []byte("cmd3")})

	// インデックス1から取得
	entries := log.GetEntriesFrom(1)
	if len(entries) != 2 {
		t.Errorf("len(GetEntriesFrom(1)) = %d, want 2", len(entries))
	}

	// インデックス0から取得（全部）
	entries = log.GetEntriesFrom(0)
	if len(entries) != 3 {
		t.Errorf("len(GetEntriesFrom(0)) = %d, want 3", len(entries))
	}

	// 範囲外
	entries = log.GetEntriesFrom(99)
	if entries != nil {
		t.Error("GetEntriesFrom(99) should be nil")
	}
}

func TestRaftLog_CommitIndex(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 2, Data: []byte("cmd3")})

	log.SetCommitIndex(2)
	if log.GetCommitIndex() != 2 {
		t.Errorf("GetCommitIndex() = %d, want 2", log.GetCommitIndex())
	}

	committed := log.GetCommittedEntries()
	if len(committed) != 3 {
		t.Errorf("len(GetCommittedEntries()) = %d, want 3", len(committed))
	}
}

func TestRaftLog_MarkApplied(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.MarkApplied(1)

	// lastApplied は直接取得できないが、エラーがないことを確認
}

func TestRaftLog_Truncate(t *testing.T) {
	log := NewRaftLog()

	log.Append(&LogEntry{Index: 1, Term: 1, Data: []byte("cmd1")})
	log.Append(&LogEntry{Index: 2, Term: 1, Data: []byte("cmd2")})
	log.Append(&LogEntry{Index: 3, Term: 2, Data: []byte("cmd3")})

	// インデックス1までを残す（2, 3を削除）
	log.Truncate(1)

	if log.LastIndex() != 2 {
		t.Errorf("LastIndex() = %d, want 2", log.LastIndex())
	}

	entries := log.GetEntries()
	if len(entries) != 2 {
		t.Errorf("len(GetEntries()) = %d, want 2", len(entries))
	}
}

func TestRaftLog_EmptyLog(t *testing.T) {
	log := NewRaftLog()

	// 空のログでパニックしないことを確認
	if log.LastIndex() != 0 {
		t.Errorf("LastIndex() = %d, want 0", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("LastTerm() = %d, want 0", log.LastTerm())
	}
	if log.GetEntry(0) != nil {
		t.Error("GetEntry(0) should be nil for empty log")
	}
	if log.GetEntriesFrom(0) != nil {
		t.Error("GetEntriesFrom(0) should be nil for empty log")
	}
	if log.GetCommittedEntries() != nil {
		t.Error("GetCommittedEntries() should be nil for empty log")
	}
}
