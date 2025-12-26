/**
* Multi-Version Concurrency Control (MVCC)
* 複数バージョンの同時実行制御
* 分散システム用のMVCC実装
*
* MVCCは、複数バージョンの同時実行制御を実現するための技術です。
* 分散システム用のMVCC実装は、分散システム用のMVCC実装です。
* 1. 書き込み：新しいバージョンを作成
* 2. 読み込み：トランザクション開始時点のバージョンを読み込む
 */
package distributed

import "sync"

// Version - データの1バージョン
type Version struct {
	Timestamp Timestamp // このバージョンのタイムスタンプ
	Data      []byte    // このバージョンのデータ
	Deleted   bool      // このバージョンが削除されたかどうか
	Next      *Version  // 次に古いバージョンへのポインタ
}

// MVCCValue - データのMVCC管理
type MVCCValue struct {
	mu       sync.RWMutex
	versions *Version // 最新のバージョンへのポインタ
}

// MVCCStore - データのMVCCストア
type MVCCStore struct {
	mu    sync.RWMutex
	clock *HybridLogicalClock
	data  map[string]*MVCCValue // key -> versions
}

// NewMVCCStore - 新しいMVCCストアを作成
func NewMVCCStore(clock *HybridLogicalClock) *MVCCStore {
	return &MVCCStore{
		clock: clock,
		data:  make(map[string]*MVCCValue),
	}
}

func (s *MVCCStore) Put(key string, data []byte) Timestamp {
	// 1. 最新のバージョンを取得
	ts := s.clock.Now()
	// 2. バージョンを取得
	newVersion := &Version{
		Timestamp: ts,
		Data:      data,
		Deleted:   false,
		Next:      nil, // 最新のバージョンなので、次に古いバージョンは存在しない
	}
	// 3. ストアをロック
	s.mu.Lock()
	defer s.mu.Unlock()
	// 4. バージョンを取得
	value, ok := s.data[key]
	if !ok {
		// 新規キー → 新しい MVCCValue を作成
		s.data[key] = &MVCCValue{
			versions: newVersion,
		}
		return ts
	}
	// 5. 既存キー → バージョンチェーンの先頭に挿入
	value.mu.Lock()         // 書き込みロック
	defer value.mu.Unlock() // 書き込みロックを解除
	// 現在の先頭を新バージョンの Next に設定
	newVersion.Next = value.versions
	// 新バージョンを先頭に設定
	value.versions = newVersion
	return ts
}

func (s *MVCCStore) Get(key string, readTs Timestamp) ([]byte, bool) {
	// 1. ストアから MVCCValue を取得
	s.mu.RLock() // 読み込みロック
	mvccValue, ok := s.data[key]
	s.mu.RUnlock() // 読み込みロックを解除
	if !ok {
		return nil, false // キーが存在しない
	}
	// 2. バージョンチェーンをトラバース
	mvccValue.mu.RLock()         // 読み込みロック
	defer mvccValue.mu.RUnlock() // 読み込みロックを解除
	currentVersion := mvccValue.versions
	for currentVersion != nil {
		// readTs より古いバージョンはスキップ
		// Compare: -1 = curent < readTs, 0 = equal, 1 = curent
		if currentVersion.Timestamp.Compare(readTs) <= 0 {
			// このバージョンが見える
			if currentVersion.Deleted {
				return nil, false // 削除されている
			}
			return currentVersion.Data, true // データを返す
		}
		// 次のバージョンを取得
		currentVersion = currentVersion.Next
	}
	return nil, false // 読み込み時点で見えるバージョンが存在しない
}

func (s *MVCCStore) Delete(key string) Timestamp {
	// 1. タイムスタンプの取得
	ts := s.clock.Now()
	// 2. 削除マーカーを作成
	tombstone := &Version{
		Timestamp: ts,
		Data:      nil,
		Deleted:   true,
		Next:      nil, // 最新のバージョンなので、次に古いバージョンは存在しない
	}
	// 3. ストアをロック
	s.mu.Lock()
	defer s.mu.Unlock()
	// 4. バージョンを取得
	value, ok := s.data[key]
	if !ok {
		// 存在しないキーを削除 → 何もしないか、tombstone を挿入
		s.data[key] = &MVCCValue{
			versions: tombstone,
		}
		return ts
	}
	// 5. 既存キー → バージョンチェーンの先頭に挿入
	value.mu.Lock()
	defer value.mu.Unlock()
	// 現在の先頭を新バージョンの Next に設定
	tombstone.Next = value.versions
	// 新バージョンを先頭に設定
	value.versions = tombstone
	// 6. バージョンを取得
	return ts
}
