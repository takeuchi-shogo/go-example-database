package index

const (
	order   = 4               // B+Tree の order (1つのノードに格納できるキーの最大数)
	maxKeys = order - 1       // 最大キー数
	minKeys = (order - 1) / 2 // 最小キー数
)

// nodeType はノードの種類を表す
type nodeType uint8

const (
	nodeTypeLeaf     nodeType = iota // リーフノード
	nodeTypeInternal                 // 内部ノード
)

type bTreeNode struct {
	nodeType nodeType
	keys     []int64
	// リーフノードの場合：値
	values []int64
	// 内部ノードの場合：子ノードへのポインタ
	children []*bTreeNode
	// リーフノード間のリンク（範囲検索用）
	next *bTreeNode
	prev *bTreeNode
	// 親ノードへのポインタ
	parent *bTreeNode
}

type bTree struct {
	root *bTreeNode
}

func NewBTree() *bTree {
	return &bTree{
		root: &bTreeNode{
			nodeType: nodeTypeLeaf,
			keys:     make([]int64, 0, maxKeys),
			values:   make([]int64, 0, maxKeys),
		},
	}
}

func (n *bTreeNode) isLeaf() bool {
	return n.nodeType == nodeTypeLeaf
}

func (n *bTreeNode) isFull() bool {
	return len(n.keys) == maxKeys
}

func (n *bTreeNode) keyCount() int {
	return len(n.keys)
}

func (n *bTreeNode) findKeyIndex(key int64) int {
	lo, hi := 0, n.keyCount()
	for lo < hi {
		mid := (lo + hi) / 2
		if n.keys[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (t *bTree) Search(key int64) (int64, bool) {
	return t.root.search(key)
}

func (n *bTreeNode) search(key int64) (int64, bool) {
	// キーの位置を二分探索で特定
	i := n.findKeyIndex(key)

	if n.isLeaf() {
		// キーが一致すれば値を返す
		if i < n.keyCount() && n.keys[i] == key {
			return n.values[i], true
		}
		return -1, false
	}

	// 内部ノードの場合
	// B+Tree では内部ノードのキーは「右の子ツリーの最小値」を表す
	// key < keys[i] なら children[i] へ
	// key >= keys[i] なら children[i+1] へ
	if i < n.keyCount() && key >= n.keys[i] {
		return n.children[i+1].search(key)
	}
	return n.children[i].search(key)
}

func (t *bTree) Insert(key int64, value int64) {
	// ノードが満杯なら分割
	if t.root.isFull() {
		oldRoot := t.root
		t.root = &bTreeNode{
			nodeType: nodeTypeInternal,
			keys:     make([]int64, 0, maxKeys),
			children: []*bTreeNode{oldRoot},
		}
		oldRoot.parent = t.root
		t.splitChild(t.root, 0)
	}

	// 非満杯のノードを探して挿入
	t.insertNonFull(t.root, key, value)
}

func (t *bTree) splitChild(parent *bTreeNode, childIndex int) {
	child := parent.children[childIndex]
	mid := maxKeys / 2 // 中央のキー

	// 新しいノードを作成（子ノードと同じ種類）
	newNode := &bTreeNode{
		nodeType: child.nodeType,
		keys:     make([]int64, 0, maxKeys),
		parent:   parent,
	}

	// 昇格
	// 中央のキーを親に移動
	promotedKey := child.keys[mid]
	// 親のキー配列に昇格キーを挿入
	parent.keys = insertAt(parent.keys, childIndex, promotedKey)
	// 親の子ノード配列に新しいノードを追加
	parent.children = insertAt(parent.children, childIndex+1, newNode)

	// リーフノードの場合
	if child.isLeaf() {
		// リーフノード：中央のキーも新しいノードに含める（B+Tree の特性）
		newNode.keys = append(newNode.keys, child.keys[mid:]...)
		child.keys = child.keys[:mid]
		newNode.values = append(newNode.values, child.values[mid:]...)
		child.values = child.values[:mid]
		// リンクを更新
		if child.next != nil {
			child.next.prev = newNode
		}
		newNode.next = child.next
		child.next = newNode
		newNode.prev = child
	} else {
		// 内部ノードの場合：中央のキーは親に昇格するので、新ノードには含めない
		newNode.keys = append(newNode.keys, child.keys[mid+1:]...)
		child.keys = child.keys[:mid]
		newNode.children = append(newNode.children, child.children[mid+1:]...)
		child.children = child.children[:mid+1]
		// 移動した子ノードの親を更新
		for _, c := range newNode.children {
			c.parent = newNode
		}
	}
}

func (t *bTree) insertNonFull(node *bTreeNode, key int64, value int64) {
	i := node.findKeyIndex(key)

	if node.isLeaf() {
		// リーフノードの場合：キーと値を挿入
		node.keys = insertAt(node.keys, i, key)
		node.values = insertAt(node.values, i, value)
	} else {
		// 内部ノードの場合：子ノードを探して挿入
		if node.children[i].isFull() {
			t.splitChild(node, i)
			// 分割後、どちらの子に入れるか再判定
			if key >= node.keys[i] {
				i++
			}
		}
		t.insertNonFull(node.children[i], key, value)
	}
}

func insertAt[T any](slice []T, i int, value T) []T {
	// スライスの末尾に値を追加
	slice = append(slice, value)
	// 指定位置以降の要素を1つずらす
	copy(slice[i+1:], slice[i:])
	// 指定位置に値を挿入
	slice[i] = value
	return slice
}
