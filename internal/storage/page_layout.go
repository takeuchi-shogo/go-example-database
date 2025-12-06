package storage

import (
	"encoding/binary"
	"errors"
)

var (
	ErrPageFull      = errors.New("page is full")
	ErrInvalidSlotID = errors.New("invalid slot ID")
	ErrSlotDeleted   = errors.New("slot is deleted")
)

const (
	// ページヘッダーのサイズ
	// rowCount(2) + freeSpaceStart(2) + freeSpaceEnd(2) = 6 bytes
	PageHeaderSize = 6

	// スロット1つのサイズ
	// offset(2) + size(2) = 4 bytes
	SlotSize = 4

	// 削除されたスロットを示す
	DeletedSlotOffset = 0xFFFF
)

// SlottedPage はスロット配列方式のページ
type SlottedPage struct {
	data [pageSize]byte
}

// NewSlottedPage は新しいスロットページを作成
func NewSlottedPage() *SlottedPage {
	p := &SlottedPage{}
	p.setRowCount(0)
	p.setFreeSpaceStart(PageHeaderSize) // ヘッダー直後から
	p.setFreeSpaceEnd(uint16(pageSize)) // ページ末尾から
	return p
}

// LoadSlottedPage は既存のページデータを読み込む
func LoadSlottedPage(data [pageSize]byte) *SlottedPage {
	p := &SlottedPage{data: data}
	return p
}

// ============ ヘッダー操作 ============

// rowCount: 行数（スロット数）
func (p *SlottedPage) rowCount() uint16 {
	return binary.LittleEndian.Uint16(p.data[0:2])
}

func (p *SlottedPage) setRowCount(count uint16) {
	binary.LittleEndian.PutUint16(p.data[0:2], count)
}

// freeSpaceStart: 空き領域の開始位置（スロット配列の末尾）
func (p *SlottedPage) freeSpaceStart() uint16 {
	return binary.LittleEndian.Uint16(p.data[2:4])
}

func (p *SlottedPage) setFreeSpaceStart(offset uint16) {
	binary.LittleEndian.PutUint16(p.data[2:4], offset)
}

// freeSpaceEnd: 空き領域の終了位置（行データの先頭）
func (p *SlottedPage) freeSpaceEnd() uint16 {
	return binary.LittleEndian.Uint16(p.data[4:6])
}

func (p *SlottedPage) setFreeSpaceEnd(offset uint16) {
	binary.LittleEndian.PutUint16(p.data[4:6], offset)
}

// ============ スロット操作 ============

// getSlot はスロット情報を取得
func (p *SlottedPage) getSlot(slotID uint16) (offset uint16, size uint16) {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	offset = binary.LittleEndian.Uint16(p.data[slotPos : slotPos+2])
	size = binary.LittleEndian.Uint16(p.data[slotPos+2 : slotPos+4])
	return
}

// setSlot はスロット情報を設定
func (p *SlottedPage) setSlot(slotID uint16, offset uint16, size uint16) {
	slotPos := PageHeaderSize + (slotID * SlotSize)
	binary.LittleEndian.PutUint16(p.data[slotPos:slotPos+2], offset)
	binary.LittleEndian.PutUint16(p.data[slotPos+2:slotPos+4], size)
}

// ============ 空き容量計算 ============

// FreeSpace は空き容量を返す
func (p *SlottedPage) FreeSpace() uint16 {
	return p.freeSpaceEnd() - p.freeSpaceStart()
}

// ============ 行の追加・取得・削除 ============

// InsertRow は行を追加し、スロットIDを返す
func (p *SlottedPage) InsertRow(rowData []byte) (uint16, error) {
	rowSize := uint16(len(rowData))

	// 必要な空き容量: スロット(4byte) + 行データ
	needed := SlotSize + rowSize
	if p.FreeSpace() < needed {
		return 0, ErrPageFull
	}

	// 新しいスロットID
	slotID := p.rowCount()

	// 行データを末尾から格納
	newFreeSpaceEnd := p.freeSpaceEnd() - rowSize
	copy(p.data[newFreeSpaceEnd:], rowData)

	// スロットを設定
	p.setSlot(slotID, newFreeSpaceEnd, rowSize)

	// ヘッダー更新
	p.setRowCount(slotID + 1)
	p.setFreeSpaceStart(p.freeSpaceStart() + SlotSize)
	p.setFreeSpaceEnd(newFreeSpaceEnd)

	return slotID, nil
}

// GetRow は指定スロットIDの行データを取得
func (p *SlottedPage) GetRow(slotID uint16) ([]byte, error) {
	if slotID >= p.rowCount() {
		return nil, ErrInvalidSlotID
	}

	offset, size := p.getSlot(slotID)

	// 削除チェック
	if offset == DeletedSlotOffset {
		return nil, ErrSlotDeleted
	}

	// データをコピーして返す
	rowData := make([]byte, size)
	copy(rowData, p.data[offset:offset+size])

	return rowData, nil
}

// DeleteRow は指定スロットIDの行を削除（論理削除）
func (p *SlottedPage) DeleteRow(slotID uint16) error {
	if slotID >= p.rowCount() {
		return ErrInvalidSlotID
	}

	// スロットを削除マーク
	p.setSlot(slotID, DeletedSlotOffset, 0)

	return nil
}

// ============ ページデータ ============

// Data はページの生データを返す
func (p *SlottedPage) Data() [pageSize]byte {
	return p.data
}

// RowCount は行数を返す
func (p *SlottedPage) RowCount() uint16 {
	return p.rowCount()
}
