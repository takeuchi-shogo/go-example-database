package planner

type Cost interface {
	// GetRowCost は推定行数（RowCost）を返す
	GetRowCost() float64
	AddRowCost(rowCost float64)
	MultiplyRowCount(factor float64)
}

// cost はコストを表す
type cost struct {
	RowCost    float64 // 予想行数
	CPUCost    float64 // 予想CPUコスト
	IOCost     float64 // 予想入出力コスト
	MemoryCost float64 // 予想メモリコスト
}

func NewCost(rowCost, cpuCost, ioCost, memoryCost float64) Cost {
	return &cost{
		RowCost:    rowCost,
		CPUCost:    cpuCost,
		IOCost:     ioCost,
		MemoryCost: memoryCost,
	}
}

// AddRowCount は行数を追加する
func (c *cost) AddRowCost(rowCost float64) {
	c.RowCost += rowCost
}

func (c *cost) GetRowCost() float64 {
	return c.RowCost
}

// MultiplyRowCount は行数を乗算する
func (c *cost) MultiplyRowCount(factor float64) {
	c.RowCost *= factor
}
