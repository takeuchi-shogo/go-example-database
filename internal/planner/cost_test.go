package planner

import (
	"testing"
)

func TestNewCost(t *testing.T) {
	cost := NewCost(100.0, 10.0, 5.0, 2.0)
	if cost == nil {
		t.Fatal("NewCost returned nil")
	}

	if cost.GetRowCost() != 100.0 {
		t.Errorf("expected RowCost 100.0, got %f", cost.GetRowCost())
	}
}

func TestCostAddRowCost(t *testing.T) {
	cost := NewCost(100.0, 10.0, 5.0, 2.0)

	cost.AddRowCost(50.0)
	if cost.GetRowCost() != 150.0 {
		t.Errorf("expected RowCost 150.0 after AddRowCost, got %f", cost.GetRowCost())
	}

	cost.AddRowCost(25.0)
	if cost.GetRowCost() != 175.0 {
		t.Errorf("expected RowCost 175.0 after second AddRowCost, got %f", cost.GetRowCost())
	}
}

func TestCostMultiplyRowCount(t *testing.T) {
	cost := NewCost(100.0, 10.0, 5.0, 2.0)

	cost.MultiplyRowCount(0.5)
	if cost.GetRowCost() != 50.0 {
		t.Errorf("expected RowCost 50.0 after MultiplyRowCount(0.5), got %f", cost.GetRowCost())
	}

	cost.MultiplyRowCount(2.0)
	if cost.GetRowCost() != 100.0 {
		t.Errorf("expected RowCost 100.0 after MultiplyRowCount(2.0), got %f", cost.GetRowCost())
	}
}

func TestCostZeroValues(t *testing.T) {
	cost := NewCost(0.0, 0.0, 0.0, 0.0)
	if cost.GetRowCost() != 0.0 {
		t.Errorf("expected RowCost 0.0, got %f", cost.GetRowCost())
	}

	cost.AddRowCost(10.0)
	if cost.GetRowCost() != 10.0 {
		t.Errorf("expected RowCost 10.0 after AddRowCost, got %f", cost.GetRowCost())
	}

	cost.MultiplyRowCount(0.0)
	if cost.GetRowCost() != 0.0 {
		t.Errorf("expected RowCost 0.0 after MultiplyRowCount(0.0), got %f", cost.GetRowCost())
	}
}
