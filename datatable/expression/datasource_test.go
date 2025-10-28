// Copyright 2025 Magnus Pierre
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/magpierre/fyne-datatable/datatable"
)

// mockDataSource is a simple in-memory data source for testing.
type mockDataSource struct {
	columnNames []string
	columnTypes []datatable.DataType
	data        [][]any
}

func newMockDataSource(columnNames []string, columnTypes []datatable.DataType, data [][]any) *mockDataSource {
	return &mockDataSource{
		columnNames: columnNames,
		columnTypes: columnTypes,
		data:        data,
	}
}

func (m *mockDataSource) RowCount() int {
	return len(m.data)
}

func (m *mockDataSource) ColumnCount() int {
	return len(m.columnNames)
}

func (m *mockDataSource) ColumnName(col int) (string, error) {
	if col < 0 || col >= len(m.columnNames) {
		return "", datatable.ErrInvalidColumn
	}
	return m.columnNames[col], nil
}

func (m *mockDataSource) ColumnType(col int) (datatable.DataType, error) {
	if col < 0 || col >= len(m.columnTypes) {
		return datatable.TypeString, datatable.ErrInvalidColumn
	}
	return m.columnTypes[col], nil
}

func (m *mockDataSource) Cell(row, col int) (datatable.Value, error) {
	if row < 0 || row >= len(m.data) {
		return datatable.Value{}, datatable.ErrInvalidRow
	}
	if col < 0 || col >= len(m.columnNames) {
		return datatable.Value{}, datatable.ErrInvalidColumn
	}

	value := m.data[row][col]
	if value == nil {
		return datatable.NewNullValue(m.columnTypes[col]), nil
	}

	return datatable.NewValue(value, m.columnTypes[col]), nil
}

func (m *mockDataSource) Row(row int) ([]datatable.Value, error) {
	if row < 0 || row >= len(m.data) {
		return nil, datatable.ErrInvalidRow
	}

	values := make([]datatable.Value, len(m.columnNames))
	for col := range m.columnNames {
		val, err := m.Cell(row, col)
		if err != nil {
			return nil, err
		}
		values[col] = val
	}

	return values, nil
}

func (m *mockDataSource) Metadata() datatable.Metadata {
	return datatable.Metadata{}
}

// Tests

func TestNewExpressionDataSource(t *testing.T) {
	source := newMockDataSource(
		[]string{"name", "age"},
		[]datatable.DataType{datatable.TypeString, datatable.TypeInt},
		[][]any{
			{"Alice", int64(25)},
			{"Bob", int64(30)},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Should have same number of columns as source
	if ds.ColumnCount() != 2 {
		t.Errorf("ColumnCount() = %d, want 2", ds.ColumnCount())
	}

	// Should have same row count as source
	if ds.RowCount() != 2 {
		t.Errorf("RowCount() = %d, want 2", ds.RowCount())
	}

	// Column names should match
	name, _ := ds.ColumnName(0)
	if name != "name" {
		t.Errorf("ColumnName(0) = %s, want name", name)
	}
}

func TestAddComputedColumn(t *testing.T) {
	source := newMockDataSource(
		[]string{"price", "quantity"},
		[]datatable.DataType{datatable.TypeFloat, datatable.TypeInt},
		[][]any{
			{10.0, int64(2)},
			{20.0, int64(3)},
			{30.0, int64(4)},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Create expression: price * quantity
	expr, err := NewExpression(
		"price * quantity",
		[]string{"price", "quantity"},
		arrow.PrimitiveTypes.Float64,
	)
	if err != nil {
		t.Fatalf("NewExpression() error = %v", err)
	}

	// Add computed column
	err = ds.AddComputedColumn("total", expr, datatable.TypeFloat)
	if err != nil {
		t.Fatalf("AddComputedColumn() error = %v", err)
	}

	// Should have 3 columns now
	if ds.ColumnCount() != 3 {
		t.Errorf("ColumnCount() = %d, want 3", ds.ColumnCount())
	}

	// Column name should be "total"
	name, _ := ds.ColumnName(2)
	if name != "total" {
		t.Errorf("ColumnName(2) = %s, want total", name)
	}
}

func TestLazyEvaluation(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add computed column: x * 2
	expr, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("doubled", expr, datatable.TypeInt)

	// Column should not be materialized yet
	if ds.IsMaterialized("doubled") {
		t.Error("Column should not be materialized before access")
	}

	// Access a cell - should trigger materialization
	val, err := ds.Cell(0, 1)
	if err != nil {
		t.Fatalf("Cell() error = %v", err)
	}

	if val.Raw.(int64) != 2 {
		t.Errorf("Cell(0, 1) = %v, want 2", val.Raw)
	}

	// Column should now be materialized
	if !ds.IsMaterialized("doubled") {
		t.Error("Column should be materialized after access")
	}

	// Verify all values
	expected := []int64{2, 4, 6}
	for i, want := range expected {
		val, _ := ds.Cell(i, 1)
		got := val.Raw.(int64)
		if got != want {
			t.Errorf("Cell(%d, 1) = %v, want %v", i, got, want)
		}
	}
}

func TestExplicitMaterialization(t *testing.T) {
	source := newMockDataSource(
		[]string{"value"},
		[]datatable.DataType{datatable.TypeFloat},
		[][]any{
			{1.5},
			{2.5},
			{3.5},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add computed column
	expr, _ := NewExpression("value * 2", []string{"value"}, arrow.PrimitiveTypes.Float64)
	ds.AddComputedColumn("doubled", expr, datatable.TypeFloat)

	// Explicitly materialize
	err := ds.Materialize("doubled")
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}

	// Should be materialized
	if !ds.IsMaterialized("doubled") {
		t.Error("Column should be materialized")
	}

	// Access should be fast now (cached)
	val, _ := ds.Cell(0, 1)
	if val.Raw.(float64) != 3.0 {
		t.Errorf("Cell(0, 1) = %v, want 3.0", val.Raw)
	}
}

func TestUnmaterialize(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{{int64(5)}},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	expr, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("doubled", expr, datatable.TypeInt)

	// Materialize
	ds.Materialize("doubled")
	if !ds.IsMaterialized("doubled") {
		t.Error("Column should be materialized")
	}

	// Unmaterialize
	err := ds.Unmaterialize("doubled")
	if err != nil {
		t.Fatalf("Unmaterialize() error = %v", err)
	}

	// Should not be materialized anymore
	if ds.IsMaterialized("doubled") {
		t.Error("Column should not be materialized after Unmaterialize")
	}
}

func TestReferenceColumns(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{
			{int64(2)},
			{int64(3)},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add first computed column: x * 2
	expr1, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("doubled", expr1, datatable.TypeInt)

	// Add second computed column that references first: doubled * 3
	expr2, _ := NewExpression("doubled * 3", []string{"doubled"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("tripled", expr2, datatable.TypeInt)

	// Access the second computed column
	val, err := ds.Cell(0, 2)
	if err != nil {
		t.Fatalf("Cell() error = %v", err)
	}

	// Should be (2 * 2) * 3 = 12
	if val.Raw.(int64) != 12 {
		t.Errorf("Cell(0, 2) = %v, want 12", val.Raw)
	}

	// Second row: (3 * 2) * 3 = 18
	val, _ = ds.Cell(1, 2)
	if val.Raw.(int64) != 18 {
		t.Errorf("Cell(1, 2) = %v, want 18", val.Raw)
	}
}

func TestCircularDependency(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{{int64(1)}},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add column A
	exprA, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("A", exprA, datatable.TypeInt)

	// Try to add column B that depends on A
	exprB, _ := NewExpression("A * 2", []string{"A"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("B", exprB, datatable.TypeInt)

	// Now try to modify A to depend on B (should create cycle)
	exprACircular, _ := NewExpression("B * 2", []string{"B"}, arrow.PrimitiveTypes.Int64)
	err := ds.SetColumnExpression("A", exprACircular)

	if err == nil {
		t.Error("SetColumnExpression() should return error for circular dependency")
	}
}

func TestGetDependencies(t *testing.T) {
	source := newMockDataSource(
		[]string{"a", "b"},
		[]datatable.DataType{datatable.TypeInt, datatable.TypeInt},
		[][]any{{int64(1), int64(2)}},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add computed column: a + b
	expr, _ := NewExpression("a + b", []string{"a", "b"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("sum", expr, datatable.TypeInt)

	// Get dependencies
	deps := ds.GetDependencies("sum")

	if len(deps) != 2 {
		t.Errorf("GetDependencies() returned %d dependencies, want 2", len(deps))
	}

	// Check that a and b are in dependencies
	hasA, hasB := false, false
	for _, dep := range deps {
		if dep == "a" {
			hasA = true
		}
		if dep == "b" {
			hasB = true
		}
	}

	if !hasA || !hasB {
		t.Errorf("GetDependencies() = %v, want [a, b]", deps)
	}
}

func TestRemoveColumn(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{{int64(5)}},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add computed column
	expr, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("doubled", expr, datatable.TypeInt)

	if ds.ColumnCount() != 2 {
		t.Errorf("ColumnCount() = %d, want 2", ds.ColumnCount())
	}

	// Remove computed column
	err := ds.RemoveColumn("doubled")
	if err != nil {
		t.Fatalf("RemoveColumn() error = %v", err)
	}

	if ds.ColumnCount() != 1 {
		t.Errorf("ColumnCount() = %d, want 1 after removal", ds.ColumnCount())
	}

	// Try to remove source column (should fail)
	err = ds.RemoveColumn("x")
	if err == nil {
		t.Error("RemoveColumn() should fail for source columns")
	}
}

func TestMaterializeAll(t *testing.T) {
	source := newMockDataSource(
		[]string{"x"},
		[]datatable.DataType{datatable.TypeInt},
		[][]any{
			{int64(1)},
			{int64(2)},
		},
	)

	ds := NewExpressionDataSource(source)
	defer ds.Release()

	// Add multiple computed columns
	expr1, _ := NewExpression("x * 2", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("col1", expr1, datatable.TypeInt)

	expr2, _ := NewExpression("x * 3", []string{"x"}, arrow.PrimitiveTypes.Int64)
	ds.AddComputedColumn("col2", expr2, datatable.TypeInt)

	// Materialize all
	err := ds.Materialize("")
	if err != nil {
		t.Fatalf("Materialize(\"\") error = %v", err)
	}

	// Both should be materialized
	if !ds.IsMaterialized("col1") {
		t.Error("col1 should be materialized")
	}
	if !ds.IsMaterialized("col2") {
		t.Error("col2 should be materialized")
	}
}
