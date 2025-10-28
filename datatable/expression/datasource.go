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
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/magpierre/fyne-datatable/datatable"
)

// ExpressionDataSource wraps a DataSource and adds expression-based computed columns.
// It implements the datatable.DataSource interface with additional expression capabilities.
//
// Features:
//   - Lazy evaluation: columns are computed on first access
//   - Materialization: computed results are cached for subsequent access
//   - Reference columns: computed columns can reference other computed columns
//   - Dependency tracking: automatically detects circular dependencies
type ExpressionDataSource struct {
	source              datatable.DataSource
	columns             []ColumnDefinition
	materializedColumns map[int]arrow.Array
	dependencyGraph     *DependencyGraph
	allocator           memory.Allocator
	mu                  sync.RWMutex
}

// NewExpressionDataSource creates a new expression-based data source.
// It initializes with pass-through columns from the underlying source.
func NewExpressionDataSource(source datatable.DataSource) *ExpressionDataSource {
	ds := &ExpressionDataSource{
		source:              source,
		columns:             make([]ColumnDefinition, 0),
		materializedColumns: make(map[int]arrow.Array),
		allocator:           memory.NewGoAllocator(),
	}

	// Initialize with pass-through columns from source
	for i := 0; i < source.ColumnCount(); i++ {
		name, _ := source.ColumnName(i)
		colType, _ := source.ColumnType(i)
		sourceIdx := i

		ds.columns = append(ds.columns, ColumnDefinition{
			Name:         name,
			Type:         colType,
			SourceColumn: &sourceIdx,
			Expression:   nil,
			Materialized: false,
		})
	}

	// Build initial dependency graph
	ds.rebuildDependencyGraph()

	return ds
}

// AddComputedColumn adds a new computed column to the data source.
//
// Parameters:
//   - name: the column name (must be unique)
//   - expression: the compiled expression
//   - outputType: the expected data type of results
//
// Returns an error if:
//   - A column with the same name already exists
//   - The expression references unknown columns
//   - Adding this column would create a circular dependency
func (ds *ExpressionDataSource) AddComputedColumn(name string, expression *Expression, outputType datatable.DataType) error {
	return ds.AddComputedColumnWithDescription(name, expression, outputType, "")
}

// AddComputedColumnWithDescription adds a computed column with a description.
func (ds *ExpressionDataSource) AddComputedColumnWithDescription(
	name string,
	expression *Expression,
	outputType datatable.DataType,
	description string,
) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Check for duplicate name
	for _, col := range ds.columns {
		if col.Name == name {
			return fmt.Errorf("column %s already exists", name)
		}
	}

	// Validate expression references
	for _, inputCol := range expression.InputColumns() {
		if !ds.hasColumnLocked(inputCol) {
			return fmt.Errorf("expression references unknown column: %s", inputCol)
		}
	}

	// Create column definition
	newCol := ColumnDefinition{
		Name:         name,
		Type:         outputType,
		SourceColumn: nil,
		Expression:   expression,
		Materialized: false,
		Description:  description,
		Metadata:     make(map[string]any),
	}

	// Add to columns
	ds.columns = append(ds.columns, newCol)

	// Rebuild dependency graph to check for cycles
	if err := ds.rebuildDependencyGraph(); err != nil {
		// Roll back the addition
		ds.columns = ds.columns[:len(ds.columns)-1]
		return fmt.Errorf("cannot add column: %w", err)
	}

	return nil
}

// SetColumnExpression sets or updates an expression on an existing column.
func (ds *ExpressionDataSource) SetColumnExpression(colName string, expr *Expression) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return ErrColumnNotFound(colName)
	}

	// Clear materialization if changing expression
	if ds.columns[colIdx].Materialized {
		ds.unmaterializeColumnLocked(colIdx)
	}

	ds.columns[colIdx].Expression = expr
	ds.columns[colIdx].Materialized = false

	// Rebuild dependency graph
	if err := ds.rebuildDependencyGraph(); err != nil {
		return err
	}

	return nil
}

// RemoveColumn removes a column from the data source.
// Only computed columns can be removed (not source columns).
func (ds *ExpressionDataSource) RemoveColumn(colName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return ErrColumnNotFound(colName)
	}

	// Cannot remove source columns
	if ds.columns[colIdx].IsPassThrough() && ds.columns[colIdx].SourceColumn != nil {
		sourceIdx := *ds.columns[colIdx].SourceColumn
		if sourceIdx < ds.source.ColumnCount() {
			return fmt.Errorf("cannot remove source column %s", colName)
		}
	}

	// Check if any other columns depend on this one
	if ds.dependencyGraph != nil {
		dependents := ds.dependencyGraph.GetDependents(colName)
		if len(dependents) > 0 {
			return fmt.Errorf("cannot remove column %s: columns %v depend on it", colName, dependents)
		}
	}

	// Release materialized data if any
	if ds.columns[colIdx].Materialized {
		ds.unmaterializeColumnLocked(colIdx)
	}

	// Remove the column
	ds.columns = append(ds.columns[:colIdx], ds.columns[colIdx+1:]...)

	// Rebuild dependency graph
	ds.rebuildDependencyGraph()

	return nil
}

// Materialize explicitly materializes (caches) a column or all columns.
// Pass empty string to materialize all computed columns.
func (ds *ExpressionDataSource) Materialize(colName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if colName == "" {
		// Materialize all computed columns
		for i := range ds.columns {
			if ds.columns[i].IsComputed() && !ds.columns[i].Materialized {
				if err := ds.materializeColumnLocked(i); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Materialize specific column
	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return ErrColumnNotFound(colName)
	}

	if !ds.columns[colIdx].IsComputed() {
		return fmt.Errorf("column %s is not computed", colName)
	}

	return ds.materializeColumnLocked(colIdx)
}

// Unmaterialize removes the cached data for a column, forcing re-evaluation on next access.
func (ds *ExpressionDataSource) Unmaterialize(colName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return ErrColumnNotFound(colName)
	}

	ds.unmaterializeColumnLocked(colIdx)
	return nil
}

// IsMaterialized returns true if the column is currently materialized.
func (ds *ExpressionDataSource) IsMaterialized(colName string) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return false
	}

	return ds.columns[colIdx].Materialized
}

// GetDependencies returns the columns that the given column depends on.
func (ds *ExpressionDataSource) GetDependencies(colName string) []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if ds.dependencyGraph == nil {
		return []string{}
	}

	return ds.dependencyGraph.GetDependencies(colName)
}

// GetDependents returns the columns that depend on the given column.
func (ds *ExpressionDataSource) GetDependents(colName string) []string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if ds.dependencyGraph == nil {
		return []string{}
	}

	return ds.dependencyGraph.GetDependents(colName)
}

// Helper methods (must hold lock when calling these)

func (ds *ExpressionDataSource) hasColumnLocked(name string) bool {
	for _, col := range ds.columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func (ds *ExpressionDataSource) findColumnIndexLocked(name string) int {
	for i, col := range ds.columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (ds *ExpressionDataSource) rebuildDependencyGraph() error {
	graph, err := NewDependencyGraph(ds.columns)
	if err != nil {
		return err
	}
	ds.dependencyGraph = graph
	return nil
}

func (ds *ExpressionDataSource) unmaterializeColumnLocked(colIdx int) {
	if arr, exists := ds.materializedColumns[colIdx]; exists {
		arr.Release()
		delete(ds.materializedColumns, colIdx)
	}
	ds.columns[colIdx].Materialized = false
}

// materializeColumnLocked computes and caches a column's values.
// This is where the actual expression evaluation happens.
func (ds *ExpressionDataSource) materializeColumnLocked(colIdx int) error {
	// Already materialized?
	if ds.columns[colIdx].Materialized {
		return nil
	}

	colDef := ds.columns[colIdx]
	if colDef.Expression == nil {
		return fmt.Errorf("cannot materialize column without expression")
	}

	// Get input columns as Arrow arrays
	inputArrays := make([]arrow.Array, len(colDef.Expression.InputColumns()))
	for i, inputColName := range colDef.Expression.InputColumns() {
		arr, err := ds.getColumnAsArrowLocked(inputColName)
		if err != nil {
			return fmt.Errorf("failed to get input column %s: %w", inputColName, err)
		}
		inputArrays[i] = arr
		// Note: we don't defer Release() here because getColumnAsArrowLocked may return
		// a cached array that we don't own
	}

	// Execute expression
	result, err := colDef.Expression.Evaluate(inputArrays, ds.allocator)
	if err != nil {
		return fmt.Errorf("failed to evaluate expression: %w", err)
	}

	// Cache result
	ds.materializedColumns[colIdx] = result
	ds.columns[colIdx].Materialized = true

	return nil
}

// getColumnAsArrowLocked retrieves a column as an Arrow array.
// For materialized columns, returns the cached array.
// For non-materialized computed columns, materializes them first.
// For source columns, converts them to Arrow format.
func (ds *ExpressionDataSource) getColumnAsArrowLocked(colName string) (arrow.Array, error) {
	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return nil, ErrColumnNotFound(colName)
	}

	colDef := ds.columns[colIdx]

	// If materialized, return cached array
	if colDef.Materialized {
		if arr, exists := ds.materializedColumns[colIdx]; exists {
			return arr, nil
		}
	}

	// If source column, convert to Arrow
	if colDef.IsPassThrough() {
		return ds.sourceColumnToArrowLocked(*colDef.SourceColumn, colDef.Type)
	}

	// If computed but not materialized, materialize it first
	if colDef.IsComputed() {
		if err := ds.materializeColumnLocked(colIdx); err != nil {
			return nil, err
		}
		return ds.materializedColumns[colIdx], nil
	}

	return nil, fmt.Errorf("cannot convert column %s to Arrow", colName)
}

// sourceColumnToArrowLocked converts a source column to an Arrow array.
func (ds *ExpressionDataSource) sourceColumnToArrowLocked(sourceColIdx int, colType datatable.DataType) (arrow.Array, error) {
	// Build Arrow array from source data
	rowCount := ds.source.RowCount()

	// Determine Arrow type from datatable type
	arrowType := datatypeToArrow(colType)
	builder := array.NewBuilder(ds.allocator, arrowType)
	defer builder.Release()

	// Read all values and build array
	for row := 0; row < rowCount; row++ {
		val, err := ds.source.Cell(row, sourceColIdx)
		if err != nil {
			return nil, err
		}

		if val.IsNull {
			builder.AppendNull()
		} else {
			if err := appendValueToBuilder(builder, val.Raw, arrowType); err != nil {
				return nil, err
			}
		}
	}

	return builder.NewArray(), nil
}

// GetMaterializedArrowArray returns the materialized Arrow array for a specific column.
// This allows direct access to the Arrow array after materialization.
func (ds *ExpressionDataSource) GetMaterializedArrowArray(colName string) (arrow.Array, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	colIdx := ds.findColumnIndexLocked(colName)
	if colIdx == -1 {
		return nil, ErrColumnNotFound(colName)
	}

	if !ds.columns[colIdx].Materialized {
		return nil, fmt.Errorf("column %s is not materialized", colName)
	}

	arr, exists := ds.materializedColumns[colIdx]
	if !exists {
		return nil, fmt.Errorf("materialized array not found for column %s", colName)
	}

	return arr, nil
}

// GetAllMaterializedArrays returns all materialized Arrow arrays with their column names.
// This provides a complete view of all materialized computed columns.
func (ds *ExpressionDataSource) GetAllMaterializedArrays() map[string]arrow.Array {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make(map[string]arrow.Array)
	for colIdx, arr := range ds.materializedColumns {
		if colIdx < len(ds.columns) && ds.columns[colIdx].Materialized {
			result[ds.columns[colIdx].Name] = arr
		}
	}
	return result
}

// GetColumnAsArrow returns a specific column as an Arrow array.
// This method handles both original and computed columns.
func (ds *ExpressionDataSource) GetColumnAsArrow(colIdx int) (arrow.Field, arrow.Column, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if colIdx < 0 || colIdx >= len(ds.columns) {
		return arrow.Field{}, arrow.Column{}, ErrColumnNotFound(fmt.Sprintf("column %d", colIdx))
	}

	colDef := ds.columns[colIdx]
	colName := colDef.Name
	colType := colDef.Type

	// Create Arrow field
	arrowType := datatypeToArrow(colType)
	field := arrow.Field{Name: colName, Type: arrowType}

	// Get data as Arrow array
	arr, err := ds.getColumnAsArrowLocked(colName)
	if err != nil {
		return arrow.Field{}, arrow.Column{}, fmt.Errorf("failed to get column %s: %w", colName, err)
	}

	// Create Arrow column
	column := *arrow.NewColumn(field, arrow.NewChunked(arrowType, []arrow.Array{arr}))

	return field, column, nil
}
