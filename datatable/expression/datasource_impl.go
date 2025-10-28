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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/magpierre/fyne-datatable/datatable"
)

// DataSource interface implementation

// RowCount implements datatable.DataSource.
func (ds *ExpressionDataSource) RowCount() int {
	return ds.source.RowCount()
}

// ColumnCount implements datatable.DataSource.
func (ds *ExpressionDataSource) ColumnCount() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return len(ds.columns)
}

// ColumnName implements datatable.DataSource.
func (ds *ExpressionDataSource) ColumnName(col int) (string, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if col < 0 || col >= len(ds.columns) {
		return "", datatable.ErrInvalidColumn
	}

	return ds.columns[col].Name, nil
}

// ColumnType implements datatable.DataSource.
func (ds *ExpressionDataSource) ColumnType(col int) (datatable.DataType, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if col < 0 || col >= len(ds.columns) {
		return datatable.TypeString, datatable.ErrInvalidColumn
	}

	return ds.columns[col].Type, nil
}

// IsComputedColumn returns true if the column at the given index is a computed column.
func (ds *ExpressionDataSource) IsComputedColumn(col int) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if col < 0 || col >= len(ds.columns) {
		return false
	}

	return ds.columns[col].IsComputed()
}

// Cell implements datatable.DataSource with lazy evaluation.
// On first access to a computed column, it triggers materialization.
func (ds *ExpressionDataSource) Cell(row, col int) (datatable.Value, error) {
	if row < 0 || row >= ds.source.RowCount() {
		return datatable.Value{}, datatable.ErrInvalidRow
	}

	ds.mu.RLock()
	if col < 0 || col >= len(ds.columns) {
		ds.mu.RUnlock()
		return datatable.Value{}, datatable.ErrInvalidColumn
	}

	colDef := ds.columns[col]
	ds.mu.RUnlock()

	// Case 1: Pass-through column (no expression)
	if colDef.IsPassThrough() {
		return ds.source.Cell(row, *colDef.SourceColumn)
	}

	// Case 2: Computed column
	// Check if materialized
	ds.mu.RLock()
	materialized := colDef.Materialized
	ds.mu.RUnlock()

	if !materialized {
		// Lazy evaluation: materialize on first access
		ds.mu.Lock()
		// Double-check after acquiring write lock
		if !ds.columns[col].Materialized {
			if err := ds.materializeColumnLocked(col); err != nil {
				ds.mu.Unlock()
				return datatable.Value{}, err
			}
		}
		ds.mu.Unlock()
	}

	// Get value from materialized column
	return ds.getMaterializedValue(col, row)
}

// Row implements datatable.DataSource.
func (ds *ExpressionDataSource) Row(row int) ([]datatable.Value, error) {
	if row < 0 || row >= ds.source.RowCount() {
		return nil, datatable.ErrInvalidRow
	}

	ds.mu.RLock()
	colCount := len(ds.columns)
	ds.mu.RUnlock()

	values := make([]datatable.Value, colCount)
	for col := 0; col < colCount; col++ {
		val, err := ds.Cell(row, col)
		if err != nil {
			return nil, err
		}
		values[col] = val
	}

	return values, nil
}

// Metadata implements datatable.DataSource.
func (ds *ExpressionDataSource) Metadata() datatable.Metadata {
	sourceMeta := ds.source.Metadata()

	// Add expression-specific metadata
	result := make(datatable.Metadata)
	for k, v := range sourceMeta {
		result[k] = v
	}

	ds.mu.RLock()
	defer ds.mu.RUnlock()

	// Add computed column information
	computedCols := []string{}
	for _, col := range ds.columns {
		if col.IsComputed() {
			computedCols = append(computedCols, col.Name)
		}
	}

	if len(computedCols) > 0 {
		result["computed_columns"] = computedCols
	}

	return result
}

// Helper methods

// getMaterializedValue extracts a value from a materialized Arrow array.
func (ds *ExpressionDataSource) getMaterializedValue(col, row int) (datatable.Value, error) {
	ds.mu.RLock()
	arr, exists := ds.materializedColumns[col]
	ds.mu.RUnlock()

	if !exists {
		return datatable.Value{}, fmt.Errorf("column %d not materialized", col)
	}

	if row < 0 || row >= arr.Len() {
		return datatable.Value{}, datatable.ErrInvalidRow
	}

	// Convert Arrow value to datatable.Value
	return arrowToValue(arr, row), nil
}

// arrowToValue converts an Arrow array value at the given index to a datatable.Value.
func arrowToValue(arr arrow.Array, row int) datatable.Value {
	if arr.IsNull(row) {
		// For null values, we need to determine if this represents an error
		// This is a heuristic: if the array type suggests it should have a value
		// but it's null, it might be an error
		dataType := determineDatatableType(arr.DataType())
		return datatable.NewNullValue(dataType)
	}

	// Determine the datatable type from Arrow type
	var dataType datatable.DataType
	switch arr.DataType().ID() {
	case arrow.STRING:
		dataType = datatable.TypeString
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		dataType = datatable.TypeInt
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		dataType = datatable.TypeFloat
	case arrow.BOOL:
		dataType = datatable.TypeBool
	case arrow.DATE32, arrow.DATE64:
		dataType = datatable.TypeDate
	case arrow.TIMESTAMP:
		dataType = datatable.TypeTimestamp
	default:
		dataType = datatable.TypeString // Default fallback
	}

	var rawValue any
	switch a := arr.(type) {
	case *array.Int8:
		rawValue = int64(a.Value(row))
	case *array.Int16:
		rawValue = int64(a.Value(row))
	case *array.Int32:
		rawValue = int64(a.Value(row))
	case *array.Int64:
		rawValue = a.Value(row)
	case *array.Uint8:
		rawValue = uint64(a.Value(row))
	case *array.Uint16:
		rawValue = uint64(a.Value(row))
	case *array.Uint32:
		rawValue = uint64(a.Value(row))
	case *array.Uint64:
		rawValue = a.Value(row)
	case *array.Float32:
		rawValue = float64(a.Value(row))
	case *array.Float64:
		rawValue = a.Value(row)
	case *array.String:
		strValue := a.Value(row)
		// Check if this is an error value
		if strings.HasPrefix(strValue, "Error: ") {
			errorMsg := strings.TrimPrefix(strValue, "Error: ")
			return datatable.NewErrorValue(errorMsg, dataType)
		}
		rawValue = strValue
	case *array.Boolean:
		rawValue = a.Value(row)
	case *array.Date64:
		dateValue := a.Value(row)
		// Check if this is an error marker (-1)
		if dateValue == arrow.Date64(-1) {
			return datatable.NewErrorValue("date parsing failed", dataType)
		}
		rawValue = int64(dateValue)
	case *array.Timestamp:
		timestampValue := a.Value(row)
		// Check if this is an error marker (-1)
		if timestampValue == arrow.Timestamp(-1) {
			return datatable.NewErrorValue("timestamp parsing failed", dataType)
		}
		rawValue = int64(timestampValue)
	default:
		rawValue = nil
	}

	// Create a proper datatable.Value with all fields set
	return datatable.NewValue(rawValue, dataType)
}

// determineDatatableType converts Arrow data type to datatable data type
func determineDatatableType(arrowType arrow.DataType) datatable.DataType {
	switch arrowType.ID() {
	case arrow.STRING:
		return datatable.TypeString
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return datatable.TypeInt
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return datatable.TypeFloat
	case arrow.BOOL:
		return datatable.TypeBool
	case arrow.DATE32, arrow.DATE64:
		return datatable.TypeDate
	case arrow.TIMESTAMP:
		return datatable.TypeTimestamp
	default:
		return datatable.TypeString // Default fallback
	}
}

// datatypeToArrow converts a datatable.DataType to an Arrow data type.
func datatypeToArrow(dt datatable.DataType) arrow.DataType {
	switch dt {
	case datatable.TypeInt:
		return arrow.PrimitiveTypes.Int64
	case datatable.TypeFloat:
		return arrow.PrimitiveTypes.Float64
	case datatable.TypeString:
		return arrow.BinaryTypes.String
	case datatable.TypeBool:
		return arrow.FixedWidthTypes.Boolean
	case datatable.TypeDate:
		return arrow.FixedWidthTypes.Date64
	case datatable.TypeTimestamp:
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		return arrow.PrimitiveTypes.Float64 // Default fallback
	}
}

// appendValueToBuilder appends a value to an Arrow builder based on type.
func appendValueToBuilder(builder array.Builder, value any, targetType arrow.DataType) error {
	switch targetType.ID() {
	case arrow.INT64:
		b := builder.(*array.Int64Builder)
		switch v := value.(type) {
		case int:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		case float64:
			b.Append(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}

	case arrow.FLOAT64:
		b := builder.(*array.Float64Builder)
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}

	case arrow.STRING:
		b := builder.(*array.StringBuilder)
		b.Append(fmt.Sprintf("%v", value))

	case arrow.BOOL:
		b := builder.(*array.BooleanBuilder)
		switch v := value.(type) {
		case bool:
			b.Append(v)
		default:
			return fmt.Errorf("cannot convert %T to bool", value)
		}

	default:
		return fmt.Errorf("unsupported Arrow type: %v", targetType)
	}

	return nil
}

// Release releases all materialized Arrow arrays.
// Should be called when the data source is no longer needed.
func (ds *ExpressionDataSource) Release() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for _, arr := range ds.materializedColumns {
		arr.Release()
	}
	ds.materializedColumns = make(map[int]arrow.Array)

	for i := range ds.columns {
		ds.columns[i].Materialized = false
	}
}
