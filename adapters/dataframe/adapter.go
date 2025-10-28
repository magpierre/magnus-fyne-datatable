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

// Package dataframe provides an adapter to use mp_dataframe.DataFrame
// as a datatable.DataSource for magnus-fyne-datatable widgets.
package dataframe

import (
	"fmt"
	"sync"

	"github.com/magpierre/fyne-datatable/datatable"
	mpdf "github.com/magpierre/mp_dataframe/dataframe"
)

// Adapter makes mp_dataframe.DataFrame implement datatable.DataSource.
// It provides a bridge between the powerful mp_dataframe data engine
// and the magnus-fyne-datatable UI widgets.
//
// The adapter is thread-safe for concurrent reads, matching the
// DataSource interface requirements.
//
// Example usage:
//
//	df, _ := dataframe.NewDataFrame(data)
//	adapter := dataframe.NewAdapter(df)
//	table := widget.NewDataTable(adapter)
type Adapter struct {
	df       *mpdf.DataFrame
	mu       sync.RWMutex
	metadata datatable.Metadata
}

// NewAdapter creates a new DataFrameAdapter wrapping the given DataFrame.
// The adapter maintains a reference to the DataFrame and provides
// read-only access through the DataSource interface.
func NewAdapter(df *mpdf.DataFrame) *Adapter {
	return &Adapter{
		df:       df,
		metadata: make(datatable.Metadata),
	}
}

// RowCount returns the total number of rows in the DataFrame.
func (a *Adapter) RowCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.df.RowCount()
}

// ColumnCount returns the total number of columns in the DataFrame.
func (a *Adapter) ColumnCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.df.ColumnCount()
}

// ColumnName returns the name of the column at the given index.
// Returns an error if the column index is out of range.
func (a *Adapter) ColumnName(col int) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	names := a.df.ColumnNames()
	if col < 0 || col >= len(names) {
		return "", fmt.Errorf("column index %d out of range [0, %d)", col, len(names))
	}
	return names[col], nil
}

// ColumnType returns the data type of the column at the given index.
// Maps mp_dataframe DataType to datatable DataType.
// Returns an error if the column index is out of range.
func (a *Adapter) ColumnType(col int) (datatable.DataType, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	names := a.df.ColumnNames()
	if col < 0 || col >= len(names) {
		return 0, fmt.Errorf("column index %d out of range [0, %d)", col, len(names))
	}

	series, err := a.df.Column(names[col])
	if err != nil {
		return 0, fmt.Errorf("failed to get column %s: %w", names[col], err)
	}

	return mapDataType(series.Type()), nil
}

// Cell returns the value at the specified row and column.
// Returns an error if the row or column index is out of range.
func (a *Adapter) Cell(row, col int) (datatable.Value, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Validate indices
	if row < 0 || row >= a.df.RowCount() {
		return datatable.Value{}, fmt.Errorf("row index %d out of range [0, %d)", row, a.df.RowCount())
	}

	names := a.df.ColumnNames()
	if col < 0 || col >= len(names) {
		return datatable.Value{}, fmt.Errorf("column index %d out of range [0, %d)", col, len(names))
	}

	// Get the column
	series, err := a.df.Column(names[col])
	if err != nil {
		return datatable.NewErrorValue(err.Error(), datatable.TypeString), nil
	}

	// Get the value
	val, err := series.Get(row)
	if err != nil {
		return datatable.NewErrorValue(err.Error(), datatable.TypeString), nil
	}

	// Map to datatable.Value
	return mapValue(val, series.Type()), nil
}

// Row returns all values for the specified row.
// Returns an error if the row index is out of range.
func (a *Adapter) Row(row int) ([]datatable.Value, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Validate row index
	if row < 0 || row >= a.df.RowCount() {
		return nil, fmt.Errorf("row index %d out of range [0, %d)", row, a.df.RowCount())
	}

	names := a.df.ColumnNames()
	values := make([]datatable.Value, len(names))

	for col, name := range names {
		series, err := a.df.Column(name)
		if err != nil {
			values[col] = datatable.NewErrorValue(err.Error(), datatable.TypeString)
			continue
		}

		val, err := series.Get(row)
		if err != nil {
			values[col] = datatable.NewErrorValue(err.Error(), datatable.TypeString)
			continue
		}

		values[col] = mapValue(val, series.Type())
	}

	return values, nil
}

// Metadata returns optional metadata about the DataFrame.
func (a *Adapter) Metadata() datatable.Metadata {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Add some useful metadata
	meta := make(datatable.Metadata)
	for k, v := range a.metadata {
		meta[k] = v
	}

	meta["rows"] = a.df.RowCount()
	meta["columns"] = a.df.ColumnCount()
	meta["source"] = "mp_dataframe"

	return meta
}

// SetMetadata allows setting custom metadata on the adapter.
func (a *Adapter) SetMetadata(key string, value any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.metadata[key] = value
}

// DataFrame returns the underlying mp_dataframe.DataFrame.
// This allows accessing the full power of mp_dataframe operations
// like GroupBy, Join, and Transactions.
func (a *Adapter) DataFrame() *mpdf.DataFrame {
	return a.df
}

// mapDataType converts mp_dataframe.DataType to datatable.DataType.
func mapDataType(mpType mpdf.DataType) datatable.DataType {
	switch mpType {
	case mpdf.TypeString, mpdf.TypeLargeString:
		return datatable.TypeString
	case mpdf.TypeInt, mpdf.TypeInt64:
		return datatable.TypeInt
	case mpdf.TypeFloat, mpdf.TypeFloat64:
		return datatable.TypeFloat
	case mpdf.TypeBool:
		return datatable.TypeBool
	case mpdf.TypeDate:
		return datatable.TypeDate
	case mpdf.TypeTimestamp:
		return datatable.TypeTimestamp
	case mpdf.TypeBinary, mpdf.TypeLargeBinary, mpdf.TypeFixedSizeBinary:
		return datatable.TypeBinary
	case mpdf.TypeDecimal, mpdf.TypeDecimal128, mpdf.TypeDecimal256:
		return datatable.TypeDecimal
	case mpdf.TypeStruct:
		return datatable.TypeStruct
	case mpdf.TypeList, mpdf.TypeLargeList, mpdf.TypeFixedSizeList:
		return datatable.TypeList
	case mpdf.TypeMap, mpdf.TypeUnion:
		return datatable.TypeStruct // Map union types to struct for display
	default:
		return datatable.TypeString // Fallback to string for unknown types
	}
}

// mapValue converts mp_dataframe.Value to datatable.Value.
func mapValue(mpVal mpdf.Value, mpType mpdf.DataType) datatable.Value {
	// Handle null values
	if mpVal.Raw == nil {
		return datatable.NewNullValue(mapDataType(mpType))
	}

	// Create datatable value with type mapping
	dtType := mapDataType(mpType)
	return datatable.NewValue(mpVal.Raw, dtType)
}
