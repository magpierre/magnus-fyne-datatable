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

package dataframe

import (
	"testing"

	"github.com/magpierre/fyne-datatable/datatable"
	mpdf "github.com/magpierre/mp_dataframe/dataframe"
)

// createTestDataFrame creates a DataFrame for testing.
func createTestDataFrame() (*mpdf.DataFrame, error) {
	data := map[string]any{
		"Name":   []string{"Alice", "Bob", "Charlie"},
		"Age":    []int{25, 30, 35},
		"Salary": []float64{50000.0, 60000.0, 70000.0},
		"Active": []bool{true, false, true},
	}
	return mpdf.NewDataFrame(data)
}

// TestNewAdapter tests the creation of a new adapter.
func TestNewAdapter(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	if adapter == nil {
		t.Fatal("NewAdapter returned nil")
	}

	if adapter.df != df {
		t.Error("Adapter does not reference the correct DataFrame")
	}
}

// TestRowCount tests the RowCount method.
func TestRowCount(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	rowCount := adapter.RowCount()

	expectedRowCount := 3
	if rowCount != expectedRowCount {
		t.Errorf("Expected row count %d, got %d", expectedRowCount, rowCount)
	}
}

// TestColumnCount tests the ColumnCount method.
func TestColumnCount(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	columnCount := adapter.ColumnCount()

	expectedColumnCount := 4
	if columnCount != expectedColumnCount {
		t.Errorf("Expected column count %d, got %d", expectedColumnCount, columnCount)
	}
}

// TestColumnName tests the ColumnName method.
func TestColumnName(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	// Get all column names to verify they exist
	expectedNames := map[string]bool{
		"Name":   true,
		"Age":    true,
		"Salary": true,
		"Active": true,
	}

	// Test valid columns
	for col := 0; col < 4; col++ {
		name, err := adapter.ColumnName(col)
		if err != nil {
			t.Errorf("Unexpected error for column %d: %v", col, err)
		}
		if !expectedNames[name] {
			t.Errorf("Unexpected column name %s at index %d", name, col)
		}
	}

	// Test out of range
	_, err = adapter.ColumnName(4)
	if err == nil {
		t.Error("Expected error for column 4, got nil")
	}

	// Test negative index
	_, err = adapter.ColumnName(-1)
	if err == nil {
		t.Error("Expected error for column -1, got nil")
	}
}

// TestColumnType tests the ColumnType method.
func TestColumnType(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	// Map column name to expected type
	expectedTypes := map[string]datatable.DataType{
		"Name":   datatable.TypeString,
		"Age":    datatable.TypeInt,
		"Salary": datatable.TypeFloat,
		"Active": datatable.TypeBool,
	}

	// Test valid columns
	for col := 0; col < 4; col++ {
		colName, err := adapter.ColumnName(col)
		if err != nil {
			t.Fatalf("Failed to get column name for %d: %v", col, err)
		}

		colType, err := adapter.ColumnType(col)
		if err != nil {
			t.Errorf("Unexpected error for column %d: %v", col, err)
		}

		expectedType := expectedTypes[colName]
		if colType != expectedType {
			t.Errorf("Column %s: expected type %v, got %v", colName, expectedType, colType)
		}
	}

	// Test out of range
	_, err = adapter.ColumnType(4)
	if err == nil {
		t.Error("Expected error for column 4, got nil")
	}

	// Test negative index
	_, err = adapter.ColumnType(-1)
	if err == nil {
		t.Error("Expected error for column -1, got nil")
	}
}

// TestCell tests the Cell method.
func TestCell(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	// Build column name to index map
	colIndex := make(map[string]int)
	for i := 0; i < adapter.ColumnCount(); i++ {
		name, _ := adapter.ColumnName(i)
		colIndex[name] = i
	}

	// Test specific cells based on column name
	nameCol := colIndex["Name"]
	ageCol := colIndex["Age"]
	salaryCol := colIndex["Salary"]
	activeCol := colIndex["Active"]

	tests := []struct {
		row          int
		col          int
		expectedRaw  any
		expectedType datatable.DataType
		expectError  bool
	}{
		{0, nameCol, "Alice", datatable.TypeString, false},
		{1, nameCol, "Bob", datatable.TypeString, false},
		{0, ageCol, 25, datatable.TypeInt, false},
		{1, salaryCol, 60000.0, datatable.TypeFloat, false},
		{2, activeCol, true, datatable.TypeBool, false},
		{3, 0, nil, datatable.TypeString, true},  // Row out of range
		{0, 5, nil, datatable.TypeString, true},  // Column out of range
		{-1, 0, nil, datatable.TypeString, true}, // Negative row
		{0, -1, nil, datatable.TypeString, true}, // Negative column
	}

	for _, test := range tests {
		value, err := adapter.Cell(test.row, test.col)
		if test.expectError {
			if err == nil {
				t.Errorf("Expected error for cell (%d, %d), got nil", test.row, test.col)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for cell (%d, %d): %v", test.row, test.col, err)
			}
			if value.IsError() {
				t.Errorf("Cell (%d, %d) has error: %s", test.row, test.col, value.Error)
			}
			if value.Type != test.expectedType {
				t.Errorf("Expected type %v, got %v", test.expectedType, value.Type)
			}
			if value.Raw != test.expectedRaw {
				t.Errorf("Expected value %v, got %v", test.expectedRaw, value.Raw)
			}
		}
	}
}

// TestRow tests the Row method.
func TestRow(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	// Build column name to index map
	colIndex := make(map[string]int)
	for i := 0; i < adapter.ColumnCount(); i++ {
		name, _ := adapter.ColumnName(i)
		colIndex[name] = i
	}

	// Test valid row
	row0, err := adapter.Row(0)
	if err != nil {
		t.Fatalf("Unexpected error getting row 0: %v", err)
	}
	if len(row0) != 4 {
		t.Errorf("Expected 4 values in row, got %d", len(row0))
	}

	// Verify specific values by column name
	nameIdx := colIndex["Name"]
	ageIdx := colIndex["Age"]
	if row0[nameIdx].Raw != "Alice" {
		t.Errorf("Expected Name='Alice', got %v", row0[nameIdx].Raw)
	}
	if row0[ageIdx].Raw != 25 {
		t.Errorf("Expected Age=25, got %v", row0[ageIdx].Raw)
	}

	// Test row out of range
	_, err = adapter.Row(10)
	if err == nil {
		t.Error("Expected error for row out of range, got nil")
	}

	// Test negative row
	_, err = adapter.Row(-1)
	if err == nil {
		t.Error("Expected error for negative row, got nil")
	}
}

// TestMetadata tests the Metadata method.
func TestMetadata(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	metadata := adapter.Metadata()

	if metadata == nil {
		t.Fatal("Metadata returned nil")
	}

	// Check default metadata
	if rows, ok := metadata["rows"]; !ok || rows != 3 {
		t.Errorf("Expected rows metadata to be 3, got %v", rows)
	}
	if cols, ok := metadata["columns"]; !ok || cols != 4 {
		t.Errorf("Expected columns metadata to be 4, got %v", cols)
	}
	if source, ok := metadata["source"]; !ok || source != "mp_dataframe" {
		t.Errorf("Expected source metadata to be 'mp_dataframe', got %v", source)
	}
}

// TestSetMetadata tests the SetMetadata method.
func TestSetMetadata(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	adapter.SetMetadata("custom_key", "custom_value")

	metadata := adapter.Metadata()
	if val, ok := metadata["custom_key"]; !ok || val != "custom_value" {
		t.Errorf("Expected custom_key to be 'custom_value', got %v", val)
	}
}

// TestDataFrame tests the DataFrame method.
func TestDataFrame(t *testing.T) {
	df, err := createTestDataFrame()
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	retrievedDF := adapter.DataFrame()

	if retrievedDF != df {
		t.Error("DataFrame method did not return the original DataFrame")
	}
}

// TestMapDataType tests the mapDataType function.
func TestMapDataType(t *testing.T) {
	tests := []struct {
		mpType       mpdf.DataType
		expectedType datatable.DataType
	}{
		{mpdf.TypeString, datatable.TypeString},
		{mpdf.TypeLargeString, datatable.TypeString},
		{mpdf.TypeInt, datatable.TypeInt},
		{mpdf.TypeInt64, datatable.TypeInt},
		{mpdf.TypeFloat, datatable.TypeFloat},
		{mpdf.TypeFloat64, datatable.TypeFloat},
		{mpdf.TypeBool, datatable.TypeBool},
		{mpdf.TypeDate, datatable.TypeDate},
		{mpdf.TypeTimestamp, datatable.TypeTimestamp},
		{mpdf.TypeBinary, datatable.TypeBinary},
		{mpdf.TypeLargeBinary, datatable.TypeBinary},
		{mpdf.TypeFixedSizeBinary, datatable.TypeBinary},
		{mpdf.TypeDecimal, datatable.TypeDecimal},
		{mpdf.TypeDecimal128, datatable.TypeDecimal},
		{mpdf.TypeDecimal256, datatable.TypeDecimal},
		{mpdf.TypeStruct, datatable.TypeStruct},
		{mpdf.TypeList, datatable.TypeList},
		{mpdf.TypeLargeList, datatable.TypeList},
		{mpdf.TypeFixedSizeList, datatable.TypeList},
		{mpdf.TypeMap, datatable.TypeStruct},
		{mpdf.TypeUnion, datatable.TypeStruct},
	}

	for _, test := range tests {
		result := mapDataType(test.mpType)
		if result != test.expectedType {
			t.Errorf("mapDataType(%v) = %v, expected %v", test.mpType, result, test.expectedType)
		}
	}
}

// TestMapValue tests the mapValue function.
func TestMapValue(t *testing.T) {
	tests := []struct {
		mpVal        mpdf.Value
		mpType       mpdf.DataType
		expectedNull bool
		expectedType datatable.DataType
	}{
		{
			mpVal:        mpdf.Value{Raw: "test"},
			mpType:       mpdf.TypeString,
			expectedNull: false,
			expectedType: datatable.TypeString,
		},
		{
			mpVal:        mpdf.Value{Raw: 42},
			mpType:       mpdf.TypeInt,
			expectedNull: false,
			expectedType: datatable.TypeInt,
		},
		{
			mpVal:        mpdf.Value{Raw: nil},
			mpType:       mpdf.TypeString,
			expectedNull: true,
			expectedType: datatable.TypeString,
		},
	}

	for _, test := range tests {
		result := mapValue(test.mpVal, test.mpType)
		if result.IsNull != test.expectedNull {
			t.Errorf("mapValue IsNull = %v, expected %v", result.IsNull, test.expectedNull)
		}
		if result.Type != test.expectedType {
			t.Errorf("mapValue Type = %v, expected %v", result.Type, test.expectedType)
		}
		if !test.expectedNull && result.Raw != test.mpVal.Raw {
			t.Errorf("mapValue Raw = %v, expected %v", result.Raw, test.mpVal.Raw)
		}
	}
}

// TestAdapterWithNullValues tests adapter behavior with null values.
func TestAdapterWithNullValues(t *testing.T) {
	// Create a DataFrame and test that adapter can handle cells correctly
	data := map[string]any{
		"Name":   []string{"Alice", "Bob"},
		"Age":    []int{25, 30},
		"Salary": []float64{50000.0, 60000.0},
	}

	df, err := mpdf.NewDataFrame(data)
	if err != nil {
		t.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	// Test that all values can be retrieved
	for row := 0; row < adapter.RowCount(); row++ {
		for col := 0; col < adapter.ColumnCount(); col++ {
			val, err := adapter.Cell(row, col)
			if err != nil {
				t.Errorf("Error getting cell (%d, %d): %v", row, col, err)
			}
			if val.IsError() {
				t.Errorf("Cell (%d, %d) has error: %s", row, col, val.Error)
			}
		}
	}
}

// TestAdapterWithEmptyDataFrame tests adapter with empty DataFrame.
func TestAdapterWithEmptyDataFrame(t *testing.T) {
	data := map[string]any{
		"Name": []string{},
		"Age":  []int{},
	}

	df, err := mpdf.NewDataFrame(data)
	if err != nil {
		t.Fatalf("Failed to create empty DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	if adapter.RowCount() != 0 {
		t.Errorf("Expected 0 rows, got %d", adapter.RowCount())
	}

	if adapter.ColumnCount() != 2 {
		t.Errorf("Expected 2 columns, got %d", adapter.ColumnCount())
	}

	// Try to access a cell in empty DataFrame
	_, err = adapter.Cell(0, 0)
	if err == nil {
		t.Error("Expected error accessing cell in empty DataFrame")
	}
}
