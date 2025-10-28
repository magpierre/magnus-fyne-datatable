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

package dataframe_test

import (
	"fmt"

	dfadapter "github.com/magpierre/fyne-datatable/adapters/dataframe"
	mpdf "github.com/magpierre/mp_dataframe/dataframe"
)

// ExampleAdapter demonstrates basic usage of the DataFrameAdapter.
func ExampleAdapter() {
	// Create a DataFrame with employee data
	data := map[string]any{
		"Name":       []string{"Alice", "Bob", "Charlie", "David"},
		"Department": []string{"Sales", "IT", "Sales", "IT"},
		"Salary":     []float64{50000, 60000, 55000, 65000},
		"Active":     []bool{true, true, false, true},
	}

	df, _ := mpdf.NewDataFrame(data)

	// Wrap in adapter for UI integration
	adapter := dfadapter.NewAdapter(df)

	// Display basic information
	fmt.Printf("Rows: %d\n", adapter.RowCount())
	fmt.Printf("Columns: %d\n", adapter.ColumnCount())

	// Find Name column and display its type
	for i := 0; i < adapter.ColumnCount(); i++ {
		name, _ := adapter.ColumnName(i)
		if name == "Name" {
			colType, _ := adapter.ColumnType(i)
			fmt.Printf("Name column type: %v\n", colType)
			break
		}
	}

	// Output:
	// Rows: 4
	// Columns: 4
	// Name column type: String
}

// ExampleAdapter_groupBy demonstrates using GroupBy through the adapter.
func ExampleAdapter_groupBy() {
	// Create DataFrame
	data := map[string]any{
		"Department": []string{"Sales", "IT", "Sales", "IT"},
		"Salary":     []float64{50000, 60000, 55000, 65000},
	}

	df, _ := mpdf.NewDataFrame(data)

	// Perform GroupBy operation
	grouped, _ := df.GroupBy("Department")
	result, _ := grouped.Sum("Salary")

	// Wrap result in adapter for display
	adapter := dfadapter.NewAdapter(result)

	fmt.Printf("Grouped data has %d rows\n", adapter.RowCount())

	// Output:
	// Grouped data has 2 rows
}

// ExampleAdapter_metadata demonstrates metadata usage.
func ExampleAdapter_metadata() {
	data := map[string]any{
		"Name": []string{"Alice", "Bob"},
		"Age":  []int{25, 30},
	}

	df, _ := mpdf.NewDataFrame(data)
	adapter := dfadapter.NewAdapter(df)

	// Set custom metadata
	adapter.SetMetadata("source", "employee_database")
	adapter.SetMetadata("version", "1.0")

	// Retrieve metadata
	meta := adapter.Metadata()
	fmt.Printf("Rows: %v\n", meta["rows"])
	fmt.Printf("Data source type: %v\n", meta["source"])

	// Output:
	// Rows: 2
	// Data source type: mp_dataframe
}

// ExampleAdapter_cellAccess demonstrates accessing individual cells.
func ExampleAdapter_cellAccess() {
	data := map[string]any{
		"Name":   []string{"Alice", "Bob"},
		"Age":    []int{25, 30},
		"Salary": []float64{50000, 60000},
	}

	df, _ := mpdf.NewDataFrame(data)
	adapter := dfadapter.NewAdapter(df)

	// Find the Age column
	var ageCol int
	for i := 0; i < adapter.ColumnCount(); i++ {
		name, _ := adapter.ColumnName(i)
		if name == "Age" {
			ageCol = i
			break
		}
	}

	// Access a specific cell
	cell, _ := adapter.Cell(0, ageCol)
	fmt.Printf("Alice's age: %v\n", cell.Raw)

	// Output:
	// Alice's age: 25
}

// ExampleAdapter_rowAccess demonstrates accessing entire rows.
func ExampleAdapter_rowAccess() {
	data := map[string]any{
		"Name":   []string{"Alice", "Bob"},
		"Age":    []int{25, 30},
		"Active": []bool{true, false},
	}

	df, _ := mpdf.NewDataFrame(data)
	adapter := dfadapter.NewAdapter(df)

	// Get first row
	row, _ := adapter.Row(0)
	fmt.Printf("First row has %d values\n", len(row))
	fmt.Printf("All values are non-null: %v\n", !row[0].IsNull && !row[1].IsNull && !row[2].IsNull)

	// Output:
	// First row has 3 values
	// All values are non-null: true
}
