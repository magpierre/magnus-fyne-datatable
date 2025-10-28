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

package main

import (
	"log"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"

	"github.com/magpierre/fyne-datatable/adapters/slice"
	"github.com/magpierre/fyne-datatable/datatable"
	dtwidget "github.com/magpierre/fyne-datatable/widget"
)

func main() {
	// Create Fyne application
	myApp := app.New()
	window := myApp.NewWindow("Magnus Fyne DataTable Test Application")
	window.Resize(fyne.Size{Width: 1000, Height: 700})

	// Create comprehensive sample data with different data types
	data := [][]any{
		// Employee data with various types
		{"Alice Johnson", 30, "Software Engineer", 75000.50, true, "2023-01-15", "Engineering"},
		{"Bob Smith", 25, "UI/UX Designer", 65000.00, true, "2023-03-20", "Design"},
		{"Charlie Brown", 35, "Product Manager", 85000.75, true, "2022-11-10", "Product"},
		{"Diana Prince", 28, "Data Scientist", 70000.25, true, "2023-02-28", "Data"},
		{"Eve Wilson", 32, "DevOps Engineer", 76000.00, false, "2023-05-12", "Engineering"},
		{"Frank Miller", 27, "Marketing Specialist", 66000.50, true, "2023-04-05", "Marketing"},
		{"Grace Lee", 31, "QA Engineer", 72000.00, true, "2023-01-30", "Engineering"},
		{"Henry Davis", 29, "Sales Representative", 74000.25, false, "2023-06-15", "Sales"},
		{"Iris Chen", 33, "Technical Writer", 68000.00, true, "2022-12-01", "Documentation"},
		{"Jack Thompson", 26, "Frontend Developer", 68000.75, true, "2023-07-20", "Engineering"},
		{"Karen White", 34, "Backend Developer", 78000.00, true, "2022-10-15", "Engineering"},
		{"Liam O'Connor", 24, "Junior Developer", 55000.00, true, "2023-08-01", "Engineering"},
		{"Maya Patel", 36, "Senior Engineer", 90000.00, true, "2022-05-10", "Engineering"},
		{"Noah Garcia", 30, "Mobile Developer", 73000.50, true, "2023-03-15", "Engineering"},
		{"Olivia Martinez", 27, "Content Creator", 62000.00, true, "2023-05-25", "Marketing"},
		{"Paul Anderson", 38, "System Administrator", 82000.00, true, "2022-08-20", "IT"},
		{"Quinn Taylor", 29, "Business Analyst", 71000.00, true, "2023-02-10", "Business"},
		{"Rachel Kim", 32, "UX Researcher", 69000.00, true, "2023-04-18", "Design"},
		{"Sam Wilson", 31, "Security Engineer", 85000.00, true, "2022-12-15", "Security"},
		{"Tina Rodriguez", 28, "Customer Success", 64000.00, true, "2023-06-30", "Customer Success"},
	}

	headers := []string{"Name", "Age", "Position", "Salary", "Active", "Start Date", "Department"}

	// Create data source
	source, err := slice.NewFromInterfaces(data, headers)
	if err != nil {
		log.Fatal("Failed to create data source:", err)
	}

	// Create table model
	model, err := datatable.NewTableModel(source)
	if err != nil {
		log.Fatal("Failed to create table model:", err)
	}

	// Create DataTable widget with comprehensive configuration
	config := dtwidget.DefaultConfig()
	config.ShowColumnSelector = true                 // Enable column selection accordion
	config.ShowFilterBar = true                      // Enable row filtering
	config.ShowStatusBar = true                      // Enable status bar
	config.ShowSettingsButton = true                 // Enable settings button
	config.AutoAdjustColumnWidths = true             // Auto-adjust columns to fit headers
	config.SelectionMode = dtwidget.SelectionModeRow // Enable row selection for copy functionality
	config.MinColumnWidth = 120                      // Set minimum column width

	table := dtwidget.NewDataTableWithConfig(model, config)

	// Set window reference for settings dialog and keyboard shortcuts
	table.SetWindow(window)

	// Add selection handler for logging
	table.OnCellSelected(func(row, col int) {
		if col == -1 {
			// Row selection mode
			rowData, err := model.VisibleRow(row)
			if err != nil {
				log.Printf("Row selection error: %v\n", err)
				return
			}
			log.Printf("Row %d selected: %v\n", row, rowData)
		} else {
			// Cell selection mode (shouldn't happen in row mode, but just in case)
			cell, err := model.VisibleCell(row, col)
			if err != nil {
				log.Printf("Cell selection error: %v\n", err)
				return
			}
			colName, _ := model.VisibleColumnName(col)
			log.Printf("Cell selected: [%d, %d] (%s) = %s\n", row, col, colName, cell.Formatted)
		}
	})

	// Create a simple info panel to show DataTable features
	infoText := widget.NewRichTextFromMarkdown(`
# Magnus Fyne DataTable Test Application

## Features Demonstrated:

### 🔍 **Filtering**
- Use the filter bar above the table to search data
- Try filtering by name: "Alice" or department: "Engineering"
- Supports complex expressions and multiple conditions

### 📊 **Sorting**
- Click column headers to sort data
- Cycles through: None → Ascending → Descending → None
- Sort indicators (↑ ↓) show current sort state

### 📋 **Column Management**
- Use the column selector accordion to show/hide columns
- Toggle columns on/off to customize your view
- Column order can be changed in settings

### 📱 **Selection & Copy**
- Select rows using checkboxes in the left column
- Use CMD+C (Mac) or Ctrl+C (Windows/Linux) to copy selected rows
- Copy includes headers and maintains tab-separated format

### ⚙️ **Settings**
- Click the settings button (⚙️) to access advanced options
- Configure display preferences and behavior

### 📈 **Status Information**
- Status bar shows current row count, sort state, and filter status
- Updates dynamically as you interact with the data

## Sample Data:
This test uses employee data with various data types:
- **Text**: Names, positions, departments
- **Numbers**: Ages, salaries
- **Booleans**: Active status
- **Dates**: Start dates
`)

	infoText.Wrapping = fyne.TextWrapWord

	// Create a scrollable info panel
	infoScroll := container.NewScroll(infoText)
	infoScroll.SetMinSize(fyne.NewSize(300, 200))

	// Create a split container with info panel and table
	split := container.NewHSplit(infoScroll, table)
	split.SetOffset(0.3) // 30% for info, 70% for table

	// Wrap the table with tooltip layer to enable tooltips
	// Note: We need to wrap the table before putting it in the split container
	wrappedTable := dtwidget.WrapWithTooltips(table, window.Canvas())

	// Recreate the split with the wrapped table
	split = container.NewHSplit(infoScroll, wrappedTable)
	split.SetOffset(0.3) // 30% for info, 70% for table

	window.SetContent(split)
	window.ShowAndRun()
}
