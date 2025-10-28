# Magnus Fyne DataTable Test Application

This is a comprehensive test application for the `magnus-fyne-datatable` library, demonstrating all the key features and capabilities of the DataTable widget.

## Features Demonstrated

### 🔍 **Data Filtering**
- Interactive filter bar for searching and filtering data
- Support for complex expressions and multiple conditions
- Real-time filtering with immediate results

### 📊 **Column Sorting**
- Click column headers to sort data
- Three-state sorting: None → Ascending → Descending → None
- Visual sort indicators (↑ ↓) in column headers
- Maintains sort state across filter operations

### 📋 **Column Management**
- Column selector accordion to show/hide columns
- Dynamic column visibility control
- Customizable column order through settings

### 📱 **Row Selection & Copy**
- Row selection using checkboxes
- Multi-row selection support
- Keyboard shortcuts: CMD+C (Mac) / Ctrl+C (Windows/Linux)
- Copy includes headers and maintains tab-separated format

### ⚙️ **Settings & Configuration**
- Settings dialog for advanced configuration
- Customizable display preferences
- Behavior configuration options

### 📈 **Status Information**
- Real-time status bar showing:
  - Current row count
  - Active sort state
  - Filter status
  - Selection information

## Sample Data

The test application includes comprehensive sample data with various data types:

- **Text**: Employee names, positions, departments
- **Numbers**: Ages, salaries (with decimal precision)
- **Booleans**: Active employment status
- **Dates**: Employee start dates
- **Mixed Types**: Demonstrates proper type handling and formatting

## Running the Application

### Prerequisites
- Go 1.25.3 or later
- Fyne v2.7.0 or later
- Access to the `magnus-fyne-datatable` library

### Build and Run
```bash
cd tests
go mod tidy
go run main.go
```

### Build Executable
```bash
cd tests
go build -o test-app main.go
./test-app
```

## Usage Instructions

1. **Filtering**: Use the filter bar above the table to search for specific data
   - Try: `name = "Alice"` or `department = "Engineering"`
   - Supports complex expressions and multiple conditions

2. **Sorting**: Click any column header to sort the data
   - First click: Ascending sort
   - Second click: Descending sort  
   - Third click: Clear sort

3. **Column Selection**: Use the column selector accordion to show/hide columns
   - Toggle columns on/off to customize your view
   - Column order can be changed in settings

4. **Row Selection**: Select rows using the checkboxes in the left column
   - Select multiple rows for bulk operations
   - Use CMD+C or Ctrl+C to copy selected data

5. **Settings**: Click the settings button (⚙️) to access advanced options
   - Configure display preferences
   - Modify behavior settings

## Technical Details

- **Data Source**: Uses the `slice` adapter for in-memory data
- **Widget Configuration**: Demonstrates all available configuration options
- **Tooltips**: Enabled for full cell content display on hover
- **Keyboard Shortcuts**: Integrated copy functionality
- **Responsive Layout**: Split view with information panel and data table

This test application serves as both a demonstration and a comprehensive test suite for the DataTable widget functionality.
