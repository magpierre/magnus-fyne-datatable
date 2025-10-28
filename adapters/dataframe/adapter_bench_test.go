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

	mpdf "github.com/magpierre/mp_dataframe/dataframe"
)

// createLargeDataFrame creates a large DataFrame for benchmarking.
func createLargeDataFrame(rows int) (*mpdf.DataFrame, error) {
	names := make([]string, rows)
	ages := make([]int, rows)
	salaries := make([]float64, rows)
	active := make([]bool, rows)

	for i := 0; i < rows; i++ {
		names[i] = "Person" + string(rune(i))
		ages[i] = 20 + (i % 60)
		salaries[i] = 30000.0 + float64(i*1000)
		active[i] = i%2 == 0
	}

	data := map[string]any{
		"Name":   names,
		"Age":    ages,
		"Salary": salaries,
		"Active": active,
	}

	return mpdf.NewDataFrame(data)
}

// BenchmarkRowCount benchmarks the RowCount method.
func BenchmarkRowCount(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.RowCount()
	}
}

// BenchmarkColumnCount benchmarks the ColumnCount method.
func BenchmarkColumnCount(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.ColumnCount()
	}
}

// BenchmarkColumnName benchmarks the ColumnName method.
func BenchmarkColumnName(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = adapter.ColumnName(0)
	}
}

// BenchmarkColumnType benchmarks the ColumnType method.
func BenchmarkColumnType(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = adapter.ColumnType(0)
	}
}

// BenchmarkCell benchmarks the Cell method.
func BenchmarkCell(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = adapter.Cell(i%10000, 0)
	}
}

// BenchmarkRow benchmarks the Row method.
func BenchmarkRow(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = adapter.Row(i % 10000)
	}
}

// BenchmarkAdapterOverhead compares direct DataFrame access vs Adapter access.
func BenchmarkAdapterOverhead(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.Run("DirectAccess", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			col, _ := df.Column("Name")
			_, _ = col.Get(i % 10000)
		}
	})

	b.Run("AdapterAccess", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = adapter.Cell(i%10000, 0)
		}
	})
}

// BenchmarkMetadata benchmarks the Metadata method.
func BenchmarkMetadata(b *testing.B) {
	df, err := createLargeDataFrame(10000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = adapter.Metadata()
	}
}

// BenchmarkCellSequential benchmarks sequential cell access (realistic UI scenario).
func BenchmarkCellSequential(b *testing.B) {
	df, err := createLargeDataFrame(1000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	rows := adapter.RowCount()
	cols := adapter.ColumnCount()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < rows; row++ {
			for col := 0; col < cols; col++ {
				_, _ = adapter.Cell(row, col)
			}
		}
	}
}

// BenchmarkRowSequential benchmarks sequential row access (realistic UI scenario).
func BenchmarkRowSequential(b *testing.B) {
	df, err := createLargeDataFrame(1000)
	if err != nil {
		b.Fatalf("Failed to create DataFrame: %v", err)
	}

	adapter := NewAdapter(df)
	rows := adapter.RowCount()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row := 0; row < rows; row++ {
			_, _ = adapter.Row(row)
		}
	}
}
