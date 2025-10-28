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

	"github.com/magpierre/fyne-datatable/datatable"
)

// Benchmark simple numeric filter
func BenchmarkFilter_SimpleNumeric(b *testing.B) {
	filter, _ := NewExpressionFilter("age > 18")

	row := []datatable.Value{
		{Raw: int64(25), IsNull: false},
	}
	columnNames := []string{"age"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark string equality filter
func BenchmarkFilter_StringEquality(b *testing.B) {
	filter, _ := NewExpressionFilter("status == 'active'")

	row := []datatable.Value{
		{Raw: "active", IsNull: false},
	}
	columnNames := []string{"status"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark complex AND filter
func BenchmarkFilter_ComplexAND(b *testing.B) {
	filter, _ := NewExpressionFilter("age >= 18 && status == 'active' && balance > 1000")

	row := []datatable.Value{
		{Raw: int64(25), IsNull: false},
		{Raw: "active", IsNull: false},
		{Raw: 1500.0, IsNull: false},
	}
	columnNames := []string{"age", "status", "balance"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark complex OR filter
func BenchmarkFilter_ComplexOR(b *testing.B) {
	filter, _ := NewExpressionFilter("balance > 5000 || vip == true || (age >= 65 && retired)")

	row := []datatable.Value{
		{Raw: 3000.0, IsNull: false},
		{Raw: false, IsNull: false},
		{Raw: int64(70), IsNull: false},
		{Raw: true, IsNull: false},
	}
	columnNames := []string{"balance", "vip", "age", "retired"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark filter with function call
func BenchmarkFilter_WithFunction(b *testing.B) {
	filter, _ := NewExpressionFilter("upper(status) == 'ACTIVE'")

	row := []datatable.Value{
		{Raw: "active", IsNull: false},
	}
	columnNames := []string{"status"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark null check filter
func BenchmarkFilter_NullCheck(b *testing.B) {
	filter, _ := NewExpressionFilter("!isNull(email)")

	row := []datatable.Value{
		{Raw: "test@example.com", IsNull: false},
	}
	columnNames := []string{"email"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Evaluate(row, columnNames)
	}
}

// Benchmark filtering many rows
func BenchmarkFilter_LargeDataset(b *testing.B) {
	filter, _ := NewExpressionFilter("age >= 18 && status == 'active'")

	// Create 1000 test rows
	rows := make([][]datatable.Value, 1000)
	for i := 0; i < 1000; i++ {
		status := "active"
		if i%3 == 0 {
			status = "inactive"
		}

		rows[i] = []datatable.Value{
			{Raw: int64(20 + (i % 50)), IsNull: false},
			{Raw: status, IsNull: false},
		}
	}

	columnNames := []string{"age", "status"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, row := range rows {
			filter.Evaluate(row, columnNames)
		}
	}
}
