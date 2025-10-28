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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestIntegration_MathFunctions tests all math functions from Phase 2.
func TestIntegration_MathFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		input      []float64
		expected   []float64
	}{
		{
			name:       "abs",
			expression: "abs(x)",
			input:      []float64{-5, -2.5, 0, 2.5, 5},
			expected:   []float64{5, 2.5, 0, 2.5, 5},
		},
		{
			name:       "ceil",
			expression: "ceil(x)",
			input:      []float64{1.1, 2.5, 3.9},
			expected:   []float64{2, 3, 4},
		},
		{
			name:       "floor",
			expression: "floor(x)",
			input:      []float64{1.1, 2.5, 3.9},
			expected:   []float64{1, 2, 3},
		},
		{
			name:       "round",
			expression: "round(x)",
			input:      []float64{1.4, 2.5, 3.6},
			expected:   []float64{1, 3, 4}, // Go's round uses "round half away from zero"
		},
		{
			name:       "sqrt",
			expression: "sqrt(x)",
			input:      []float64{0, 1, 4, 9, 16},
			expected:   []float64{0, 1, 2, 3, 4},
		},
		{
			name:       "pow",
			expression: "pow(x, 2)",
			input:      []float64{0, 1, 2, 3, 4},
			expected:   []float64{0, 1, 4, 9, 16},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expression, []string{"x"}, arrow.PrimitiveTypes.Float64)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input array
			builder := array.NewFloat64Builder(mem)
			defer builder.Release()
			for _, val := range tt.input {
				builder.Append(val)
			}
			inputArray := builder.NewArray()
			defer inputArray.Release()

			// Evaluate
			result, err := expr.Evaluate([]arrow.Array{inputArray}, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify
			resultArray := result.(*array.Float64)
			for i := 0; i < resultArray.Len(); i++ {
				got := resultArray.Value(i)
				want := tt.expected[i]
				if math.Abs(got-want) > 1e-9 {
					t.Errorf("result[%d] = %v, want %v", i, got, want)
				}
			}
		})
	}
}

// TestIntegration_StringFunctions tests all string functions from Phase 2.
func TestIntegration_StringFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		input      []string
		expected   []string
	}{
		{
			name:       "upper",
			expression: "upper(s)",
			input:      []string{"hello", "world", "test"},
			expected:   []string{"HELLO", "WORLD", "TEST"},
		},
		{
			name:       "lower",
			expression: "lower(s)",
			input:      []string{"HELLO", "WORLD", "TEST"},
			expected:   []string{"hello", "world", "test"},
		},
		{
			name:       "trim",
			expression: "trim(s)",
			input:      []string{"  hello  ", "  world  ", "  test  "},
			expected:   []string{"hello", "world", "test"},
		},
		{
			name:       "len",
			expression: "string(len(s))",
			input:      []string{"a", "ab", "abc"},
			expected:   []string{"1", "2", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expression, []string{"s"}, arrow.BinaryTypes.String)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input array
			builder := array.NewStringBuilder(mem)
			defer builder.Release()
			for _, val := range tt.input {
				builder.Append(val)
			}
			inputArray := builder.NewArray()
			defer inputArray.Release()

			// Evaluate
			result, err := expr.Evaluate([]arrow.Array{inputArray}, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify
			resultArray := result.(*array.String)
			for i := 0; i < resultArray.Len(); i++ {
				got := resultArray.Value(i)
				want := tt.expected[i]
				if got != want {
					t.Errorf("result[%d] = %v, want %v", i, got, want)
				}
			}
		})
	}
}

// TestIntegration_NestedFunctions tests nested function calls.
func TestIntegration_NestedFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		input      []string
		expected   []string
	}{
		{
			name:       "nested upper(lower())",
			expression: "upper(lower(s))",
			input:      []string{"HeLLo", "WoRLd"},
			expected:   []string{"HELLO", "WORLD"},
		},
		{
			name:       "nested lower(upper())",
			expression: "lower(upper(s))",
			input:      []string{"HeLLo", "WoRLd"},
			expected:   []string{"hello", "world"},
		},
		{
			name:       "nested trim(upper())",
			expression: "trim(upper(s))",
			input:      []string{"  hello  ", "  world  "},
			expected:   []string{"HELLO", "WORLD"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expression, []string{"s"}, arrow.BinaryTypes.String)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input array
			builder := array.NewStringBuilder(mem)
			defer builder.Release()
			for _, val := range tt.input {
				builder.Append(val)
			}
			inputArray := builder.NewArray()
			defer inputArray.Release()

			// Evaluate
			result, err := expr.Evaluate([]arrow.Array{inputArray}, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify
			resultArray := result.(*array.String)
			for i := 0; i < resultArray.Len(); i++ {
				got := resultArray.Value(i)
				want := tt.expected[i]
				if got != want {
					t.Errorf("result[%d] = %v, want %v", i, got, want)
				}
			}
		})
	}
}

// TestIntegration_ComplexExpressions tests complex multi-column expressions.
func TestIntegration_ComplexExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("price * quantity * (1 + tax)", func(t *testing.T) {
		expr, err := NewExpression(
			"price * quantity * (1 + tax)",
			[]string{"price", "quantity", "tax"},
			arrow.PrimitiveTypes.Float64,
		)
		if err != nil {
			t.Fatalf("NewExpression() error = %v", err)
		}

		// Build input arrays
		priceBuilder := array.NewFloat64Builder(mem)
		defer priceBuilder.Release()
		priceBuilder.AppendValues([]float64{10, 20, 30}, nil)
		priceArray := priceBuilder.NewArray()
		defer priceArray.Release()

		quantityBuilder := array.NewFloat64Builder(mem)
		defer quantityBuilder.Release()
		quantityBuilder.AppendValues([]float64{2, 3, 4}, nil)
		quantityArray := quantityBuilder.NewArray()
		defer quantityArray.Release()

		taxBuilder := array.NewFloat64Builder(mem)
		defer taxBuilder.Release()
		taxBuilder.AppendValues([]float64{0.1, 0.15, 0.2}, nil)
		taxArray := taxBuilder.NewArray()
		defer taxArray.Release()

		// Evaluate
		result, err := expr.Evaluate(
			[]arrow.Array{priceArray, quantityArray, taxArray},
			mem,
		)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		defer result.Release()

		// Verify
		expected := []float64{22, 69, 144} // 10*2*1.1, 20*3*1.15, 30*4*1.2
		resultArray := result.(*array.Float64)
		for i := 0; i < resultArray.Len(); i++ {
			got := resultArray.Value(i)
			want := expected[i]
			if math.Abs(got-want) > 1e-9 {
				t.Errorf("result[%d] = %v, want %v", i, got, want)
			}
		}
	})
}

// TestIntegration_ConditionalExpressions tests ternary operator.
func TestIntegration_ConditionalExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("conditional status", func(t *testing.T) {
		expr, err := NewExpression(
			`balance > 0 ? "Active" : "Inactive"`,
			[]string{"balance"},
			arrow.BinaryTypes.String,
		)
		if err != nil {
			t.Fatalf("NewExpression() error = %v", err)
		}

		// Build input array
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.AppendValues([]float64{100, -50, 0, 25}, nil)
		inputArray := builder.NewArray()
		defer inputArray.Release()

		// Evaluate
		result, err := expr.Evaluate([]arrow.Array{inputArray}, mem)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		defer result.Release()

		// Verify
		expected := []string{"Active", "Inactive", "Inactive", "Active"}
		resultArray := result.(*array.String)
		for i := 0; i < resultArray.Len(); i++ {
			got := resultArray.Value(i)
			want := expected[i]
			if got != want {
				t.Errorf("result[%d] = %v, want %v", i, got, want)
			}
		}
	})
}

// TestIntegration_NullHandling tests null value handling.
func TestIntegration_NullHandling(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("coalesce function", func(t *testing.T) {
		expr, err := NewExpression(
			"coalesce(a, b, 0)",
			[]string{"a", "b"},
			arrow.PrimitiveTypes.Float64,
		)
		if err != nil {
			t.Fatalf("NewExpression() error = %v", err)
		}

		// Build input arrays with nulls
		aBuilder := array.NewFloat64Builder(mem)
		defer aBuilder.Release()
		aBuilder.AppendNull()
		aBuilder.Append(10)
		aBuilder.AppendNull()
		aArray := aBuilder.NewArray()
		defer aArray.Release()

		bBuilder := array.NewFloat64Builder(mem)
		defer bBuilder.Release()
		bBuilder.Append(5)
		bBuilder.Append(20)
		bBuilder.AppendNull()
		bArray := bBuilder.NewArray()
		defer bArray.Release()

		// Evaluate
		result, err := expr.Evaluate([]arrow.Array{aArray, bArray}, mem)
		if err != nil {
			t.Fatalf("Evaluate() error = %v", err)
		}
		defer result.Release()

		// Verify: coalesce should pick first non-null value
		expected := []float64{5, 10, 0}
		resultArray := result.(*array.Float64)
		for i := 0; i < resultArray.Len(); i++ {
			got := resultArray.Value(i)
			want := expected[i]
			if math.Abs(got-want) > 1e-9 {
				t.Errorf("result[%d] = %v, want %v", i, got, want)
			}
		}
	})
}

// TestIntegration_Performance benchmarks expression evaluation performance.
func TestIntegration_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	mem := memory.NewGoAllocator()

	// Test with 10k rows
	rowCount := 10000

	expr, err := NewExpression(
		"sqrt(x * x + y * y)",
		[]string{"x", "y"},
		arrow.PrimitiveTypes.Float64,
	)
	if err != nil {
		t.Fatalf("NewExpression() error = %v", err)
	}

	// Build large input arrays
	xBuilder := array.NewFloat64Builder(mem)
	defer xBuilder.Release()
	yBuilder := array.NewFloat64Builder(mem)
	defer yBuilder.Release()

	for i := 0; i < rowCount; i++ {
		xBuilder.Append(float64(i))
		yBuilder.Append(float64(i))
	}

	xArray := xBuilder.NewArray()
	defer xArray.Release()
	yArray := yBuilder.NewArray()
	defer yArray.Release()

	// Evaluate
	result, err := expr.Evaluate([]arrow.Array{xArray, yArray}, mem)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	defer result.Release()

	// Just verify it completed successfully
	if result.Len() != rowCount {
		t.Errorf("result length = %v, want %v", result.Len(), rowCount)
	}

	t.Logf("Successfully evaluated expression on %d rows", rowCount)
}
