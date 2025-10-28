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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewExpression(t *testing.T) {
	tests := []struct {
		name         string
		source       string
		inputColumns []string
		outputType   arrow.DataType
		wantErr      bool
	}{
		{
			name:         "valid simple expression",
			source:       "x + 1",
			inputColumns: []string{"x"},
			outputType:   arrow.PrimitiveTypes.Float64,
			wantErr:      false,
		},
		{
			name:         "valid string expression",
			source:       "upper(name)",
			inputColumns: []string{"name"},
			outputType:   arrow.BinaryTypes.String,
			wantErr:      false,
		},
		{
			name:         "empty source",
			source:       "",
			inputColumns: []string{"x"},
			outputType:   arrow.PrimitiveTypes.Float64,
			wantErr:      true,
		},
		{
			name:         "nil output type",
			source:       "x + 1",
			inputColumns: []string{"x"},
			outputType:   nil,
			wantErr:      true,
		},
		{
			name:         "invalid syntax",
			source:       "x +",
			inputColumns: []string{"x"},
			outputType:   arrow.PrimitiveTypes.Float64,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.source, tt.inputColumns, tt.outputType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && expr == nil {
				t.Error("NewExpression() returned nil expression without error")
			}
			if !tt.wantErr {
				if expr.Source() != tt.source {
					t.Errorf("Source() = %v, want %v", expr.Source(), tt.source)
				}
				if len(expr.InputColumns()) != len(tt.inputColumns) {
					t.Errorf("InputColumns() length = %v, want %v", len(expr.InputColumns()), len(tt.inputColumns))
				}
			}
		})
	}
}

func TestExpression_Evaluate_Numeric(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		inputData  map[string][]float64
		expected   []float64
	}{
		{
			name:       "simple addition",
			expression: "x + y",
			inputData: map[string][]float64{
				"x": {1, 2, 3},
				"y": {10, 20, 30},
			},
			expected: []float64{11, 22, 33},
		},
		{
			name:       "multiplication",
			expression: "x * y",
			inputData: map[string][]float64{
				"x": {2, 3, 4},
				"y": {5, 6, 7},
			},
			expected: []float64{10, 18, 28},
		},
		{
			name:       "complex expression",
			expression: "(x + y) * 2",
			inputData: map[string][]float64{
				"x": {1, 2, 3},
				"y": {4, 5, 6},
			},
			expected: []float64{10, 14, 18},
		},
		{
			name:       "math function",
			expression: "abs(x)",
			inputData: map[string][]float64{
				"x": {-1, -2, 3},
			},
			expected: []float64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract column names
			var inputColumns []string
			for col := range tt.inputData {
				inputColumns = append(inputColumns, col)
			}

			// Create expression
			expr, err := NewExpression(tt.expression, inputColumns, arrow.PrimitiveTypes.Float64)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input arrays
			inputs := make([]arrow.Array, len(inputColumns))
			for i, colName := range inputColumns {
				builder := array.NewFloat64Builder(mem)
				defer builder.Release()

				for _, val := range tt.inputData[colName] {
					builder.Append(val)
				}

				inputs[i] = builder.NewArray()
				defer inputs[i].Release()
			}

			// Evaluate
			result, err := expr.Evaluate(inputs, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify result
			resultArray := result.(*array.Float64)
			if resultArray.Len() != len(tt.expected) {
				t.Fatalf("result length = %v, want %v", resultArray.Len(), len(tt.expected))
			}

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

func TestExpression_Evaluate_String(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		inputData  map[string][]string
		expected   []string
	}{
		{
			name:       "upper case",
			expression: "upper(name)",
			inputData: map[string][]string{
				"name": {"alice", "bob", "charlie"},
			},
			expected: []string{"ALICE", "BOB", "CHARLIE"},
		},
		{
			name:       "lower case",
			expression: "lower(name)",
			inputData: map[string][]string{
				"name": {"ALICE", "BOB", "CHARLIE"},
			},
			expected: []string{"alice", "bob", "charlie"},
		},
		{
			name:       "trim whitespace",
			expression: "trim(name)",
			inputData: map[string][]string{
				"name": {"  alice  ", "  bob  ", "  charlie  "},
			},
			expected: []string{"alice", "bob", "charlie"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract column names
			var inputColumns []string
			for col := range tt.inputData {
				inputColumns = append(inputColumns, col)
			}

			// Create expression
			expr, err := NewExpression(tt.expression, inputColumns, arrow.BinaryTypes.String)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input arrays
			inputs := make([]arrow.Array, len(inputColumns))
			for i, colName := range inputColumns {
				builder := array.NewStringBuilder(mem)
				defer builder.Release()

				for _, val := range tt.inputData[colName] {
					builder.Append(val)
				}

				inputs[i] = builder.NewArray()
				defer inputs[i].Release()
			}

			// Evaluate
			result, err := expr.Evaluate(inputs, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify result
			resultArray := result.(*array.String)
			if resultArray.Len() != len(tt.expected) {
				t.Fatalf("result length = %v, want %v", resultArray.Len(), len(tt.expected))
			}

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

func TestExpression_Evaluate_Boolean(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name       string
		expression string
		inputData  map[string][]float64
		expected   []bool
	}{
		{
			name:       "greater than",
			expression: "x > 5",
			inputData: map[string][]float64{
				"x": {1, 5, 10},
			},
			expected: []bool{false, false, true},
		},
		{
			name:       "equality",
			expression: "x == y",
			inputData: map[string][]float64{
				"x": {1, 2, 3},
				"y": {1, 0, 3},
			},
			expected: []bool{true, false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract column names
			var inputColumns []string
			for col := range tt.inputData {
				inputColumns = append(inputColumns, col)
			}

			// Create expression
			expr, err := NewExpression(tt.expression, inputColumns, arrow.FixedWidthTypes.Boolean)
			if err != nil {
				t.Fatalf("NewExpression() error = %v", err)
			}

			// Build input arrays
			inputs := make([]arrow.Array, len(inputColumns))
			for i, colName := range inputColumns {
				builder := array.NewFloat64Builder(mem)
				defer builder.Release()

				for _, val := range tt.inputData[colName] {
					builder.Append(val)
				}

				inputs[i] = builder.NewArray()
				defer inputs[i].Release()
			}

			// Evaluate
			result, err := expr.Evaluate(inputs, mem)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			defer result.Release()

			// Verify result
			resultArray := result.(*array.Boolean)
			if resultArray.Len() != len(tt.expected) {
				t.Fatalf("result length = %v, want %v", resultArray.Len(), len(tt.expected))
			}

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

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "simple expression",
			input:   "x + 1",
			wantErr: false,
		},
		{
			name:    "string expression",
			input:   "upper(name)",
			wantErr: false,
		},
		{
			name:    "complex expression",
			input:   "(price * quantity) * (1 + tax)",
			wantErr: false,
		},
		{
			name:    "empty expression",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			input:   "   ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && expr == nil {
				t.Error("Parse() returned nil expression without error")
			}
		})
	}
}

func TestParseWithContext(t *testing.T) {
	availableColumns := []string{"name", "age", "salary"}

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid column reference",
			input:   "upper(name)",
			wantErr: false,
		},
		{
			name:    "valid multi-column",
			input:   "salary > 50000",
			wantErr: false,
		},
		{
			name:    "empty expression",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := ParseWithContext(tt.input, availableColumns, arrow.BinaryTypes.String)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWithContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && expr == nil {
				t.Error("ParseWithContext() returned nil expression without error")
			}
		})
	}
}

func TestExtractColumnReferences(t *testing.T) {
	tests := []struct {
		name             string
		expression       string
		availableColumns []string
		want             []string
	}{
		{
			name:             "single column",
			expression:       "upper(name)",
			availableColumns: []string{"name", "age"},
			want:             []string{"name"},
		},
		{
			name:             "multiple columns",
			expression:       "price * quantity",
			availableColumns: []string{"price", "quantity", "tax"},
			want:             []string{"price", "quantity"},
		},
		{
			name:             "no columns",
			expression:       "42",
			availableColumns: []string{"x", "y"},
			want:             []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColumnReferences(tt.expression, tt.availableColumns)
			if len(got) != len(tt.want) {
				t.Errorf("extractColumnReferences() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpression_Validate(t *testing.T) {
	// Valid expression
	expr, err := Parse("x + 1")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if err := expr.Validate(); err != nil {
		t.Errorf("Validate() on valid expression returned error: %v", err)
	}

	// Invalid expression (no program)
	invalidExpr := &Expression{
		source:       "test",
		program:      nil,
		inputColumns: []string{"x"},
		outputType:   arrow.PrimitiveTypes.Float64,
	}

	if err := invalidExpr.Validate(); err == nil {
		t.Error("Validate() on invalid expression should return error")
	}
}
