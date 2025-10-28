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

package functions

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

func TestMaxFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 5, 3, 9, 2}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	// Get max function
	fn, err := computepkg.Get("max")
	if err != nil {
		t.Fatalf("Failed to get max function: %v", err)
	}

	// Execute
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	// Verify result
	if result.Len() != 1 {
		t.Errorf("Expected 1 element, got %d", result.Len())
	}

	maxVal := result.(*array.Int64).Value(0)
	if maxVal != 9 {
		t.Errorf("Expected max 9, got %d", maxVal)
	}
}

func TestMinFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 5, 3, 9, 2}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("min")
	if err != nil {
		t.Fatalf("Failed to get min function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	minVal := result.(*array.Int64).Value(0)
	if minVal != 1 {
		t.Errorf("Expected min 1, got %d", minVal)
	}
}

func TestSumFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("sum")
	if err != nil {
		t.Fatalf("Failed to get sum function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	sumVal := result.(*array.Int64).Value(0)
	if sumVal != 15 {
		t.Errorf("Expected sum 15, got %d", sumVal)
	}
}

func TestMeanFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("mean")
	if err != nil {
		t.Fatalf("Failed to get mean function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	meanVal := result.(*array.Float64).Value(0)
	if meanVal != 3.0 {
		t.Errorf("Expected mean 3.0, got %f", meanVal)
	}
}

func TestCountFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	// Add some values with nulls
	builder.Append(1)
	builder.Append(2)
	builder.AppendNull()
	builder.Append(4)
	builder.Append(5)

	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("count")
	if err != nil {
		t.Fatalf("Failed to get count function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	countVal := result.(*array.Int64).Value(0)
	if countVal != 4 {
		t.Errorf("Expected count 4 (excluding null), got %d", countVal)
	}
}

func TestMaxWithFloats(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{1.5, 5.2, 3.7, 9.1, 2.3}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("max")
	if err != nil {
		t.Fatalf("Failed to get max function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	maxVal := result.(*array.Float64).Value(0)
	if maxVal != 9.1 {
		t.Errorf("Expected max 9.1, got %f", maxVal)
	}
}

func TestAggregateInterface(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 5, 3, 9, 2}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	// Get max function and cast to AggregateFunction
	fn, err := computepkg.Get("max")
	if err != nil {
		t.Fatalf("Failed to get max function: %v", err)
	}

	aggFn, ok := fn.(computepkg.AggregateFunction)
	if !ok {
		t.Fatal("Max function should implement AggregateFunction interface")
	}

	// Test Aggregate method
	result, err := aggFn.Aggregate(arr)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	maxVal, ok := result.(int64)
	if !ok {
		t.Fatalf("Expected int64 result, got %T", result)
	}

	if maxVal != 9 {
		t.Errorf("Expected max 9, got %d", maxVal)
	}

	// Test SupportsGrouped
	if !aggFn.SupportsGrouped() {
		t.Error("Max function should support grouped aggregation")
	}
}

func TestInvalidType(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create string array (invalid for max)
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"a", "b", "c"}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("max")
	if err != nil {
		t.Fatalf("Failed to get max function: %v", err)
	}

	// Should fail validation
	_, err = fn.Execute(arr, mem, false)
	if err == nil {
		t.Error("Expected error when executing max on string array")
	}
}
