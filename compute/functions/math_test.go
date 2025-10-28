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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

func TestAbsFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{-5, 3, -2, 0, 7}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("abs")
	if err != nil {
		t.Fatalf("Failed to get abs function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	intArr := result.(*array.Int64)
	expected := []int64{5, 3, 2, 0, 7}

	for i, exp := range expected {
		if intArr.Value(i) != exp {
			t.Errorf("Expected %d at index %d, got %d", exp, i, intArr.Value(i))
		}
	}
}

func TestAbsWithFloats(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{-5.5, 3.2, -2.1, 0.0, 7.9}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("abs")
	if err != nil {
		t.Fatalf("Failed to get abs function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	floatArr := result.(*array.Float64)
	expected := []float64{5.5, 3.2, 2.1, 0.0, 7.9}

	for i, exp := range expected {
		if math.Abs(floatArr.Value(i)-exp) > 1e-10 {
			t.Errorf("Expected %f at index %d, got %f", exp, i, floatArr.Value(i))
		}
	}
}

func TestRoundFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{1.4, 1.5, 1.6, 2.5, -1.5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("round")
	if err != nil {
		t.Fatalf("Failed to get round function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	floatArr := result.(*array.Float64)

	// Note: Go's math.Round uses "round half away from zero"
	// So 1.5 -> 2, 2.5 -> 3, -1.5 -> -2
	expected := []float64{1.0, 2.0, 2.0, 3.0, -2.0}

	for i, exp := range expected {
		if math.Abs(floatArr.Value(i)-exp) > 1e-10 {
			t.Errorf("Expected %f at index %d, got %f", exp, i, floatArr.Value(i))
		}
	}
}

func TestCeilFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{1.1, 1.9, -1.1, -1.9, 0.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("ceil")
	if err != nil {
		t.Fatalf("Failed to get ceil function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	floatArr := result.(*array.Float64)
	expected := []float64{2.0, 2.0, -1.0, -1.0, 0.0}

	for i, exp := range expected {
		if math.Abs(floatArr.Value(i)-exp) > 1e-10 {
			t.Errorf("Expected %f at index %d, got %f", exp, i, floatArr.Value(i))
		}
	}
}

func TestFloorFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{1.1, 1.9, -1.1, -1.9, 0.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("floor")
	if err != nil {
		t.Fatalf("Failed to get floor function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	floatArr := result.(*array.Float64)
	expected := []float64{1.0, 1.0, -2.0, -2.0, 0.0}

	for i, exp := range expected {
		if math.Abs(floatArr.Value(i)-exp) > 1e-10 {
			t.Errorf("Expected %f at index %d, got %f", exp, i, floatArr.Value(i))
		}
	}
}
