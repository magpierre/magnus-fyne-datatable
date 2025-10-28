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

func TestCastIntToFloat(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("cast_float64")
	if err != nil {
		t.Fatalf("Failed to get cast_float64 function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	floatArr := result.(*array.Float64)
	expected := []float64{1.0, 2.0, 3.0, 4.0, 5.0}

	for i, exp := range expected {
		if floatArr.Value(i) != exp {
			t.Errorf("Expected %f at index %d, got %f", exp, i, floatArr.Value(i))
		}
	}
}

func TestCastFloatToInt(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{1.9, 2.1, 3.5, 4.0, 5.8}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("cast_int64")
	if err != nil {
		t.Fatalf("Failed to get cast_int64 function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	intArr := result.(*array.Int64)
	expected := []int64{1, 2, 3, 4, 5}

	for i, exp := range expected {
		if intArr.Value(i) != exp {
			t.Errorf("Expected %d at index %d, got %d", exp, i, intArr.Value(i))
		}
	}
}

func TestCastIntToString(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("cast_string")
	if err != nil {
		t.Fatalf("Failed to get cast_string function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	strArr := result.(*array.String)
	expected := []string{"1", "2", "3", "4", "5"}

	for i, exp := range expected {
		if strArr.Value(i) != exp {
			t.Errorf("Expected %q at index %d, got %q", exp, i, strArr.Value(i))
		}
	}
}

func TestCastStringToInt(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"1", "2", "3", "4", "5"}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("cast_int64")
	if err != nil {
		t.Fatalf("Failed to get cast_int64 function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	intArr := result.(*array.Int64)
	expected := []int64{1, 2, 3, 4, 5}

	for i, exp := range expected {
		if intArr.Value(i) != exp {
			t.Errorf("Expected %d at index %d, got %d", exp, i, intArr.Value(i))
		}
	}
}

func TestCastBool(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{0, 1, 2, 0, 1}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("cast_bool")
	if err != nil {
		t.Fatalf("Failed to get cast_bool function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, true, true, false, true}

	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("Expected %v at index %d, got %v", exp, i, boolArr.Value(i))
		}
	}
}
