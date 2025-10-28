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

func TestUpperFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"hello", "world", "Test"}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("upper")
	if err != nil {
		t.Fatalf("Failed to get upper function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	strArr := result.(*array.String)
	expected := []string{"HELLO", "WORLD", "TEST"}

	for i, exp := range expected {
		if strArr.Value(i) != exp {
			t.Errorf("Expected %q at index %d, got %q", exp, i, strArr.Value(i))
		}
	}
}

func TestLowerFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"HELLO", "WORLD", "Test"}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("lower")
	if err != nil {
		t.Fatalf("Failed to get lower function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	strArr := result.(*array.String)
	expected := []string{"hello", "world", "test"}

	for i, exp := range expected {
		if strArr.Value(i) != exp {
			t.Errorf("Expected %q at index %d, got %q", exp, i, strArr.Value(i))
		}
	}
}

func TestTrimFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"  hello  ", "\tworld\t", " test "}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("trim")
	if err != nil {
		t.Fatalf("Failed to get trim function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	strArr := result.(*array.String)
	expected := []string{"hello", "world", "test"}

	for i, exp := range expected {
		if strArr.Value(i) != exp {
			t.Errorf("Expected %q at index %d, got %q", exp, i, strArr.Value(i))
		}
	}
}

func TestLengthFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"hello", "world", "test", ""}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("length")
	if err != nil {
		t.Fatalf("Failed to get length function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	intArr := result.(*array.Int32)
	expected := []int32{5, 5, 4, 0}

	for i, exp := range expected {
		if intArr.Value(i) != exp {
			t.Errorf("Expected %d at index %d, got %d", exp, i, intArr.Value(i))
		}
	}
}

func TestStringFunctionWithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()

	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.Append("hello")
	builder.AppendNull()
	builder.Append("world")
	arr := builder.NewArray()
	defer arr.Release()

	fn, err := computepkg.Get("upper")
	if err != nil {
		t.Fatalf("Failed to get upper function: %v", err)
	}

	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	defer result.Release()

	strArr := result.(*array.String)

	// Check first value
	if strArr.Value(0) != "HELLO" {
		t.Errorf("Expected HELLO at index 0, got %q", strArr.Value(0))
	}

	// Check null is preserved
	if !strArr.IsNull(1) {
		t.Error("Expected null at index 1")
	}

	// Check third value
	if strArr.Value(2) != "WORLD" {
		t.Errorf("Expected WORLD at index 2, got %q", strArr.Value(2))
	}
}
