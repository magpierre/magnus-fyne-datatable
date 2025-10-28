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

package compute

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// MockFunction is a simple mock implementation of VectorFunction for testing.
type MockFunction struct {
	BaseVectorFunction
}

func NewMockFunction(name string) *MockFunction {
	return &MockFunction{
		BaseVectorFunction: NewBaseVectorFunction(
			name,
			"Mock function for testing",
			CategoryOther,
			NumericTypes(),
		),
	}
}

func (m *MockFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	// Return same type as input
	return inputType, nil
}

func (m *MockFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	// Simple passthrough - return input as-is
	input.Retain()
	return input, nil
}

func TestFunctionRegistryRegister(t *testing.T) {
	registry := NewFunctionRegistry()

	// Test successful registration
	fn := NewMockFunction("test")
	err := registry.Register(fn)
	if err != nil {
		t.Fatalf("Failed to register function: %v", err)
	}

	// Verify function is registered
	if !registry.Has("test") {
		t.Error("Function should be registered")
	}

	// Test duplicate registration fails
	err = registry.Register(fn)
	if err == nil {
		t.Error("Expected error when registering duplicate function")
	}
}

func TestFunctionRegistryRegisterNil(t *testing.T) {
	registry := NewFunctionRegistry()

	// Test registering nil function
	err := registry.Register(nil)
	if err == nil {
		t.Error("Expected error when registering nil function")
	}
}

func TestFunctionRegistryGet(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register a function
	fn := NewMockFunction("test")
	registry.Register(fn)

	// Test successful retrieval
	retrieved, err := registry.Get("test")
	if err != nil {
		t.Fatalf("Failed to get function: %v", err)
	}

	if retrieved.Name() != "test" {
		t.Errorf("Expected function name 'test', got %q", retrieved.Name())
	}

	// Test getting non-existent function
	_, err = registry.Get("nonexistent")
	if err == nil {
		t.Error("Expected error when getting non-existent function")
	}
}

func TestFunctionRegistryUnregister(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register a function
	fn := NewMockFunction("test")
	registry.Register(fn)

	// Verify it exists
	if !registry.Has("test") {
		t.Fatal("Function should be registered")
	}

	// Unregister it
	err := registry.Unregister("test")
	if err != nil {
		t.Fatalf("Failed to unregister function: %v", err)
	}

	// Verify it's gone
	if registry.Has("test") {
		t.Error("Function should be unregistered")
	}

	// Test unregistering non-existent function
	err = registry.Unregister("nonexistent")
	if err == nil {
		t.Error("Expected error when unregistering non-existent function")
	}
}

func TestFunctionRegistryList(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register multiple functions
	fn1 := NewMockFunction("alpha")
	fn2 := NewMockFunction("beta")
	fn3 := NewMockFunction("gamma")

	registry.Register(fn1)
	registry.Register(fn2)
	registry.Register(fn3)

	// Get list
	names := registry.ListFunctions()

	// Should be 3 functions
	if len(names) != 3 {
		t.Errorf("Expected 3 functions, got %d", len(names))
	}

	// Should be alphabetically sorted
	expected := []string{"alpha", "beta", "gamma"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("Expected %q at position %d, got %q", expected[i], i, name)
		}
	}
}

func TestFunctionRegistryCount(t *testing.T) {
	registry := NewFunctionRegistry()

	// Should start empty
	if registry.Count() != 0 {
		t.Errorf("Expected count 0, got %d", registry.Count())
	}

	// Add functions
	registry.Register(NewMockFunction("fn1"))
	registry.Register(NewMockFunction("fn2"))

	if registry.Count() != 2 {
		t.Errorf("Expected count 2, got %d", registry.Count())
	}
}

func TestFunctionRegistryClear(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register functions
	registry.Register(NewMockFunction("fn1"))
	registry.Register(NewMockFunction("fn2"))

	// Clear
	registry.Clear()

	// Should be empty
	if registry.Count() != 0 {
		t.Errorf("Expected count 0 after clear, got %d", registry.Count())
	}

	if registry.Has("fn1") {
		t.Error("Function should be removed after clear")
	}
}

func TestFunctionRegistryListByCategory(t *testing.T) {
	registry := NewFunctionRegistry()

	// Create functions with different categories
	fn1 := &MockFunction{
		BaseVectorFunction: NewBaseVectorFunction("agg1", "Aggregate", CategoryAggregate, nil),
	}
	fn2 := &MockFunction{
		BaseVectorFunction: NewBaseVectorFunction("agg2", "Aggregate", CategoryAggregate, nil),
	}
	fn3 := &MockFunction{
		BaseVectorFunction: NewBaseVectorFunction("str1", "String", CategoryString, nil),
	}

	registry.Register(fn1)
	registry.Register(fn2)
	registry.Register(fn3)

	// List by category
	byCategory := registry.ListByCategory()

	// Check aggregate category
	aggFuncs := byCategory[CategoryAggregate]
	if len(aggFuncs) != 2 {
		t.Errorf("Expected 2 aggregate functions, got %d", len(aggFuncs))
	}

	// Check string category
	strFuncs := byCategory[CategoryString]
	if len(strFuncs) != 1 {
		t.Errorf("Expected 1 string function, got %d", len(strFuncs))
	}
}

func TestFunctionRegistryMetadata(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register function
	fn := NewMockFunction("test")
	registry.Register(fn)

	// Get metadata
	meta, err := registry.GetMetadata("test")
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	// Verify metadata
	if meta.Name != "test" {
		t.Errorf("Expected name 'test', got %q", meta.Name)
	}

	if meta.Category != CategoryOther {
		t.Errorf("Expected category Other, got %v", meta.Category)
	}
}

func TestMustRegister(t *testing.T) {
	registry := NewFunctionRegistry()

	// Should not panic for valid function
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("MustRegister panicked unexpectedly: %v", r)
		}
	}()

	fn := NewMockFunction("test")
	registry.MustRegister(fn)
}

func TestMustRegisterPanic(t *testing.T) {
	registry := NewFunctionRegistry()

	// Register function first
	fn := NewMockFunction("test")
	registry.Register(fn)

	// Should panic on duplicate
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustRegister should panic on duplicate registration")
		}
	}()

	registry.MustRegister(fn)
}

func TestGlobalRegistry(t *testing.T) {
	// Clear global registry first
	GetGlobalRegistry().Clear()

	// Test global convenience functions
	fn := NewMockFunction("global_test")

	// Register
	err := Register(fn)
	if err != nil {
		t.Fatalf("Failed to register to global registry: %v", err)
	}

	// Check existence
	if !Has("global_test") {
		t.Error("Function should exist in global registry")
	}

	// Get function
	retrieved, err := Get("global_test")
	if err != nil {
		t.Fatalf("Failed to get from global registry: %v", err)
	}

	if retrieved.Name() != "global_test" {
		t.Errorf("Expected name 'global_test', got %q", retrieved.Name())
	}

	// List functions
	names := ListFunctions()
	found := false
	for _, name := range names {
		if name == "global_test" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Function not found in global list")
	}

	// Clean up
	GetGlobalRegistry().Clear()
}

func TestBaseVectorFunctionValidate(t *testing.T) {
	// Create function that only accepts int64
	base := NewBaseVectorFunction(
		"test",
		"Test function",
		CategoryOther,
		[]arrow.DataType{arrow.PrimitiveTypes.Int64},
	)

	// Should accept int64
	err := base.Validate(arrow.PrimitiveTypes.Int64)
	if err != nil {
		t.Errorf("Should accept int64: %v", err)
	}

	// Should reject string
	err = base.Validate(arrow.BinaryTypes.String)
	if err == nil {
		t.Error("Should reject string type")
	}
}

func TestBaseVectorFunctionAcceptAnyType(t *testing.T) {
	// Create function with no input types (accepts any)
	base := NewBaseVectorFunction(
		"test",
		"Test function",
		CategoryOther,
		[]arrow.DataType{},
	)

	// Should accept any type
	err := base.Validate(arrow.PrimitiveTypes.Int64)
	if err != nil {
		t.Errorf("Should accept int64: %v", err)
	}

	err = base.Validate(arrow.BinaryTypes.String)
	if err != nil {
		t.Errorf("Should accept string: %v", err)
	}
}

func TestIsNumericType(t *testing.T) {
	tests := []struct {
		dt       arrow.DataType
		expected bool
	}{
		{arrow.PrimitiveTypes.Int64, true},
		{arrow.PrimitiveTypes.Float64, true},
		{arrow.BinaryTypes.String, false},
		{arrow.FixedWidthTypes.Boolean, false},
	}

	for _, tt := range tests {
		result := IsNumericType(tt.dt)
		if result != tt.expected {
			t.Errorf("IsNumericType(%v) = %v, expected %v", tt.dt, result, tt.expected)
		}
	}
}

func TestIsStringType(t *testing.T) {
	tests := []struct {
		dt       arrow.DataType
		expected bool
	}{
		{arrow.BinaryTypes.String, true},
		{arrow.BinaryTypes.LargeString, true},
		{arrow.PrimitiveTypes.Int64, false},
		{arrow.PrimitiveTypes.Float64, false},
	}

	for _, tt := range tests {
		result := IsStringType(tt.dt)
		if result != tt.expected {
			t.Errorf("IsStringType(%v) = %v, expected %v", tt.dt, result, tt.expected)
		}
	}
}
