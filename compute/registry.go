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
	"fmt"
	"sort"
	"sync"
)

// FunctionRegistry manages registered vectorized functions.
// It provides thread-safe registration and lookup of functions by name.
type FunctionRegistry struct {
	functions map[string]VectorFunction
	metadata  map[string]FunctionMetadata
	mu        sync.RWMutex
}

// NewFunctionRegistry creates a new empty function registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]VectorFunction),
		metadata:  make(map[string]FunctionMetadata),
	}
}

// Global registry instance used by package-level convenience functions.
var globalRegistry = NewFunctionRegistry()

// Register adds a function to the registry.
// Returns an error if:
//   - The function is nil
//   - The function name is empty
//   - A function with the same name already exists
func (r *FunctionRegistry) Register(fn VectorFunction) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fn == nil {
		return fmt.Errorf("cannot register nil function")
	}

	name := fn.Name()
	if name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %q already registered", name)
	}

	// Register function
	r.functions[name] = fn

	// Generate and store metadata
	r.metadata[name] = NewMetadata(fn)

	return nil
}

// RegisterWithMetadata adds a function with custom metadata.
func (r *FunctionRegistry) RegisterWithMetadata(fn VectorFunction, meta FunctionMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fn == nil {
		return fmt.Errorf("cannot register nil function")
	}

	name := fn.Name()
	if name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %q already registered", name)
	}

	r.functions[name] = fn
	r.metadata[name] = meta

	return nil
}

// Unregister removes a function from the registry.
// Returns an error if the function doesn't exist.
func (r *FunctionRegistry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.functions[name]; !exists {
		return fmt.Errorf("function %q not found", name)
	}

	delete(r.functions, name)
	delete(r.metadata, name)

	return nil
}

// Get retrieves a function by name.
// Returns an error if the function is not found.
func (r *FunctionRegistry) Get(name string) (VectorFunction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, exists := r.functions[name]
	if !exists {
		return nil, fmt.Errorf("function %q not found", name)
	}

	return fn, nil
}

// GetMetadata retrieves metadata for a function.
// Returns an error if the function is not found.
func (r *FunctionRegistry) GetMetadata(name string) (FunctionMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, exists := r.metadata[name]
	if !exists {
		return FunctionMetadata{}, fmt.Errorf("function %q not found", name)
	}

	return meta, nil
}

// Has checks if a function is registered.
func (r *FunctionRegistry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.functions[name]
	return exists
}

// MustRegister registers a function or panics on error.
// This is useful for init() functions where registration should always succeed.
func (r *FunctionRegistry) MustRegister(fn VectorFunction) {
	if err := r.Register(fn); err != nil {
		panic(fmt.Sprintf("failed to register function: %v", err))
	}
}

// ListFunctions returns all registered function names in alphabetical order.
func (r *FunctionRegistry) ListFunctions() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// ListByCategory returns function names grouped by category.
func (r *FunctionRegistry) ListByCategory() map[FunctionCategory][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[FunctionCategory][]string)

	for name, fn := range r.functions {
		category := fn.Category()
		result[category] = append(result[category], name)
	}

	// Sort each category's functions
	for category := range result {
		sort.Strings(result[category])
	}

	return result
}

// Count returns the number of registered functions.
func (r *FunctionRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.functions)
}

// Clear removes all registered functions.
// This is mainly useful for testing.
func (r *FunctionRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.functions = make(map[string]VectorFunction)
	r.metadata = make(map[string]FunctionMetadata)
}

// Package-level convenience functions that use the global registry.

// Register adds a function to the global registry.
func Register(fn VectorFunction) error {
	return globalRegistry.Register(fn)
}

// RegisterWithMetadata adds a function with custom metadata to the global registry.
func RegisterWithMetadata(fn VectorFunction, meta FunctionMetadata) error {
	return globalRegistry.RegisterWithMetadata(fn, meta)
}

// Get retrieves a function from the global registry.
func Get(name string) (VectorFunction, error) {
	return globalRegistry.Get(name)
}

// GetMetadata retrieves metadata from the global registry.
func GetMetadata(name string) (FunctionMetadata, error) {
	return globalRegistry.GetMetadata(name)
}

// Has checks if a function exists in the global registry.
func Has(name string) bool {
	return globalRegistry.Has(name)
}

// MustRegister registers a function to the global registry or panics.
func MustRegister(fn VectorFunction) {
	globalRegistry.MustRegister(fn)
}

// ListFunctions returns all function names from the global registry.
func ListFunctions() []string {
	return globalRegistry.ListFunctions()
}

// ListByCategory returns functions grouped by category from the global registry.
func ListByCategory() map[FunctionCategory][]string {
	return globalRegistry.ListByCategory()
}

// GetGlobalRegistry returns the global registry instance.
// This can be useful for advanced scenarios or testing.
func GetGlobalRegistry() *FunctionRegistry {
	return globalRegistry
}
