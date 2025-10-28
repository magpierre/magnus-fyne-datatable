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

// Package compute provides vectorized operations on Arrow arrays.
package compute

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// VectorFunction defines the interface for vectorized operations on Arrow arrays.
// Functions operate on entire columns (arrays) rather than individual values,
// enabling significant performance improvements through vectorization and SIMD.
type VectorFunction interface {
	// Name returns the unique function name (e.g., "max", "upper", "cast").
	Name() string

	// Description returns a human-readable description of what the function does.
	Description() string

	// Category returns the function category for organization.
	Category() FunctionCategory

	// InputTypes returns the expected input Arrow data types.
	// Multiple entries indicate type polymorphism (function accepts multiple types).
	// An empty slice means the function accepts any type.
	InputTypes() []arrow.DataType

	// OutputType returns the output Arrow data type given an input type.
	// Returns an error if the input type is not supported.
	OutputType(inputType arrow.DataType) (arrow.DataType, error)

	// Execute performs the vectorized operation on an entire Arrow array.
	//
	// Parameters:
	//   - input: the input Arrow array (entire column)
	//   - mem: memory allocator for creating new arrays
	//   - inPlace: hint that in-place modification is acceptable (optimization)
	//
	// Returns:
	//   - A new Arrow array with the results
	//   - An error if the operation fails
	//
	// Note: The returned array must be released by the caller.
	Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error)

	// Validate checks if the function can be applied to the given input type.
	// This allows early validation before execution.
	Validate(inputType arrow.DataType) error
}

// AggregateFunction is a specialized interface for aggregation operations.
// Aggregate functions reduce an array to a single scalar value.
type AggregateFunction interface {
	VectorFunction

	// Aggregate performs reduction operation on the entire array.
	// Returns a single scalar value representing the aggregate result.
	//
	// For example:
	//   - max: returns the maximum value
	//   - sum: returns the sum of all values
	//   - count: returns the number of non-null values
	Aggregate(input arrow.Array) (any, error)

	// SupportsGrouped indicates if this function can be used in grouped aggregations.
	// Most aggregate functions support grouping by default.
	SupportsGrouped() bool
}

// UnaryFunction is a specialized interface for element-wise transformations.
// Unary functions operate on each element independently and produce an array
// of the same length as the input.
type UnaryFunction interface {
	VectorFunction

	// Apply transforms each element in the input array.
	// The output array has the same length as the input.
	Apply(input arrow.Array, mem memory.Allocator) (arrow.Array, error)
}

// BinaryFunction is a specialized interface for operations on two arrays.
// Binary functions combine two arrays element-wise.
type BinaryFunction interface {
	// Name returns the unique function name.
	Name() string

	// Description returns a human-readable description.
	Description() string

	// Execute performs the binary operation on two arrays.
	// Both arrays must have the same length.
	Execute(left, right arrow.Array, mem memory.Allocator) (arrow.Array, error)

	// OutputType returns the output type given two input types.
	OutputType(leftType, rightType arrow.DataType) (arrow.DataType, error)

	// Validate checks if the operation is valid for the given input types.
	Validate(leftType, rightType arrow.DataType) error
}
