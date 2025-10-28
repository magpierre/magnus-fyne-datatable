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

/*
Package compute provides vectorized operations on Apache Arrow arrays.

This package implements a function registry system that enables efficient
columnar operations on Arrow data. Functions operate on entire arrays
(columns) rather than individual values, providing 10-100x performance
improvements through vectorization and SIMD acceleration.

# Overview

The compute package consists of three main components:

1. Function Interface: VectorFunction defines the contract for vectorized operations
2. Function Registry: Thread-safe registration and lookup of functions
3. Base Implementations: Helper types for creating custom functions

# Basic Usage

Register and use vectorized functions:

	import (
		"github.com/magpierre/fyne-datatable/compute"
		"github.com/apache/arrow-go/v18/arrow/memory"
	)

	// Get a function from the registry
	maxFn, err := compute.Get("max")
	if err != nil {
		panic(err)
	}

	// Execute on an Arrow array
	result, err := maxFn.Execute(inputArray, memory.NewGoAllocator(), false)
	if err != nil {
		panic(err)
	}
	defer result.Release()

# Creating Custom Functions

Implement the VectorFunction interface:

	type DoubleFunction struct {
		compute.BaseVectorFunction
	}

	func NewDoubleFunction() *DoubleFunction {
		return &DoubleFunction{
			BaseVectorFunction: compute.NewBaseVectorFunction(
				"double",
				"Multiply all values by 2",
				compute.CategoryMath,
				compute.NumericTypes(),
			),
		}
	}

	func (f *DoubleFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
		return inputType, nil
	}

	func (f *DoubleFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
		// Implementation here
	}

	// Register the function
	compute.MustRegister(NewDoubleFunction())

# Function Categories

Functions are organized into categories:

  - Aggregate: max, min, sum, mean (reduce arrays to scalars)
  - String: upper, lower, trim (string transformations)
  - Cast: type conversions
  - Math: abs, round, sqrt (mathematical operations)
  - Temporal: year, month, day (date/time operations)
  - Boolean: and, or, not (logical operations)
  - Comparison: eq, ne, gt, lt (value comparisons)

# Thread Safety

The FunctionRegistry is thread-safe and can be used concurrently from
multiple goroutines. Both the global registry and custom registry instances
support concurrent registration and lookup operations.

# Performance

Vectorized functions operate on entire columns, enabling:

  - SIMD acceleration through Arrow compute kernels
  - Better CPU cache utilization
  - Reduced function call overhead
  - 10-100x performance improvement over row-by-row operations

# Memory Management

Functions work with Apache Arrow's memory management:

  - Always release returned arrays: defer result.Release()
  - Use provided memory allocators
  - Avoid memory leaks through proper reference counting

For more information, see the function registry plan documentation.
*/
package compute
