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

// FunctionCategory classifies functions by their purpose.
type FunctionCategory int

const (
	// CategoryAggregate functions reduce arrays to scalar values (max, min, sum, mean).
	CategoryAggregate FunctionCategory = iota

	// CategoryString functions operate on string data (upper, lower, trim).
	CategoryString

	// CategoryCast functions convert between data types (cast, convert).
	CategoryCast

	// CategoryMath functions perform mathematical operations (abs, round, sqrt).
	CategoryMath

	// CategoryTemporal functions work with dates and times (year, month, day).
	CategoryTemporal

	// CategoryBoolean functions perform logical operations (and, or, not).
	CategoryBoolean

	// CategoryComparison functions compare values (eq, ne, gt, lt).
	CategoryComparison

	// CategoryBinary functions operate on two arrays (add, subtract, multiply).
	CategoryBinary

	// CategoryOther is for functions that don't fit other categories.
	CategoryOther
)

// String returns the string representation of a FunctionCategory.
func (c FunctionCategory) String() string {
	switch c {
	case CategoryAggregate:
		return "Aggregate"
	case CategoryString:
		return "String"
	case CategoryCast:
		return "Cast"
	case CategoryMath:
		return "Math"
	case CategoryTemporal:
		return "Temporal"
	case CategoryBoolean:
		return "Boolean"
	case CategoryComparison:
		return "Comparison"
	case CategoryBinary:
		return "Binary"
	case CategoryOther:
		return "Other"
	default:
		return "Unknown"
	}
}

// FunctionMetadata provides additional information about a function.
// This is useful for documentation, help systems, and function discovery.
type FunctionMetadata struct {
	// Name is the unique function identifier.
	Name string

	// Description explains what the function does.
	Description string

	// Category groups related functions.
	Category FunctionCategory

	// Examples demonstrate function usage.
	Examples []string

	// Signature is a human-readable type signature.
	// For example: "max(numeric) -> numeric" or "upper(string) -> string"
	Signature string

	// Complexity describes the algorithmic complexity (O(n), O(n log n), etc.).
	Complexity string

	// InPlace indicates if the function can modify the input array in place.
	InPlace bool

	// Vectorized indicates if the function uses SIMD/vectorized operations.
	Vectorized bool

	// Version indicates when the function was added (for compatibility).
	Version string
}

// NewMetadata creates basic metadata from a VectorFunction.
func NewMetadata(fn VectorFunction) FunctionMetadata {
	return FunctionMetadata{
		Name:        fn.Name(),
		Description: fn.Description(),
		Category:    fn.Category(),
		Signature:   inferSignature(fn),
		Complexity:  "O(n)", // Default assumption
		InPlace:     false,
		Vectorized:  true,
		Version:     "1.0.0",
	}
}

// inferSignature creates a basic signature from input/output types.
func inferSignature(fn VectorFunction) string {
	inputTypes := fn.InputTypes()
	if len(inputTypes) == 0 {
		return fn.Name() + "(any) -> any"
	}

	// Use first input type as example
	inputType := inputTypes[0]
	outputType, err := fn.OutputType(inputType)
	if err != nil {
		return fn.Name() + "(?) -> ?"
	}

	return fn.Name() + "(" + inputType.String() + ") -> " + outputType.String()
}
