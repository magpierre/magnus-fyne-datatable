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

	"github.com/apache/arrow-go/v18/arrow"
)

// BaseVectorFunction provides common functionality for VectorFunction implementations.
// Concrete functions can embed this struct to avoid reimplementing common methods.
type BaseVectorFunction struct {
	name        string
	description string
	inputTypes  []arrow.DataType
	category    FunctionCategory
}

// NewBaseVectorFunction creates a new base function with the given properties.
func NewBaseVectorFunction(name, description string, category FunctionCategory, inputTypes []arrow.DataType) BaseVectorFunction {
	return BaseVectorFunction{
		name:        name,
		description: description,
		inputTypes:  inputTypes,
		category:    category,
	}
}

// Name returns the function name.
func (b *BaseVectorFunction) Name() string {
	return b.name
}

// Description returns the function description.
func (b *BaseVectorFunction) Description() string {
	return b.description
}

// Category returns the function category.
func (b *BaseVectorFunction) Category() FunctionCategory {
	return b.category
}

// InputTypes returns the accepted input types.
func (b *BaseVectorFunction) InputTypes() []arrow.DataType {
	return b.inputTypes
}

// Validate checks if the input type is acceptable for this function.
// Returns nil if the type is valid, an error otherwise.
func (b *BaseVectorFunction) Validate(inputType arrow.DataType) error {
	// If no input types specified, accept any type
	if len(b.inputTypes) == 0 {
		return nil
	}

	// Check if input type matches any accepted type
	for _, acceptable := range b.inputTypes {
		if arrow.TypeEqual(inputType, acceptable) {
			return nil
		}
	}

	return fmt.Errorf("function %q does not support input type %v", b.name, inputType)
}

// BaseAggregateFunction provides common functionality for aggregate functions.
type BaseAggregateFunction struct {
	BaseVectorFunction
	supportsGrouped bool
}

// NewBaseAggregateFunction creates a new base aggregate function.
func NewBaseAggregateFunction(name, description string, inputTypes []arrow.DataType) BaseAggregateFunction {
	return BaseAggregateFunction{
		BaseVectorFunction: NewBaseVectorFunction(name, description, CategoryAggregate, inputTypes),
		supportsGrouped:    true, // Most aggregates support grouping
	}
}

// SupportsGrouped returns whether this function can be used in grouped aggregations.
func (b *BaseAggregateFunction) SupportsGrouped() bool {
	return b.supportsGrouped
}

// SetSupportsGrouped sets whether this function supports grouped aggregations.
func (b *BaseAggregateFunction) SetSupportsGrouped(supported bool) {
	b.supportsGrouped = supported
}

// Helper functions for type checking

// IsNumericType checks if an Arrow type is numeric.
func IsNumericType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	default:
		return false
	}
}

// IsIntegerType checks if an Arrow type is an integer.
func IsIntegerType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	default:
		return false
	}
}

// IsFloatType checks if an Arrow type is a floating-point number.
func IsFloatType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

// IsStringType checks if an Arrow type is a string.
func IsStringType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return true
	default:
		return false
	}
}

// IsTemporalType checks if an Arrow type represents date/time data.
func IsTemporalType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP,
		arrow.TIME32, arrow.TIME64, arrow.DURATION,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return true
	default:
		return false
	}
}

// NumericTypes returns all numeric Arrow types.
func NumericTypes() []arrow.DataType {
	return []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}
}

// StringTypes returns all string Arrow types.
func StringTypes() []arrow.DataType {
	return []arrow.DataType{
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeString,
	}
}
