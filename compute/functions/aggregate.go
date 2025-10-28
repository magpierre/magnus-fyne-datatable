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

// Package functions provides built-in vectorized functions.
package functions

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

// MaxFunction computes the maximum value in a numeric array.
type MaxFunction struct {
	computepkg.BaseAggregateFunction
}

func init() {
	computepkg.MustRegister(NewMaxFunction())
}

// NewMaxFunction creates a new max function.
func NewMaxFunction() *MaxFunction {
	return &MaxFunction{
		BaseAggregateFunction: computepkg.NewBaseAggregateFunction(
			"max",
			"Compute maximum value",
			computepkg.NumericTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *MaxFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute computes the maximum and returns a single-element array.
func (f *MaxFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	maxVal, err := f.Aggregate(input)
	if err != nil {
		return nil, err
	}

	// Create single-element array with the max value
	return buildSingleElementArray(mem, input.DataType(), maxVal)
}

// Aggregate returns the max value as a scalar.
func (f *MaxFunction) Aggregate(input arrow.Array) (any, error) {
	return computeMax(input)
}

// MinFunction computes the minimum value in a numeric array.
type MinFunction struct {
	computepkg.BaseAggregateFunction
}

func init() {
	computepkg.MustRegister(NewMinFunction())
}

// NewMinFunction creates a new min function.
func NewMinFunction() *MinFunction {
	return &MinFunction{
		BaseAggregateFunction: computepkg.NewBaseAggregateFunction(
			"min",
			"Compute minimum value",
			computepkg.NumericTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *MinFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute computes the minimum and returns a single-element array.
func (f *MinFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	minVal, err := f.Aggregate(input)
	if err != nil {
		return nil, err
	}

	return buildSingleElementArray(mem, input.DataType(), minVal)
}

// Aggregate returns the min value as a scalar.
func (f *MinFunction) Aggregate(input arrow.Array) (any, error) {
	return computeMin(input)
}

// SumFunction computes the sum of values in a numeric array.
type SumFunction struct {
	computepkg.BaseAggregateFunction
}

func init() {
	computepkg.MustRegister(NewSumFunction())
}

// NewSumFunction creates a new sum function.
func NewSumFunction() *SumFunction {
	return &SumFunction{
		BaseAggregateFunction: computepkg.NewBaseAggregateFunction(
			"sum",
			"Compute sum of values",
			computepkg.NumericTypes(),
		),
	}
}

// OutputType returns the same type as input (or promoted type).
func (f *SumFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute computes the sum and returns a single-element array.
func (f *SumFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	sumVal, err := f.Aggregate(input)
	if err != nil {
		return nil, err
	}

	return buildSingleElementArray(mem, input.DataType(), sumVal)
}

// Aggregate returns the sum as a scalar.
func (f *SumFunction) Aggregate(input arrow.Array) (any, error) {
	return computeSum(input)
}

// MeanFunction computes the mean (average) of values in a numeric array.
type MeanFunction struct {
	computepkg.BaseAggregateFunction
}

func init() {
	computepkg.MustRegister(NewMeanFunction())
}

// NewMeanFunction creates a new mean function.
func NewMeanFunction() *MeanFunction {
	return &MeanFunction{
		BaseAggregateFunction: computepkg.NewBaseAggregateFunction(
			"mean",
			"Compute mean (average) value",
			computepkg.NumericTypes(),
		),
	}
}

// OutputType returns float64 for mean.
func (f *MeanFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return arrow.PrimitiveTypes.Float64, nil
}

// Execute computes the mean and returns a single-element array.
func (f *MeanFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	meanVal, err := f.Aggregate(input)
	if err != nil {
		return nil, err
	}

	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	if meanVal != nil {
		builder.Append(meanVal.(float64))
	} else {
		builder.AppendNull()
	}

	return builder.NewArray(), nil
}

// Aggregate returns the mean as a scalar.
func (f *MeanFunction) Aggregate(input arrow.Array) (any, error) {
	return computeMean(input)
}

// CountFunction counts non-null values in an array.
type CountFunction struct {
	computepkg.BaseAggregateFunction
}

func init() {
	computepkg.MustRegister(NewCountFunction())
}

// NewCountFunction creates a new count function.
func NewCountFunction() *CountFunction {
	return &CountFunction{
		BaseAggregateFunction: computepkg.NewBaseAggregateFunction(
			"count",
			"Count non-null values",
			[]arrow.DataType{}, // Accept any type
		),
	}
}

// OutputType returns int64 for count.
func (f *CountFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	return arrow.PrimitiveTypes.Int64, nil
}

// Execute counts non-null values and returns a single-element array.
func (f *CountFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	count := int64(input.Len() - input.NullN())

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	builder.Append(count)
	return builder.NewArray(), nil
}

// Aggregate returns the count as a scalar.
func (f *CountFunction) Aggregate(input arrow.Array) (any, error) {
	return int64(input.Len() - input.NullN()), nil
}

// Helper functions for aggregate computations

// buildSingleElementArray creates a single-element array with the given value
func buildSingleElementArray(mem memory.Allocator, dt arrow.DataType, value any) (arrow.Array, error) {
	if value == nil {
		builder := array.NewBuilder(mem, dt)
		defer builder.Release()
		builder.AppendNull()
		return builder.NewArray(), nil
	}

	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.Append(value.(int64))
		return builder.NewArray(), nil

	case arrow.FLOAT32, arrow.FLOAT64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.Append(value.(float64))
		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported type for single element array: %v", dt)
	}
}

// computeMax computes maximum value from an array
func computeMax(input arrow.Array) (any, error) {
	if input.Len() == 0 {
		return nil, nil
	}

	var maxVal any
	hasValue := false

	switch arr := input.(type) {
	case *array.Int64:
		var max int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if !hasValue || val > max {
					max = val
					hasValue = true
				}
			}
		}
		if hasValue {
			maxVal = max
		}

	case *array.Float64:
		var max float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if !hasValue || val > max {
					max = val
					hasValue = true
				}
			}
		}
		if hasValue {
			maxVal = max
		}

	default:
		return nil, fmt.Errorf("unsupported type for max: %v", input.DataType())
	}

	return maxVal, nil
}

// computeMin computes minimum value from an array
func computeMin(input arrow.Array) (any, error) {
	if input.Len() == 0 {
		return nil, nil
	}

	var minVal any
	hasValue := false

	switch arr := input.(type) {
	case *array.Int64:
		var min int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if !hasValue || val < min {
					min = val
					hasValue = true
				}
			}
		}
		if hasValue {
			minVal = min
		}

	case *array.Float64:
		var min float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				val := arr.Value(i)
				if !hasValue || val < min {
					min = val
					hasValue = true
				}
			}
		}
		if hasValue {
			minVal = min
		}

	default:
		return nil, fmt.Errorf("unsupported type for min: %v", input.DataType())
	}

	return minVal, nil
}

// computeSum computes sum of values in an array
func computeSum(input arrow.Array) (any, error) {
	if input.Len() == 0 {
		return nil, nil
	}

	switch arr := input.(type) {
	case *array.Int64:
		var sum int64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
			}
		}
		return sum, nil

	case *array.Float64:
		var sum float64
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				sum += arr.Value(i)
			}
		}
		return sum, nil

	default:
		return nil, fmt.Errorf("unsupported type for sum: %v", input.DataType())
	}
}

// computeMean computes mean of values in an array
func computeMean(input arrow.Array) (any, error) {
	if input.Len() == 0 {
		return nil, nil
	}

	sumVal, err := computeSum(input)
	if err != nil {
		return nil, err
	}

	if sumVal == nil {
		return nil, nil
	}

	count := float64(input.Len() - input.NullN())
	if count == 0 {
		return nil, nil
	}

	switch sum := sumVal.(type) {
	case int64:
		return float64(sum) / count, nil
	case float64:
		return sum / count, nil
	default:
		return nil, fmt.Errorf("unexpected sum type: %T", sum)
	}
}
