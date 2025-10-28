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
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

// AbsFunction computes absolute value.
type AbsFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewAbsFunction())
}

// NewAbsFunction creates a new abs function.
func NewAbsFunction() *AbsFunction {
	return &AbsFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"abs",
			"Compute absolute value",
			computepkg.CategoryMath,
			computepkg.NumericTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *AbsFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute computes absolute value of each element.
func (f *AbsFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	switch arr := input.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				val := arr.Value(i)
				if val < 0 {
					builder.Append(-val)
				} else {
					builder.Append(val)
				}
			}
		}
		return builder.NewArray(), nil

	case *array.Float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(math.Abs(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported type for abs: %v", input.DataType())
	}
}

// RoundFunction rounds to nearest integer.
type RoundFunction struct {
	computepkg.BaseVectorFunction
	ndigits int64
}

func init() {
	computepkg.MustRegister(NewRoundFunction())
}

// NewRoundFunction creates a new round function (rounds to nearest integer).
func NewRoundFunction() *RoundFunction {
	return &RoundFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"round",
			"Round to nearest integer",
			computepkg.CategoryMath,
			[]arrow.DataType{
				arrow.PrimitiveTypes.Float32,
				arrow.PrimitiveTypes.Float64,
			},
		),
		ndigits: 0,
	}
}

// OutputType returns the same type as input.
func (f *RoundFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute rounds each element to nearest integer.
func (f *RoundFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	floatArr := input.(*array.Float64)
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	for i := 0; i < floatArr.Len(); i++ {
		if floatArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(math.Round(floatArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// CeilFunction rounds up to nearest integer.
type CeilFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewCeilFunction())
}

// NewCeilFunction creates a new ceil function.
func NewCeilFunction() *CeilFunction {
	return &CeilFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"ceil",
			"Round up to nearest integer",
			computepkg.CategoryMath,
			[]arrow.DataType{
				arrow.PrimitiveTypes.Float32,
				arrow.PrimitiveTypes.Float64,
			},
		),
	}
}

// OutputType returns the same type as input.
func (f *CeilFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute rounds up each element.
func (f *CeilFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	floatArr := input.(*array.Float64)
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	for i := 0; i < floatArr.Len(); i++ {
		if floatArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(math.Ceil(floatArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// FloorFunction rounds down to nearest integer.
type FloorFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewFloorFunction())
}

// NewFloorFunction creates a new floor function.
func NewFloorFunction() *FloorFunction {
	return &FloorFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"floor",
			"Round down to nearest integer",
			computepkg.CategoryMath,
			[]arrow.DataType{
				arrow.PrimitiveTypes.Float32,
				arrow.PrimitiveTypes.Float64,
			},
		),
	}
}

// OutputType returns the same type as input.
func (f *FloorFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute rounds down each element.
func (f *FloorFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	floatArr := input.(*array.Float64)
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	for i := 0; i < floatArr.Len(); i++ {
		if floatArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(math.Floor(floatArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}
