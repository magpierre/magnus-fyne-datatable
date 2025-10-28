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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

// UpperFunction converts string array to uppercase.
type UpperFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewUpperFunction())
}

// NewUpperFunction creates a new upper function.
func NewUpperFunction() *UpperFunction {
	return &UpperFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"upper",
			"Convert strings to uppercase",
			computepkg.CategoryString,
			computepkg.StringTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *UpperFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute converts all strings to uppercase.
func (f *UpperFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	strArr := input.(*array.String)
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(strings.ToUpper(strArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// LowerFunction converts string array to lowercase.
type LowerFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewLowerFunction())
}

// NewLowerFunction creates a new lower function.
func NewLowerFunction() *LowerFunction {
	return &LowerFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"lower",
			"Convert strings to lowercase",
			computepkg.CategoryString,
			computepkg.StringTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *LowerFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute converts all strings to lowercase.
func (f *LowerFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	strArr := input.(*array.String)
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(strings.ToLower(strArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// TrimFunction removes leading and trailing whitespace.
type TrimFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewTrimFunction())
}

// NewTrimFunction creates a new trim function.
func NewTrimFunction() *TrimFunction {
	return &TrimFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"trim",
			"Remove leading and trailing whitespace",
			computepkg.CategoryString,
			computepkg.StringTypes(),
		),
	}
}

// OutputType returns the same type as input.
func (f *TrimFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute trims whitespace from all strings.
func (f *TrimFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	strArr := input.(*array.String)
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(strings.TrimSpace(strArr.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// LengthFunction computes string length.
type LengthFunction struct {
	computepkg.BaseVectorFunction
}

func init() {
	computepkg.MustRegister(NewLengthFunction())
}

// NewLengthFunction creates a new length function.
func NewLengthFunction() *LengthFunction {
	return &LengthFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"length",
			"Compute string length in characters",
			computepkg.CategoryString,
			computepkg.StringTypes(),
		),
	}
}

// OutputType returns int32 for string length.
func (f *LengthFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return arrow.PrimitiveTypes.Int32, nil
}

// Execute computes length of each string.
func (f *LengthFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	strArr := input.(*array.String)
	builder := array.NewInt32Builder(mem)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(int32(len(strArr.Value(i))))
		}
	}

	return builder.NewArray(), nil
}

// SubstringFunction extracts a substring from each string.
type SubstringFunction struct {
	computepkg.BaseVectorFunction
	start int
	stop  int
}

func init() {
	computepkg.MustRegister(NewSubstringFunction())
}

// NewSubstringFunction creates a new substring function.
func NewSubstringFunction() *SubstringFunction {
	return &SubstringFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			"substring",
			"Extract substring from strings",
			computepkg.CategoryString,
			computepkg.StringTypes(),
		),
	}
}

// SetParameters sets the start and stop indices for substring extraction.
func (f *SubstringFunction) SetParameters(start, stop int) {
	f.start = start
	f.stop = stop
}

// OutputType returns the same type as input.
func (f *SubstringFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	if err := f.Validate(inputType); err != nil {
		return nil, err
	}
	return inputType, nil
}

// Execute extracts substring from each string.
func (f *SubstringFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	if err := f.Validate(input.DataType()); err != nil {
		return nil, err
	}

	strArr := input.(*array.String)
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			str := strArr.Value(i)
			// Handle negative indices and bounds checking
			start := f.start
			stop := f.stop

			// Handle negative indices (count from end)
			if start < 0 {
				start = len(str) + start
			}
			if stop < 0 {
				stop = len(str) + stop
			}

			// Bounds checking
			if start < 0 {
				start = 0
			}
			if stop > len(str) {
				stop = len(str)
			}
			if start >= stop {
				builder.Append("")
			} else {
				builder.Append(str[start:stop])
			}
		}
	}

	return builder.NewArray(), nil
}
