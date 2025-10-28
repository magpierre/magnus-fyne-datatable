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

package expression

import (
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/magpierre/fyne-datatable/compute"
	"github.com/magpierre/fyne-datatable/compute/functions"
)

// buildSafeEnvironment creates an environment with registered functions.
// This environment is used for expression compilation and includes:
//   - Standard math functions
//   - String manipulation functions
//   - Type conversion functions
//   - Helper utility functions
//   - Compute registry functions (vectorized functions as scalar wrappers)
//
// Phase 2 vectorized functions are registered here as scalar functions
// for use in expr-lang expressions. While these lose their vectorization
// benefit in the expr-lang context (called row-by-row), they provide
// consistent function names and behavior.
func buildSafeEnvironment() map[string]any {
	env := make(map[string]any)

	// Math functions (from Go's math package)
	env["abs"] = math.Abs
	env["ceil"] = math.Ceil
	env["floor"] = math.Floor
	env["round"] = math.Round
	env["max"] = math.Max
	env["min"] = math.Min
	env["pow"] = math.Pow
	env["sqrt"] = math.Sqrt
	env["exp"] = math.Exp
	env["log"] = math.Log
	env["log10"] = math.Log10
	env["sin"] = math.Sin
	env["cos"] = math.Cos
	env["tan"] = math.Tan
	env["asin"] = math.Asin
	env["acos"] = math.Acos
	env["atan"] = math.Atan

	// String functions (from Go's strings package)
	env["upper"] = strings.ToUpper
	env["lower"] = strings.ToLower
	env["trim"] = strings.TrimSpace
	env["trimLeft"] = strings.TrimLeft
	env["trimRight"] = strings.TrimRight
	env["contains"] = strings.Contains
	env["hasPrefix"] = strings.HasPrefix
	env["hasSuffix"] = strings.HasSuffix
	env["replace"] = strings.Replace
	env["replaceAll"] = strings.ReplaceAll
	env["split"] = strings.Split
	env["join"] = strings.Join
	env["repeat"] = strings.Repeat

	// String length (custom wrapper)
	env["len"] = func(s string) int {
		return len(s)
	}

	// Substring function
	env["substr"] = func(s string, start, length int) string {
		if start < 0 || start >= len(s) {
			return ""
		}
		end := start + length
		if end > len(s) {
			end = len(s)
		}
		return s[start:end]
	}

	// Type conversions
	env["int"] = exprToInt64
	env["float"] = exprToFloat64
	env["string"] = exprToString
	env["bool"] = exprToBool

	// Null handling functions
	env["coalesce"] = coalesce
	env["ifNull"] = ifNull
	env["isNull"] = func(v any) bool {
		return v == nil
	}

	// Conditional helpers
	env["if"] = func(condition bool, trueVal, falseVal any) any {
		if condition {
			return trueVal
		}
		return falseVal
	}

	// Add compute registry functions as scalar wrappers
	addComputeRegistryFunctions(env)

	return env
}

// buildTypeEnvironment creates type hints for expression compilation.
// This includes the safe environment plus column name placeholders.
func buildTypeEnvironment(inputColumns []string) map[string]any {
	env := buildSafeEnvironment()

	// Add column name placeholders for type checking
	// We use float64(0) as a placeholder to hint at numeric types
	// expr-lang will handle actual type coercion at runtime
	for _, colName := range inputColumns {
		// Don't add anything - let expr.AllowUndefinedVariables() handle it
		// This allows expr-lang to be flexible with types at runtime
		_ = colName
	}

	return env
}

// Helper functions for type conversions in expressions

func exprToInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		// Try to parse as int
		var i int64
		_, err := fmt.Sscanf(val, "%d", &i)
		if err == nil {
			return i
		}
	}
	return 0
}

func exprToFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		// Try to parse as float
		var f float64
		_, err := fmt.Sscanf(val, "%f", &f)
		if err == nil {
			return f
		}
	}
	return 0.0
}

func exprToString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func exprToBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case int, int8, int16, int32, int64:
		return val != 0
	case uint, uint8, uint16, uint32, uint64:
		return val != 0
	case float32, float64:
		return val != 0.0
	case string:
		return val != ""
	}
	return false
}

// coalesce returns the first non-nil value from the arguments.
// Similar to SQL COALESCE function.
func coalesce(values ...any) any {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

// ifNull returns defaultValue if value is nil, otherwise returns value.
func ifNull(value, defaultValue any) any {
	if value == nil {
		return defaultValue
	}
	return value
}

// addComputeRegistryFunctions adds all registered compute functions to the environment as scalar wrappers.
// This makes vectorized functions available in expressions, though they lose their vectorization benefit
// when called row-by-row through expr-lang.
func addComputeRegistryFunctions(env map[string]any) {
	// Get all registered functions from the compute registry
	allFunctions := compute.ListFunctions()

	for _, funcName := range allFunctions {
		fn, err := compute.Get(funcName)
		if err != nil {
			// Skip functions that can't be retrieved
			continue
		}

		// Create scalar wrapper for each function
		scalarWrapper := createScalarWrapper(fn)
		if scalarWrapper != nil {
			env[funcName] = scalarWrapper
		}
	}
}

// createScalarWrapper creates a scalar wrapper for a vectorized function.
// This allows vectorized functions to be used in expr-lang expressions.
func createScalarWrapper(fn compute.VectorFunction) any {
	// Get function metadata to understand its behavior
	metadata, err := compute.GetMetadata(fn.Name())
	if err != nil {
		return nil
	}

	// Create appropriate scalar wrapper based on function category
	switch metadata.Category {
	case compute.CategoryMath:
		return createMathScalarWrapper(fn)
	case compute.CategoryString:
		return createStringScalarWrapper(fn)
	case compute.CategoryCast:
		return createCastScalarWrapper(fn)
	case compute.CategoryAggregate:
		// Aggregate functions don't make sense as scalar wrappers
		// They're designed to operate on entire arrays
		return nil
	default:
		// For unknown categories, try to create a generic wrapper
		return createGenericScalarWrapper(fn)
	}
}

// createMathScalarWrapper creates a scalar wrapper for math functions.
func createMathScalarWrapper(fn compute.VectorFunction) any {
	switch fn.Name() {
	case "abs":
		return func(x float64) any {
			return executeScalarMath(fn, x)
		}
	case "ceil":
		return func(x float64) any {
			return executeScalarMath(fn, x)
		}
	case "floor":
		return func(x float64) any {
			return executeScalarMath(fn, x)
		}
	case "round":
		return func(x float64) any {
			return executeScalarMath(fn, x)
		}
	default:
		return nil
	}
}

// createStringScalarWrapper creates a scalar wrapper for string functions.
func createStringScalarWrapper(fn compute.VectorFunction) any {
	switch fn.Name() {
	case "upper":
		return func(s string) any {
			return executeScalarString(fn, s)
		}
	case "lower":
		return func(s string) any {
			return executeScalarString(fn, s)
		}
	case "trim":
		return func(s string) any {
			return executeScalarString(fn, s)
		}
	case "length":
		return func(s string) any {
			return executeScalarStringLength(fn, s)
		}
	case "substring":
		return func(s string, start, stop int) any {
			return executeScalarSubstring(fn, s, start, stop)
		}
	default:
		return nil
	}
}

// createCastScalarWrapper creates a scalar wrapper for cast functions.
func createCastScalarWrapper(fn compute.VectorFunction) any {
	switch fn.Name() {
	case "cast_string":
		return func(x any) string {
			result := executeScalarCast(fn, x, arrow.BinaryTypes.String)
			if str, ok := result.(string); ok {
				return str
			}
			return ""
		}
	case "cast_float":
		return func(x any) float64 {
			result := executeScalarCast(fn, x, arrow.PrimitiveTypes.Float64)
			if f, ok := result.(float64); ok {
				return f
			}
			return 0.0
		}
	case "cast_int":
		return func(x any) int64 {
			result := executeScalarCast(fn, x, arrow.PrimitiveTypes.Int64)
			if i, ok := result.(int64); ok {
				return i
			}
			return 0
		}
	case "cast_bool":
		return func(x any) bool {
			result := executeScalarCast(fn, x, arrow.FixedWidthTypes.Boolean)
			if b, ok := result.(bool); ok {
				return b
			}
			return false
		}
	case "cast_date":
		return func(x any) int64 {
			result := executeScalarCast(fn, x, arrow.FixedWidthTypes.Date64)
			if i, ok := result.(int64); ok {
				return i
			}
			return 0
		}
	case "cast_timestamp":
		return func(x any) int64 {
			result := executeScalarCast(fn, x, arrow.FixedWidthTypes.Timestamp_us)
			if i, ok := result.(int64); ok {
				return i
			}
			return 0
		}
	default:
		return nil
	}
}

// createGenericScalarWrapper creates a generic scalar wrapper for unknown function types.
func createGenericScalarWrapper(fn compute.VectorFunction) any {
	// For now, return nil - we can extend this later if needed
	return nil
}

// executeScalarMath executes a math function on a single scalar value.
func executeScalarMath(fn compute.VectorFunction, x float64) any {
	// Create a single-element Arrow array
	mem := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.Append(x)
	arr := builder.NewArray()
	defer arr.Release()

	// Execute the function
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		return fmt.Sprintf("Error: %s", err.Error())
	}
	defer result.Release()

	// Extract the result
	if result.Len() > 0 && !result.IsNull(0) {
		if floatArr, ok := result.(*array.Float64); ok {
			return floatArr.Value(0)
		}
	}
	return "Error: failed to extract math result"
}

// executeScalarString executes a string function on a single scalar value.
func executeScalarString(fn compute.VectorFunction, s string) any {
	// Create a single-element Arrow array
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.Append(s)
	arr := builder.NewArray()
	defer arr.Release()

	// Execute the function
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		return fmt.Sprintf("Error: %s", err.Error())
	}
	defer result.Release()

	// Extract the result
	if result.Len() > 0 && !result.IsNull(0) {
		if stringArr, ok := result.(*array.String); ok {
			return stringArr.Value(0)
		}
	}
	return "Error: failed to extract string result"
}

// executeScalarStringLength executes a length function on a single scalar value.
func executeScalarStringLength(fn compute.VectorFunction, s string) any {
	// Create a single-element Arrow array
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.Append(s)
	arr := builder.NewArray()
	defer arr.Release()

	// Execute the function
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		return fmt.Sprintf("Error: %s", err.Error())
	}
	defer result.Release()

	// Extract the result
	if result.Len() > 0 && !result.IsNull(0) {
		if intArr, ok := result.(*array.Int64); ok {
			return int(intArr.Value(0))
		}
	}
	return "Error: failed to extract length result"
}

// executeScalarSubstring executes a substring function on a single scalar value with parameters.
func executeScalarSubstring(fn compute.VectorFunction, s string, start, stop int) any {
	// Create a single-element Arrow array
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.Append(s)
	arr := builder.NewArray()
	defer arr.Release()

	// Set parameters on the function if it's a SubstringFunction
	if substringFn, ok := fn.(*functions.SubstringFunction); ok {
		substringFn.SetParameters(start, stop)
	}

	// Execute the function
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		return fmt.Sprintf("Error: %s", err.Error())
	}
	defer result.Release()

	// Extract the result
	if result.Len() > 0 && !result.IsNull(0) {
		if stringArr, ok := result.(*array.String); ok {
			return stringArr.Value(0)
		}
	}
	return "Error: failed to extract substring result"
}

// executeScalarCast executes a cast function on a single scalar value.
func executeScalarCast(fn compute.VectorFunction, x any, targetType arrow.DataType) any {
	// Create a single-element Arrow array from the input
	mem := memory.NewGoAllocator()
	var arr arrow.Array

	switch v := x.(type) {
	case string:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.Append(v)
		arr = builder.NewArray()
	case float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.Append(v)
		arr = builder.NewArray()
	case int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.Append(v)
		arr = builder.NewArray()
	case bool:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		builder.Append(v)
		arr = builder.NewArray()
	default:
		return fmt.Sprintf("Error: cannot convert %T to %s", x, targetType)
	}
	defer arr.Release()

	// Execute the function
	result, err := fn.Execute(arr, mem, false)
	if err != nil {
		// Return error message instead of nil
		return fmt.Sprintf("Error: %s", err.Error())
	}
	defer result.Release()

	// Extract the result based on target type
	if result.Len() > 0 && !result.IsNull(0) {
		switch targetType.ID() {
		case arrow.STRING:
			if stringArr, ok := result.(*array.String); ok {
				return stringArr.Value(0)
			}
		case arrow.FLOAT64:
			if floatArr, ok := result.(*array.Float64); ok {
				return floatArr.Value(0)
			}
		case arrow.INT64:
			if intArr, ok := result.(*array.Int64); ok {
				return intArr.Value(0)
			}
		case arrow.BOOL:
			if boolArr, ok := result.(*array.Boolean); ok {
				return boolArr.Value(0)
			}
		case arrow.DATE64:
			if dateArr, ok := result.(*array.Date64); ok {
				return dateArr.Value(0)
			}
		case arrow.TIMESTAMP:
			if timestampArr, ok := result.(*array.Timestamp); ok {
				return timestampArr.Value(0)
			}
		}
	}

	// If we get here, something went wrong
	return fmt.Sprintf("Error: failed to extract result for type %s", targetType)
}
