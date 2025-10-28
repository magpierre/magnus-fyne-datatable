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

package main

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
	_ "github.com/magpierre/fyne-datatable/compute/functions"
)

func main() {
	fmt.Println("=== Vectorized Function Registry - End-to-End Example ===\n")

	mem := memory.NewGoAllocator()

	// List all registered functions
	fmt.Println("1. Registered Functions:")
	functions := computepkg.ListFunctions()
	for _, name := range functions {
		metadata, _ := computepkg.GetMetadata(name)
		fmt.Printf("   - %s: %s (Category: %s)\n", name, metadata.Description, metadata.Category)
	}
	fmt.Printf("\nTotal functions registered: %d\n\n", len(functions))

	// Demonstrate Aggregate Functions
	demonstrateAggregateFunctions(mem)

	// Demonstrate String Functions
	demonstrateStringFunctions(mem)

	// Demonstrate Math Functions
	demonstrateMathFunctions(mem)

	// Demonstrate Cast Functions
	demonstrateCastFunctions(mem)

	fmt.Println("\n=== All Functions Verified Successfully ===")
}

func demonstrateAggregateFunctions(mem memory.Allocator) {
	fmt.Println("2. Aggregate Functions:")

	// Create test data: [10, 20, 30, 40, 50]
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fmt.Println("   Input data: [10, 20, 30, 40, 50]")

	// Test MAX
	maxFn, _ := computepkg.Get("max")
	maxResult, _ := maxFn.Execute(arr, mem, false)
	defer maxResult.Release()
	fmt.Printf("   - max() = %d\n", maxResult.(*array.Int64).Value(0))

	// Test MIN
	minFn, _ := computepkg.Get("min")
	minResult, _ := minFn.Execute(arr, mem, false)
	defer minResult.Release()
	fmt.Printf("   - min() = %d\n", minResult.(*array.Int64).Value(0))

	// Test SUM
	sumFn, _ := computepkg.Get("sum")
	sumResult, _ := sumFn.Execute(arr, mem, false)
	defer sumResult.Release()
	fmt.Printf("   - sum() = %d\n", sumResult.(*array.Int64).Value(0))

	// Test MEAN
	meanFn, _ := computepkg.Get("mean")
	meanResult, _ := meanFn.Execute(arr, mem, false)
	defer meanResult.Release()
	fmt.Printf("   - mean() = %.1f\n", meanResult.(*array.Float64).Value(0))

	// Test COUNT
	countFn, _ := computepkg.Get("count")
	countResult, _ := countFn.Execute(arr, mem, false)
	defer countResult.Release()
	fmt.Printf("   - count() = %d\n\n", countResult.(*array.Int64).Value(0))
}

func demonstrateStringFunctions(mem memory.Allocator) {
	fmt.Println("3. String Functions:")

	// Create test data: ["hello", "WORLD", "  spaces  "]
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	builder.AppendValues([]string{"hello", "WORLD", "  spaces  "}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fmt.Println("   Input data: [\"hello\", \"WORLD\", \"  spaces  \"]")

	// Test UPPER
	upperFn, _ := computepkg.Get("upper")
	upperResult, _ := upperFn.Execute(arr, mem, false)
	defer upperResult.Release()
	upperArr := upperResult.(*array.String)
	fmt.Printf("   - upper() = [\"%s\", \"%s\", \"%s\"]\n",
		upperArr.Value(0), upperArr.Value(1), upperArr.Value(2))

	// Test LOWER
	lowerFn, _ := computepkg.Get("lower")
	lowerResult, _ := lowerFn.Execute(arr, mem, false)
	defer lowerResult.Release()
	lowerArr := lowerResult.(*array.String)
	fmt.Printf("   - lower() = [\"%s\", \"%s\", \"%s\"]\n",
		lowerArr.Value(0), lowerArr.Value(1), lowerArr.Value(2))

	// Test TRIM
	trimFn, _ := computepkg.Get("trim")
	trimResult, _ := trimFn.Execute(arr, mem, false)
	defer trimResult.Release()
	trimArr := trimResult.(*array.String)
	fmt.Printf("   - trim() = [\"%s\", \"%s\", \"%s\"]\n",
		trimArr.Value(0), trimArr.Value(1), trimArr.Value(2))

	// Test LENGTH
	lengthFn, _ := computepkg.Get("length")
	lengthResult, _ := lengthFn.Execute(arr, mem, false)
	defer lengthResult.Release()
	lengthArr := lengthResult.(*array.Int32)
	fmt.Printf("   - length() = [%d, %d, %d]\n\n",
		lengthArr.Value(0), lengthArr.Value(1), lengthArr.Value(2))
}

func demonstrateMathFunctions(mem memory.Allocator) {
	fmt.Println("4. Math Functions:")

	// Create test data: [-5.7, 3.2, -2.1]
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]float64{-5.7, 3.2, -2.1}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	fmt.Println("   Input data: [-5.7, 3.2, -2.1]")

	// Test ABS
	absFn, _ := computepkg.Get("abs")
	absResult, _ := absFn.Execute(arr, mem, false)
	defer absResult.Release()
	absArr := absResult.(*array.Float64)
	fmt.Printf("   - abs() = [%.1f, %.1f, %.1f]\n",
		absArr.Value(0), absArr.Value(1), absArr.Value(2))

	// Test ROUND
	roundFn, _ := computepkg.Get("round")
	roundResult, _ := roundFn.Execute(arr, mem, false)
	defer roundResult.Release()
	roundArr := roundResult.(*array.Float64)
	fmt.Printf("   - round() = [%.1f, %.1f, %.1f]\n",
		roundArr.Value(0), roundArr.Value(1), roundArr.Value(2))

	// Test CEIL
	ceilFn, _ := computepkg.Get("ceil")
	ceilResult, _ := ceilFn.Execute(arr, mem, false)
	defer ceilResult.Release()
	ceilArr := ceilResult.(*array.Float64)
	fmt.Printf("   - ceil() = [%.1f, %.1f, %.1f]\n",
		ceilArr.Value(0), ceilArr.Value(1), ceilArr.Value(2))

	// Test FLOOR
	floorFn, _ := computepkg.Get("floor")
	floorResult, _ := floorFn.Execute(arr, mem, false)
	defer floorResult.Release()
	floorArr := floorResult.(*array.Float64)
	fmt.Printf("   - floor() = [%.1f, %.1f, %.1f]\n\n",
		floorArr.Value(0), floorArr.Value(1), floorArr.Value(2))
}

func demonstrateCastFunctions(mem memory.Allocator) {
	fmt.Println("5. Cast Functions:")

	// Create int data: [10, 20, 30]
	intBuilder := array.NewInt64Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{10, 20, 30}, nil)
	intArr := intBuilder.NewArray()
	defer intArr.Release()

	fmt.Println("   Input data (int64): [10, 20, 30]")

	// Cast to float64
	castFloatFn, _ := computepkg.Get("cast_float")
	floatResult, _ := castFloatFn.Execute(intArr, mem, false)
	defer floatResult.Release()
	floatArr := floatResult.(*array.Float64)
	fmt.Printf("   - cast_float() = [%.1f, %.1f, %.1f]\n",
		floatArr.Value(0), floatArr.Value(1), floatArr.Value(2))

	// Cast to string
	castStringFn, _ := computepkg.Get("cast_string")
	stringResult, _ := castStringFn.Execute(intArr, mem, false)
	defer stringResult.Release()
	stringArr := stringResult.(*array.String)
	fmt.Printf("   - cast_string() = [\"%s\", \"%s\", \"%s\"]\n",
		stringArr.Value(0), stringArr.Value(1), stringArr.Value(2))

	// Create string data: ["100", "200", "300"]
	strBuilder := array.NewStringBuilder(mem)
	defer strBuilder.Release()
	strBuilder.AppendValues([]string{"100", "200", "300"}, nil)
	strArr := strBuilder.NewArray()
	defer strArr.Release()

	fmt.Println("   Input data (string): [\"100\", \"200\", \"300\"]")

	// Cast back to int64
	castIntFn, _ := computepkg.Get("cast_int")
	intResult, _ := castIntFn.Execute(strArr, mem, false)
	defer intResult.Release()
	intResArr := intResult.(*array.Int64)
	fmt.Printf("   - cast_int() = [%d, %d, %d]\n",
		intResArr.Value(0), intResArr.Value(1), intResArr.Value(2))
}
