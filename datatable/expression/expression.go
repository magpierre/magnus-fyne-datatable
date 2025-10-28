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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// Expression represents a compiled expression that can be evaluated on Arrow data.
// Expressions use the expr-lang library for parsing and evaluation.
type Expression struct {
	source       string         // Original expression string
	program      *vm.Program    // Compiled expr-lang program
	inputColumns []string       // Column dependencies
	outputType   arrow.DataType // Result type
}

// NewExpression creates and compiles an expression.
//
// Parameters:
//   - source: the expression string (e.g., "upper(name)", "price * quantity")
//   - inputColumns: list of column names referenced in the expression
//   - outputType: the expected Arrow data type of the result
//
// Returns an error if:
//   - The expression is empty
//   - The expression fails to compile
//   - The expression has security violations
func NewExpression(
	source string,
	inputColumns []string,
	outputType arrow.DataType,
) (*Expression, error) {
	if source == "" {
		return nil, fmt.Errorf("expression cannot be empty")
	}

	if outputType == nil {
		return nil, fmt.Errorf("output type cannot be nil")
	}

	// Build type environment for compilation
	env := buildTypeEnvironment(inputColumns)

	// Compile with security options
	// We don't use expr.AsKind() because we want flexible type coercion at runtime
	program, err := expr.Compile(source,
		expr.Env(env),
		expr.AllowUndefinedVariables(),
		expr.Patch(&securityPatcher{}),
	)
	if err != nil {
		return nil, fmt.Errorf("compilation failed: %w", err)
	}

	return &Expression{
		source:       source,
		program:      program,
		inputColumns: inputColumns,
		outputType:   outputType,
	}, nil
}

// Evaluate executes the expression on Arrow columns and returns the result as an Arrow array.
//
// This performs row-by-row evaluation:
//   - For each row, extract values from input arrays
//   - Execute the compiled program with those values
//   - Append the result to an output builder
//   - Return the built Arrow array
//
// Parameters:
//   - inputs: slice of Arrow arrays, one for each input column
//   - mem: memory allocator for creating the output array
//
// Returns:
//   - A new Arrow array containing the evaluation results
//   - An error if evaluation fails
//
// Note: The returned array must be released by the caller.
func (e *Expression) Evaluate(inputs []arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if len(inputs) != len(e.inputColumns) {
		return nil, fmt.Errorf("expected %d inputs, got %d", len(e.inputColumns), len(inputs))
	}

	if len(inputs) == 0 {
		return nil, fmt.Errorf("no input columns")
	}

	rowCount := inputs[0].Len()

	// Validate all inputs have the same length
	for i, arr := range inputs {
		if arr.Len() != rowCount {
			return nil, fmt.Errorf("input %d has different length: expected %d, got %d", i, rowCount, arr.Len())
		}
	}

	// Create builder for output
	builder := array.NewBuilder(mem, e.outputType)
	defer builder.Release()

	// Evaluate row-by-row
	for row := 0; row < rowCount; row++ {
		// Build environment for this row
		// Start with the safe function environment
		env := buildSafeEnvironment()

		// Add column values for this row
		for i, colName := range e.inputColumns {
			val := extractArrowValue(inputs[i], row)
			env[colName] = val
		}

		// Execute expression
		result, err := vm.Run(e.program, env)
		if err != nil {
			// Instead of failing the entire evaluation, append an error value
			// This allows partial results to be returned
			if err := appendErrorToBuilder(builder, err, e.outputType); err != nil {
				return nil, fmt.Errorf("failed to append error at row %d: %w", row, err)
			}
			continue
		}

		// Append to builder
		if err := appendToBuilder(builder, result, e.outputType); err != nil {
			// Instead of failing, append an error value
			if err := appendErrorToBuilder(builder, err, e.outputType); err != nil {
				return nil, fmt.Errorf("failed to append error at row %d: %w", row, err)
			}
		}
	}

	return builder.NewArray(), nil
}

// Source returns the original expression string.
func (e *Expression) Source() string {
	return e.source
}

// InputColumns returns the list of column names this expression depends on.
func (e *Expression) InputColumns() []string {
	return e.inputColumns
}

// OutputType returns the Arrow type of the evaluation results.
func (e *Expression) OutputType() arrow.DataType {
	return e.outputType
}

// Validate checks if the expression is valid and ready to evaluate.
func (e *Expression) Validate() error {
	if e.program == nil {
		return fmt.Errorf("expression not compiled")
	}
	if len(e.inputColumns) == 0 {
		return fmt.Errorf("no input columns specified")
	}
	if e.outputType == nil {
		return fmt.Errorf("output type not specified")
	}
	return nil
}

// String returns a string representation of the expression.
func (e *Expression) String() string {
	return fmt.Sprintf("Expression{source: %q, inputs: %v, outputType: %v}",
		e.source, e.inputColumns, e.outputType)
}
