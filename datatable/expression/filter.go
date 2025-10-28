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

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/magpierre/fyne-datatable/datatable"
)

// ExpressionFilter implements the datatable.Filter interface using expr-lang.
// It allows complex boolean expressions for filtering rows.
//
// Examples:
//   - Simple comparison: "age > 18"
//   - Multiple conditions: "age >= 18 && status == 'active'"
//   - Complex logic: "(balance > 1000 || vip == true) && active"
//   - String operations: "contains(name, 'John')"
//   - Null checks: "!isNull(email)"
type ExpressionFilter struct {
	expression string      // Original expression string
	program    *vm.Program // Compiled expr-lang program
}

// NewExpressionFilter creates a filter from a boolean expression.
// The expression must evaluate to a boolean value.
//
// Parameters:
//   - exprStr: the filter expression (e.g., "age > 18 && active")
//
// Returns:
//   - A compiled filter ready to use
//   - An error if the expression is invalid or doesn't return boolean
//
// Example:
//
//	filter, err := NewExpressionFilter("age >= 18 && status == 'active'")
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewExpressionFilter(exprStr string) (*ExpressionFilter, error) {
	if exprStr == "" {
		return nil, fmt.Errorf("filter expression cannot be empty")
	}

	// Build safe environment with functions
	env := buildSafeEnvironment()

	// Compile expression - must return boolean
	program, err := expr.Compile(exprStr,
		expr.Env(env),
		expr.AsBool(), // Enforce boolean return type
		expr.AllowUndefinedVariables(),
		expr.Patch(&securityPatcher{}),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}

	return &ExpressionFilter{
		expression: exprStr,
		program:    program,
	}, nil
}

// Evaluate implements the datatable.Filter interface.
// It evaluates the filter expression for a single row.
//
// Parameters:
//   - row: the row values to evaluate
//   - columnNames: the names of the columns (for mapping values to names)
//
// Returns:
//   - true if the row passes the filter (should be visible)
//   - false if the row should be filtered out
//   - An error if evaluation fails
func (f *ExpressionFilter) Evaluate(row []datatable.Value, columnNames []string) (bool, error) {
	// Build environment from row values
	env := buildRowEnvironment(row, columnNames)

	// Execute expression
	result, err := vm.Run(f.program, env)
	if err != nil {
		return false, fmt.Errorf("filter evaluation failed: %w", err)
	}

	// Type assert to bool (should always succeed due to AsBool compilation option)
	boolResult, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("filter must return boolean, got %T", result)
	}

	return boolResult, nil
}

// Description returns a human-readable description of the filter.
// Implements an optional method for better debugging and UI display.
func (f *ExpressionFilter) Description() string {
	return fmt.Sprintf("Expression: %s", f.expression)
}

// Expression returns the original filter expression string.
func (f *ExpressionFilter) Expression() string {
	return f.expression
}

// Validate checks if the filter is properly configured.
func (f *ExpressionFilter) Validate() error {
	if f.program == nil {
		return fmt.Errorf("filter not compiled")
	}
	if f.expression == "" {
		return fmt.Errorf("filter expression is empty")
	}
	return nil
}

// buildRowEnvironment creates an environment map from row values and column names.
// This makes column values available as variables in the expression.
//
// Example:
//
//	If row = [25, "Alice", true] and columnNames = ["age", "name", "active"]
//	Then env = {"age": 25, "name": "Alice", "active": true}
func buildRowEnvironment(row []datatable.Value, columnNames []string) map[string]any {
	// Start with safe function environment
	env := buildSafeEnvironment()

	// Add column values as variables
	for i, name := range columnNames {
		if i < len(row) {
			if row[i].IsNull {
				env[name] = nil
			} else {
				env[name] = row[i].Raw
			}
		}
	}

	return env
}

// Common filter constructors for convenience

// NewSimpleFilter creates a filter for a simple comparison.
// This is a helper for the most common filter case.
//
// Example:
//
//	filter := NewSimpleFilter("age", ">", 18)
func NewSimpleFilter(column, operator string, value any) (*ExpressionFilter, error) {
	var exprStr string

	switch operator {
	case "==", "=":
		// String values need quotes
		if s, ok := value.(string); ok {
			exprStr = fmt.Sprintf("%s == %q", column, s)
		} else {
			exprStr = fmt.Sprintf("%s == %v", column, value)
		}
	case "!=", "<>":
		if s, ok := value.(string); ok {
			exprStr = fmt.Sprintf("%s != %q", column, s)
		} else {
			exprStr = fmt.Sprintf("%s != %v", column, value)
		}
	case ">", "<", ">=", "<=":
		exprStr = fmt.Sprintf("%s %s %v", column, operator, value)
	case "contains":
		exprStr = fmt.Sprintf("contains(%s, %q)", column, value)
	case "startsWith":
		exprStr = fmt.Sprintf("hasPrefix(%s, %q)", column, value)
	case "endsWith":
		exprStr = fmt.Sprintf("hasSuffix(%s, %q)", column, value)
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	return NewExpressionFilter(exprStr)
}

// AndFilter combines multiple filters with AND logic.
// All filters must pass for the row to be visible.
func AndFilter(filters ...*ExpressionFilter) (*ExpressionFilter, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("at least one filter required")
	}

	if len(filters) == 1 {
		return filters[0], nil
	}

	// Build combined expression
	expressions := make([]string, len(filters))
	for i, f := range filters {
		expressions[i] = fmt.Sprintf("(%s)", f.expression)
	}

	combinedExpr := expressions[0]
	for i := 1; i < len(expressions); i++ {
		combinedExpr = fmt.Sprintf("%s && %s", combinedExpr, expressions[i])
	}

	return NewExpressionFilter(combinedExpr)
}

// OrFilter combines multiple filters with OR logic.
// At least one filter must pass for the row to be visible.
func OrFilter(filters ...*ExpressionFilter) (*ExpressionFilter, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("at least one filter required")
	}

	if len(filters) == 1 {
		return filters[0], nil
	}

	// Build combined expression
	expressions := make([]string, len(filters))
	for i, f := range filters {
		expressions[i] = fmt.Sprintf("(%s)", f.expression)
	}

	combinedExpr := expressions[0]
	for i := 1; i < len(expressions); i++ {
		combinedExpr = fmt.Sprintf("%s || %s", combinedExpr, expressions[i])
	}

	return NewExpressionFilter(combinedExpr)
}

// NotFilter negates a filter.
func NotFilter(filter *ExpressionFilter) (*ExpressionFilter, error) {
	if filter == nil {
		return nil, fmt.Errorf("filter cannot be nil")
	}

	negatedExpr := fmt.Sprintf("!(%s)", filter.expression)
	return NewExpressionFilter(negatedExpr)
}
