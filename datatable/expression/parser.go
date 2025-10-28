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
	"regexp"
	"strings"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
)

// Parse creates an expression from a string with automatic type inference.
//
// This is a convenience function that:
//   - Extracts column references from the expression
//   - Infers the output type based on the expression
//   - Creates and compiles the expression
//
// For more control, use ParseWithContext or NewExpression directly.
//
// Example:
//
//	expr, err := Parse("upper(name)")
//	if err != nil {
//	    log.Fatal(err)
//	}
func Parse(exprStr string) (*Expression, error) {
	exprStr = strings.TrimSpace(exprStr)

	if exprStr == "" {
		return nil, fmt.Errorf("expression cannot be empty")
	}

	// Extract column references automatically
	inputColumns := extractColumnReferences(exprStr)

	if len(inputColumns) == 0 {
		// Expression might be a constant (e.g., "42", "'hello'")
		// Allow it, but it won't reference any columns
	}

	// Infer output type from expression
	outputType := inferOutputType(exprStr)

	return NewExpression(exprStr, inputColumns, outputType)
}

// ParseWithContext creates an expression with explicit context information.
//
// This allows you to specify:
//   - Which columns are available for reference
//   - The expected output type
//
// This is more explicit than Parse and provides better error messages.
//
// Example:
//
//	availableColumns := []string{"name", "age", "salary"}
//	expr, err := ParseWithContext(
//	    "upper(name)",
//	    availableColumns,
//	    arrow.BinaryTypes.String,
//	)
func ParseWithContext(
	exprStr string,
	availableColumns []string,
	outputType arrow.DataType,
) (*Expression, error) {
	exprStr = strings.TrimSpace(exprStr)

	if exprStr == "" {
		return nil, fmt.Errorf("expression cannot be empty")
	}

	// Extract referenced columns
	inputColumns := extractColumnReferences(exprStr, availableColumns)

	// Validate that all referenced columns are available
	for _, col := range inputColumns {
		if !contains(availableColumns, col) {
			return nil, fmt.Errorf("expression references unknown column: %s", col)
		}
	}

	return NewExpression(exprStr, inputColumns, outputType)
}

// extractColumnReferences extracts column names from an expression string.
//
// If availableColumns is provided, only returns columns that are in that list.
// Otherwise, extracts all identifier-like tokens.
//
// This is a simple heuristic-based extraction and may not be perfect,
// but works well for most expressions.
func extractColumnReferences(exprStr string, availableColumns ...[]string) []string {
	var available []string
	if len(availableColumns) > 0 && availableColumns[0] != nil {
		available = availableColumns[0]
	}

	// Tokenize the expression
	tokens := tokenize(exprStr)

	// Collect identifiers
	var referenced []string
	seenColumns := make(map[string]bool)

	for _, token := range tokens {
		if isIdentifier(token) {
			// Skip function names (they're followed by '(')
			// This is a simple heuristic
			if isFunctionName(token) {
				continue
			}

			// If we have available columns, check if this token is one
			if len(available) > 0 {
				if contains(available, token) && !seenColumns[token] {
					referenced = append(referenced, token)
					seenColumns[token] = true
				}
			} else {
				// No available list, include all identifiers
				if !seenColumns[token] {
					referenced = append(referenced, token)
					seenColumns[token] = true
				}
			}
		}
	}

	return referenced
}

// inferOutputType attempts to infer the output type from an expression.
//
// This uses simple heuristics:
//   - Arithmetic operations → Float64
//   - Comparison operations → Bool
//   - String functions → String
//   - Default → Float64
//
// For precise control, use ParseWithContext with an explicit type.
func inferOutputType(exprStr string) arrow.DataType {
	// Check for comparison operators
	if containsAny(exprStr, []string{"==", "!=", "<", ">", "<=", ">=", "?", ":"}) {
		// Could be conditional or comparison
		// Conditionals can return any type, so default to Float64
		// For boolean comparisons without ternary, return Bool
		if strings.Contains(exprStr, "?") {
			// Ternary operator - type depends on values
			return arrow.PrimitiveTypes.Float64
		}
		// Simple comparison
		return arrow.FixedWidthTypes.Boolean
	}

	// Check for string functions
	if containsAny(exprStr, []string{"upper", "lower", "trim", "substr", "concat", "replace"}) {
		return arrow.BinaryTypes.String
	}

	// Check for arithmetic operators
	if containsAny(exprStr, []string{"+", "-", "*", "/", "%"}) {
		return arrow.PrimitiveTypes.Float64
	}

	// Check for math functions
	if containsAny(exprStr, []string{"abs", "ceil", "floor", "round", "sqrt", "pow", "exp", "log"}) {
		return arrow.PrimitiveTypes.Float64
	}

	// Default to Float64 (most flexible numeric type)
	return arrow.PrimitiveTypes.Float64
}

// Utility functions

// tokenize splits an expression into tokens.
// This is a simple whitespace and operator-based tokenizer.
func tokenize(exprStr string) []string {
	// Use regex to split on operators and whitespace while keeping them
	re := regexp.MustCompile(`[\w]+|[+\-*/().,<>=!&|?:]`)
	return re.FindAllString(exprStr, -1)
}

// isIdentifier checks if a token is a valid identifier (variable/column name).
func isIdentifier(token string) bool {
	if token == "" {
		return false
	}

	// Must start with letter or underscore
	if !unicode.IsLetter(rune(token[0])) && token[0] != '_' {
		return false
	}

	// Rest must be letter, digit, or underscore
	for _, ch := range token[1:] {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			return false
		}
	}

	return true
}

// isFunctionName checks if a token is a known function name.
func isFunctionName(token string) bool {
	// List of known function names
	functions := []string{
		// Math
		"abs", "ceil", "floor", "round", "max", "min", "pow", "sqrt",
		"exp", "log", "log10", "sin", "cos", "tan", "asin", "acos", "atan",
		// String
		"upper", "lower", "trim", "trimLeft", "trimRight", "len", "substr",
		"contains", "hasPrefix", "hasSuffix", "replace", "replaceAll",
		"split", "join", "repeat",
		// Type conversion
		"int", "float", "string", "bool",
		// Null handling
		"coalesce", "ifNull", "isNull", "if",
	}

	for _, fn := range functions {
		if token == fn {
			return true
		}
	}

	return false
}

// contains checks if a slice contains a string.
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// containsAny checks if a string contains any of the substrings.
func containsAny(str string, substrings []string) bool {
	for _, sub := range substrings {
		if strings.Contains(str, sub) {
			return true
		}
	}
	return false
}
