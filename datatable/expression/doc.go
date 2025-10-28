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

// Package expression provides expression-based computed columns using expr-lang.
//
// This package enables users to create computed columns using flexible expressions
// that can reference other columns, use functions, and perform complex calculations.
//
// # Features
//
//   - Unified expression system using github.com/expr-lang/expr
//   - Support for simple and complex expressions
//   - Row-by-row evaluation with Arrow array building
//   - Materialization cache for performance
//   - Security sandboxing to prevent unsafe operations
//   - Integration with Phase 2 vectorized functions
//
// # Examples
//
// Simple expression:
//
//	expr, err := Parse("upper(name)")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Complex expression:
//
//	expr, err := Parse("price * quantity * (1 + taxRate)")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Conditional expression:
//
//	expr, err := Parse(`balance > 0 ? "Active" : "Inactive"`)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Performance
//
// Expressions are evaluated row-by-row, which is ~10-25x slower than vectorized
// operations. However, with aggressive materialization (caching), the amortized
// cost becomes negligible for interactive use.
//
// Use Materialize() to cache computed columns after first evaluation:
//
//	// First access: pays evaluation cost (~100ms for 10k rows)
//	val := exprSource.Cell(0, col)
//
//	// Materialize for subsequent fast access
//	exprSource.Materialize("computed_column")
//
//	// Subsequent accesses: instant (<0.01ms)
//	val = exprSource.Cell(1, col)
//
// # Security
//
// Expressions are sandboxed to prevent:
//   - File system access
//   - Network access
//   - Process control
//   - Unsafe operations
//
// Only whitelisted functions are available in the expression environment.
package expression
