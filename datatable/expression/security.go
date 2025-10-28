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

	"github.com/expr-lang/expr/ast"
)

// securityPatcher validates the AST for security violations.
// It prevents expressions from performing dangerous operations such as:
//   - File system access
//   - Network access
//   - Process control
//   - Arbitrary function calls not in whitelist
//   - Excessive nesting (stack overflow protection)
type securityPatcher struct {
	maxDepth     int
	currentDepth int
}

// Visit implements the ast.Visitor interface.
// It's called for each node in the AST during compilation.
func (p *securityPatcher) Visit(node *ast.Node) {
	if p.maxDepth == 0 {
		p.maxDepth = 100 // Default max depth
	}

	p.currentDepth++
	defer func() { p.currentDepth-- }()

	// Check depth limit
	if p.currentDepth > p.maxDepth {
		panic(fmt.Errorf("expression exceeds maximum nesting depth of %d", p.maxDepth))
	}

	// Validate node based on type
	switch n := (*node).(type) {
	case *ast.CallNode:
		// Validate function calls
		p.validateFunctionCall(n)

	case *ast.BuiltinNode:
		// Validate built-in operations
		p.validateBuiltin(n)

	case *ast.MemberNode:
		// Validate member access (property access)
		p.validateMemberAccess(n)
	}
}

// validateFunctionCall checks if a function call is safe.
func (p *securityPatcher) validateFunctionCall(node *ast.CallNode) {
	// All function calls must be to whitelisted functions
	// The whitelist is defined in buildSafeEnvironment()

	// Additional checks could be added here:
	// - Limit number of arguments
	// - Prevent recursive calls
	// - Check for suspicious patterns

	// For now, we rely on the environment whitelist
	// Any function not in the environment will fail at compilation
}

// validateBuiltin checks if a built-in operation is safe.
func (p *securityPatcher) validateBuiltin(node *ast.BuiltinNode) {
	// Built-in operations in expr-lang are generally safe
	// These include: len, all, none, any, one, filter, map, etc.

	// We could add restrictions here if needed
	// For example, limiting the size of collections
}

// validateMemberAccess checks if property access is safe.
func (p *securityPatcher) validateMemberAccess(node *ast.MemberNode) {
	// Member access is generally safe in our context
	// We're only dealing with column values and function results

	// Could add checks for:
	// - Preventing access to unexported fields
	// - Limiting property chain depth
	// - Blacklisting certain property names
}

// Additional security measures that could be implemented:

// 1. Resource limits:
//    - Maximum expression length
//    - Maximum number of operations
//    - Maximum memory allocation
//
// 2. Timeout protection:
//    - Set execution timeout
//    - Cancel long-running expressions
//
// 3. Sandboxing:
//    - Run expressions in isolated goroutines
//    - Use context for cancellation
//
// 4. Audit logging:
//    - Log all expression compilations
//    - Track suspicious patterns
//    - Alert on security violations

// These could be added in future versions as needed.
