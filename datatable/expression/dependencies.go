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
	"strings"
)

// DependencyGraph tracks dependencies between columns to detect circular references.
// This is essential for supporting reference columns (computed columns that depend on other computed columns).
type DependencyGraph struct {
	columns      []ColumnDefinition
	dependencies map[string][]string // column name -> list of columns it depends on
}

// NewDependencyGraph creates a dependency graph from column definitions.
// It automatically builds the dependency map and validates for circular dependencies.
func NewDependencyGraph(columns []ColumnDefinition) (*DependencyGraph, error) {
	graph := &DependencyGraph{
		columns:      columns,
		dependencies: make(map[string][]string),
	}

	// Build dependency map
	for _, col := range columns {
		if col.Expression != nil {
			graph.dependencies[col.Name] = col.Expression.InputColumns()
		} else {
			// Non-computed columns have no dependencies
			graph.dependencies[col.Name] = []string{}
		}
	}

	// Detect circular dependencies
	if err := graph.detectCycles(); err != nil {
		return nil, err
	}

	return graph, nil
}

// detectCycles detects circular dependencies using depth-first search.
// Returns an error if a cycle is found.
func (g *DependencyGraph) detectCycles() error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	path := []string{}

	var hasCycle func(string) bool
	hasCycle = func(col string) bool {
		visited[col] = true
		recStack[col] = true
		path = append(path, col)

		for _, dep := range g.dependencies[col] {
			if !visited[dep] {
				if hasCycle(dep) {
					return true
				}
			} else if recStack[dep] {
				// Found a cycle
				return true
			}
		}

		recStack[col] = false
		path = path[:len(path)-1]
		return false
	}

	for _, col := range g.columns {
		if !visited[col.Name] {
			path = []string{}
			if hasCycle(col.Name) {
				// Build cycle path for error message
				cyclePath := make([]string, 0, len(path))
				foundStart := false
				startCol := path[len(path)-1] // The column that closes the cycle

				for _, p := range path {
					if p == startCol {
						foundStart = true
					}
					if foundStart {
						cyclePath = append(cyclePath, p)
					}
				}
				cyclePath = append(cyclePath, startCol) // Complete the cycle

				return ErrCircularDependency(strings.Join(cyclePath, " -> "))
			}
		}
	}

	return nil
}

// GetDependencies returns the columns that the given column depends on.
func (g *DependencyGraph) GetDependencies(columnName string) []string {
	deps, exists := g.dependencies[columnName]
	if !exists {
		return []string{}
	}
	// Return a copy to prevent modification
	result := make([]string, len(deps))
	copy(result, deps)
	return result
}

// GetDependents returns the columns that depend on the given column.
func (g *DependencyGraph) GetDependents(columnName string) []string {
	dependents := []string{}
	for col, deps := range g.dependencies {
		for _, dep := range deps {
			if dep == columnName {
				dependents = append(dependents, col)
				break
			}
		}
	}
	return dependents
}

// GetEvaluationOrder returns columns in an order that respects dependencies.
// Columns with no dependencies come first, followed by columns that depend on them.
// This is useful for knowing which order to materialize columns.
func (g *DependencyGraph) GetEvaluationOrder() []string {
	// Use topological sort (Kahn's algorithm)

	// Calculate in-degree (number of dependencies) for each column
	inDegree := make(map[string]int)
	for col := range g.dependencies {
		inDegree[col] = len(g.dependencies[col])
	}

	// Start with columns that have no dependencies
	queue := []string{}
	for col, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, col)
		}
	}

	// Process queue
	result := []string{}
	for len(queue) > 0 {
		// Pop from queue
		col := queue[0]
		queue = queue[1:]
		result = append(result, col)

		// For each dependent, reduce its in-degree
		dependents := g.GetDependents(col)
		for _, dependent := range dependents {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	return result
}

// Validate checks if the dependency graph is valid.
func (g *DependencyGraph) Validate() error {
	// Check that all dependencies reference existing columns
	for col, deps := range g.dependencies {
		for _, dep := range deps {
			if _, exists := g.dependencies[dep]; !exists {
				return fmt.Errorf("column %s depends on non-existent column %s", col, dep)
			}
		}
	}

	// Check for cycles (should have been caught in construction, but double-check)
	return g.detectCycles()
}

// HasDependencies returns true if the column has any dependencies.
func (g *DependencyGraph) HasDependencies(columnName string) bool {
	deps := g.GetDependencies(columnName)
	return len(deps) > 0
}

// IsDependentOn returns true if columnA depends on columnB (directly or indirectly).
func (g *DependencyGraph) IsDependentOn(columnA, columnB string) bool {
	visited := make(map[string]bool)

	var checkDependency func(string) bool
	checkDependency = func(col string) bool {
		if visited[col] {
			return false
		}
		visited[col] = true

		deps := g.GetDependencies(col)
		for _, dep := range deps {
			if dep == columnB {
				return true
			}
			if checkDependency(dep) {
				return true
			}
		}
		return false
	}

	return checkDependency(columnA)
}
