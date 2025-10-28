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
	"github.com/magpierre/fyne-datatable/datatable"
)

// ColumnDefinition defines a column in an ExpressionDataSource.
// A column can be either:
//   - A pass-through column (references a source column by index)
//   - A computed column (has an expression)
//   - A transformed column (both source and expression)
type ColumnDefinition struct {
	// Name is the column name
	Name string

	// Type is the data type of the column
	Type datatable.DataType

	// SourceColumn is the index of the source column (nil for pure computed columns)
	// If set, this column can optionally apply an expression to transform the source data
	SourceColumn *int

	// Expression is the computation for this column (nil for pure pass-through columns)
	Expression *Expression

	// Materialized indicates if the column values are cached
	Materialized bool

	// Description provides human-readable documentation for the column
	Description string

	// Metadata stores additional column information
	Metadata map[string]any
}

// IsPassThrough returns true if this is a pass-through column (no expression).
func (cd *ColumnDefinition) IsPassThrough() bool {
	return cd.Expression == nil && cd.SourceColumn != nil
}

// IsComputed returns true if this is a computed column (has expression).
func (cd *ColumnDefinition) IsComputed() bool {
	return cd.Expression != nil
}

// IsPure returns true if this is a pure computed column (no source reference).
func (cd *ColumnDefinition) IsPure() bool {
	return cd.Expression != nil && cd.SourceColumn == nil
}

// IsTransformed returns true if this transforms a source column with an expression.
func (cd *ColumnDefinition) IsTransformed() bool {
	return cd.Expression != nil && cd.SourceColumn != nil
}

// InputColumns returns the names of columns this definition depends on.
func (cd *ColumnDefinition) InputColumns() []string {
	if cd.Expression == nil {
		return []string{}
	}
	return cd.Expression.InputColumns()
}

// Validate checks if the column definition is valid.
func (cd *ColumnDefinition) Validate() error {
	if cd.Name == "" {
		return ErrInvalidColumn("column name cannot be empty")
	}

	// Must have either a source column or an expression (or both)
	if cd.SourceColumn == nil && cd.Expression == nil {
		return ErrInvalidColumn("column must have either a source column or an expression")
	}

	// Validate expression if present
	if cd.Expression != nil {
		if err := cd.Expression.Validate(); err != nil {
			return ErrInvalidExpression(err.Error())
		}
	}

	return nil
}
