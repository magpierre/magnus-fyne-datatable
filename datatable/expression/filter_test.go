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
	"testing"

	"github.com/magpierre/fyne-datatable/datatable"
)

func TestNewExpressionFilter(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{
			name:    "simple comparison",
			expr:    "age > 18",
			wantErr: false,
		},
		{
			name:    "equality",
			expr:    "status == 'active'",
			wantErr: false,
		},
		{
			name:    "complex and",
			expr:    "age >= 18 && status == 'active'",
			wantErr: false,
		},
		{
			name:    "complex or",
			expr:    "balance > 1000 || vip == true",
			wantErr: false,
		},
		{
			name:    "with hasPrefix",
			expr:    "hasPrefix(name, 'John')",
			wantErr: false,
		},
		{
			name:    "null check",
			expr:    "!isNull(email)",
			wantErr: false,
		},
		{
			name:    "empty expression",
			expr:    "",
			wantErr: true,
		},
		{
			name:    "invalid syntax",
			expr:    "age +",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewExpressionFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && filter == nil {
				t.Error("NewExpressionFilter() returned nil filter without error")
			}
		})
	}
}

func TestExpressionFilter_Evaluate_Numeric(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		row         []datatable.Value
		columnNames []string
		want        bool
	}{
		{
			name: "greater than - pass",
			expr: "age > 18",
			row: []datatable.Value{
				{Raw: int64(25), IsNull: false},
			},
			columnNames: []string{"age"},
			want:        true,
		},
		{
			name: "greater than - fail",
			expr: "age > 18",
			row: []datatable.Value{
				{Raw: int64(15), IsNull: false},
			},
			columnNames: []string{"age"},
			want:        false,
		},
		{
			name: "equality - pass",
			expr: "score == 100",
			row: []datatable.Value{
				{Raw: int64(100), IsNull: false},
			},
			columnNames: []string{"score"},
			want:        true,
		},
		{
			name: "equality - fail",
			expr: "score == 100",
			row: []datatable.Value{
				{Raw: int64(99), IsNull: false},
			},
			columnNames: []string{"score"},
			want:        false,
		},
		{
			name: "less than or equal",
			expr: "balance <= 1000",
			row: []datatable.Value{
				{Raw: 1000.0, IsNull: false},
			},
			columnNames: []string{"balance"},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if err != nil {
				t.Fatalf("NewExpressionFilter() error = %v", err)
			}

			got, err := filter.Evaluate(tt.row, tt.columnNames)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionFilter_Evaluate_String(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		row         []datatable.Value
		columnNames []string
		want        bool
	}{
		{
			name: "string equality - pass",
			expr: "status == 'active'",
			row: []datatable.Value{
				{Raw: "active", IsNull: false},
			},
			columnNames: []string{"status"},
			want:        true,
		},
		{
			name: "string equality - fail",
			expr: "status == 'active'",
			row: []datatable.Value{
				{Raw: "inactive", IsNull: false},
			},
			columnNames: []string{"status"},
			want:        false,
		},
		{
			name: "length check",
			expr: "len(name) > 5",
			row: []datatable.Value{
				{Raw: "John Doe", IsNull: false},
			},
			columnNames: []string{"name"},
			want:        true,
		},
		{
			name: "hasPrefix function",
			expr: "hasPrefix(email, 'admin')",
			row: []datatable.Value{
				{Raw: "admin@example.com", IsNull: false},
			},
			columnNames: []string{"email"},
			want:        true,
		},
		{
			name: "upper function in comparison",
			expr: "upper(name) == 'ALICE'",
			row: []datatable.Value{
				{Raw: "alice", IsNull: false},
			},
			columnNames: []string{"name"},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if err != nil {
				t.Fatalf("NewExpressionFilter() error = %v", err)
			}

			got, err := filter.Evaluate(tt.row, tt.columnNames)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionFilter_Evaluate_Boolean(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		row         []datatable.Value
		columnNames []string
		want        bool
	}{
		{
			name: "boolean field - true",
			expr: "active == true",
			row: []datatable.Value{
				{Raw: true, IsNull: false},
			},
			columnNames: []string{"active"},
			want:        true,
		},
		{
			name: "boolean field - false",
			expr: "active == true",
			row: []datatable.Value{
				{Raw: false, IsNull: false},
			},
			columnNames: []string{"active"},
			want:        false,
		},
		{
			name: "direct boolean",
			expr: "verified",
			row: []datatable.Value{
				{Raw: true, IsNull: false},
			},
			columnNames: []string{"verified"},
			want:        true,
		},
		{
			name: "negation",
			expr: "!deleted",
			row: []datatable.Value{
				{Raw: false, IsNull: false},
			},
			columnNames: []string{"deleted"},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if err != nil {
				t.Fatalf("NewExpressionFilter() error = %v", err)
			}

			got, err := filter.Evaluate(tt.row, tt.columnNames)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionFilter_Evaluate_Complex(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		row         []datatable.Value
		columnNames []string
		want        bool
	}{
		{
			name: "AND - both true",
			expr: "age >= 18 && status == 'active'",
			row: []datatable.Value{
				{Raw: int64(25), IsNull: false},
				{Raw: "active", IsNull: false},
			},
			columnNames: []string{"age", "status"},
			want:        true,
		},
		{
			name: "AND - one false",
			expr: "age >= 18 && status == 'active'",
			row: []datatable.Value{
				{Raw: int64(15), IsNull: false},
				{Raw: "active", IsNull: false},
			},
			columnNames: []string{"age", "status"},
			want:        false,
		},
		{
			name: "OR - one true",
			expr: "balance > 1000 || vip == true",
			row: []datatable.Value{
				{Raw: 500.0, IsNull: false},
				{Raw: true, IsNull: false},
			},
			columnNames: []string{"balance", "vip"},
			want:        true,
		},
		{
			name: "OR - both false",
			expr: "balance > 1000 || vip == true",
			row: []datatable.Value{
				{Raw: 500.0, IsNull: false},
				{Raw: false, IsNull: false},
			},
			columnNames: []string{"balance", "vip"},
			want:        false,
		},
		{
			name: "complex with parentheses",
			expr: "(age >= 18 && age <= 65) || retired == true",
			row: []datatable.Value{
				{Raw: int64(70), IsNull: false},
				{Raw: true, IsNull: false},
			},
			columnNames: []string{"age", "retired"},
			want:        true,
		},
		{
			name: "multiple conditions",
			expr: "score >= 60 && score <= 100 && passed == true",
			row: []datatable.Value{
				{Raw: int64(85), IsNull: false},
				{Raw: true, IsNull: false},
			},
			columnNames: []string{"score", "passed"},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if err != nil {
				t.Fatalf("NewExpressionFilter() error = %v", err)
			}

			got, err := filter.Evaluate(tt.row, tt.columnNames)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionFilter_Evaluate_Null(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		row         []datatable.Value
		columnNames []string
		want        bool
	}{
		{
			name: "isNull - true",
			expr: "isNull(email)",
			row: []datatable.Value{
				{Raw: nil, IsNull: true},
			},
			columnNames: []string{"email"},
			want:        true,
		},
		{
			name: "isNull - false",
			expr: "isNull(email)",
			row: []datatable.Value{
				{Raw: "test@example.com", IsNull: false},
			},
			columnNames: []string{"email"},
			want:        false,
		},
		{
			name: "not isNull",
			expr: "!isNull(email)",
			row: []datatable.Value{
				{Raw: "test@example.com", IsNull: false},
			},
			columnNames: []string{"email"},
			want:        true,
		},
		{
			name: "coalesce in filter",
			expr: "coalesce(nickname, name) == 'Bob'",
			row: []datatable.Value{
				{Raw: nil, IsNull: true},
				{Raw: "Bob", IsNull: false},
			},
			columnNames: []string{"nickname", "name"},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(tt.expr)
			if err != nil {
				t.Fatalf("NewExpressionFilter() error = %v", err)
			}

			got, err := filter.Evaluate(tt.row, tt.columnNames)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSimpleFilter(t *testing.T) {
	tests := []struct {
		name     string
		column   string
		operator string
		value    any
		wantExpr string
		wantErr  bool
	}{
		{
			name:     "equals string",
			column:   "status",
			operator: "==",
			value:    "active",
			wantExpr: `status == "active"`,
			wantErr:  false,
		},
		{
			name:     "equals number",
			column:   "age",
			operator: "==",
			value:    18,
			wantExpr: "age == 18",
			wantErr:  false,
		},
		{
			name:     "greater than",
			column:   "score",
			operator: ">",
			value:    50,
			wantExpr: "score > 50",
			wantErr:  false,
		},
		{
			name:     "starts with",
			column:   "name",
			operator: "startsWith",
			value:    "John",
			wantExpr: `hasPrefix(name, "John")`,
			wantErr:  false,
		},
		{
			name:     "unsupported operator",
			column:   "x",
			operator: "unknown",
			value:    1,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewSimpleFilter(tt.column, tt.operator, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSimpleFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if filter.Expression() != tt.wantExpr {
					t.Errorf("Expression() = %v, want %v", filter.Expression(), tt.wantExpr)
				}
			}
		})
	}
}

func TestAndFilter(t *testing.T) {
	filter1, _ := NewExpressionFilter("age >= 18")
	filter2, _ := NewExpressionFilter("status == 'active'")

	combined, err := AndFilter(filter1, filter2)
	if err != nil {
		t.Fatalf("AndFilter() error = %v", err)
	}

	// Test that combined filter works
	row := []datatable.Value{
		{Raw: int64(25), IsNull: false},
		{Raw: "active", IsNull: false},
	}
	columnNames := []string{"age", "status"}

	got, err := combined.Evaluate(row, columnNames)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}

	if !got {
		t.Error("Combined AND filter should pass")
	}

	// Test with one false
	row[1].Raw = "inactive"
	got, _ = combined.Evaluate(row, columnNames)
	if got {
		t.Error("Combined AND filter should fail when one condition is false")
	}
}

func TestOrFilter(t *testing.T) {
	filter1, _ := NewExpressionFilter("balance > 1000")
	filter2, _ := NewExpressionFilter("vip == true")

	combined, err := OrFilter(filter1, filter2)
	if err != nil {
		t.Fatalf("OrFilter() error = %v", err)
	}

	// Test with one true
	row := []datatable.Value{
		{Raw: 500.0, IsNull: false},
		{Raw: true, IsNull: false},
	}
	columnNames := []string{"balance", "vip"}

	got, err := combined.Evaluate(row, columnNames)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}

	if !got {
		t.Error("Combined OR filter should pass when one condition is true")
	}

	// Test with both false
	row[1].Raw = false
	got, _ = combined.Evaluate(row, columnNames)
	if got {
		t.Error("Combined OR filter should fail when both conditions are false")
	}
}

func TestNotFilter(t *testing.T) {
	filter, _ := NewExpressionFilter("active == true")

	negated, err := NotFilter(filter)
	if err != nil {
		t.Fatalf("NotFilter() error = %v", err)
	}

	row := []datatable.Value{
		{Raw: true, IsNull: false},
	}
	columnNames := []string{"active"}

	// Original should pass
	got, _ := filter.Evaluate(row, columnNames)
	if !got {
		t.Error("Original filter should pass")
	}

	// Negated should fail
	got, _ = negated.Evaluate(row, columnNames)
	if got {
		t.Error("Negated filter should fail")
	}

	// Test with false
	row[0].Raw = false
	got, _ = negated.Evaluate(row, columnNames)
	if !got {
		t.Error("Negated filter should pass when original fails")
	}
}
