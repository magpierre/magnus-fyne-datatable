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

// Package main demonstrates ExpressionFilter usage
package main

import (
	"fmt"
	"log"

	"github.com/magpierre/fyne-datatable/datatable"
	"github.com/magpierre/fyne-datatable/datatable/expression"
)

func main() {
	fmt.Println("Expression Filter Examples\n")

	// Example 1: Simple numeric filter
	example1()

	// Example 2: String filter
	example2()

	// Example 3: Complex AND/OR filter
	example3()

	// Example 4: Null handling
	example4()

	// Example 5: Using filter combinators
	example5()
}

func example1() {
	fmt.Println("=== Example 1: Simple Numeric Filter ===")

	// Create filter: age > 18
	filter, err := expression.NewExpressionFilter("age > 18")
	if err != nil {
		log.Fatal(err)
	}

	// Test some rows
	testRows := []struct {
		age  int64
		pass bool
	}{
		{25, true},
		{15, false},
		{18, false},
		{19, true},
	}

	for _, test := range testRows {
		row := []datatable.Value{
			{Raw: test.age, IsNull: false},
		}
		columnNames := []string{"age"}

		result, _ := filter.Evaluate(row, columnNames)
		status := "❌"
		if result {
			status = "✅"
		}

		fmt.Printf("  Age %d: %s (expected: %v)\n", test.age, status, test.pass)
	}
	fmt.Println()
}

func example2() {
	fmt.Println("=== Example 2: String Filter ===")

	// Create filter: status == 'active'
	filter, err := expression.NewExpressionFilter("status == 'active'")
	if err != nil {
		log.Fatal(err)
	}

	// Test some rows
	testRows := []struct {
		status string
		pass   bool
	}{
		{"active", true},
		{"inactive", false},
		{"pending", false},
	}

	for _, test := range testRows {
		row := []datatable.Value{
			{Raw: test.status, IsNull: false},
		}
		columnNames := []string{"status"}

		result, _ := filter.Evaluate(row, columnNames)
		status := "❌"
		if result {
			status = "✅"
		}

		fmt.Printf("  Status '%s': %s\n", test.status, status)
	}
	fmt.Println()
}

func example3() {
	fmt.Println("=== Example 3: Complex AND/OR Filter ===")

	// Create filter: (age >= 18 && age <= 65) || retired == true
	filter, err := expression.NewExpressionFilter("(age >= 18 && age <= 65) || retired == true")
	if err != nil {
		log.Fatal(err)
	}

	// Test some rows
	testRows := []struct {
		age     int64
		retired bool
		pass    bool
		reason  string
	}{
		{30, false, true, "age in range"},
		{70, true, true, "retired"},
		{70, false, false, "too old, not retired"},
		{16, false, false, "too young, not retired"},
	}

	for _, test := range testRows {
		row := []datatable.Value{
			{Raw: test.age, IsNull: false},
			{Raw: test.retired, IsNull: false},
		}
		columnNames := []string{"age", "retired"}

		result, _ := filter.Evaluate(row, columnNames)
		status := "❌"
		if result {
			status = "✅"
		}

		fmt.Printf("  Age %d, Retired %v: %s (%s)\n", test.age, test.retired, status, test.reason)
	}
	fmt.Println()
}

func example4() {
	fmt.Println("=== Example 4: Null Handling ===")

	// Create filter: !isNull(email)
	filter, err := expression.NewExpressionFilter("!isNull(email)")
	if err != nil {
		log.Fatal(err)
	}

	// Test some rows
	testRows := []struct {
		email  any
		isNull bool
	}{
		{"test@example.com", false},
		{nil, true},
		{"user@domain.com", false},
	}

	for _, test := range testRows {
		row := []datatable.Value{
			{Raw: test.email, IsNull: test.isNull},
		}
		columnNames := []string{"email"}

		result, _ := filter.Evaluate(row, columnNames)
		status := "❌"
		if result {
			status = "✅"
		}

		emailStr := "NULL"
		if !test.isNull {
			emailStr = test.email.(string)
		}

		fmt.Printf("  Email '%s': %s\n", emailStr, status)
	}
	fmt.Println()
}

func example5() {
	fmt.Println("=== Example 5: Filter Combinators ===")

	// Create individual filters
	filter1, _ := expression.NewExpressionFilter("age >= 18")
	filter2, _ := expression.NewExpressionFilter("status == 'active'")

	// Combine with AND
	andFilter, err := expression.AndFilter(filter1, filter2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("  Using AND combinator:")
	fmt.Printf("  Filter expression: %s\n", andFilter.Expression())

	// Test row
	row := []datatable.Value{
		{Raw: int64(25), IsNull: false},
		{Raw: "active", IsNull: false},
	}
	columnNames := []string{"age", "status"}

	result, _ := andFilter.Evaluate(row, columnNames)
	fmt.Printf("  Age 25, Status 'active': %v ✅\n", result)

	// Test with inactive status
	row[1].Raw = "inactive"
	result, _ = andFilter.Evaluate(row, columnNames)
	fmt.Printf("  Age 25, Status 'inactive': %v ❌\n", result)

	fmt.Println()
}
