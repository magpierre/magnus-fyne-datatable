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

import "fmt"

// Common errors for the expression package

// ErrInvalidExpression represents an invalid expression error.
type ErrInvalidExpression string

func (e ErrInvalidExpression) Error() string {
	return fmt.Sprintf("invalid expression: %s", string(e))
}

// ErrInvalidColumn represents an invalid column definition error.
type ErrInvalidColumn string

func (e ErrInvalidColumn) Error() string {
	return fmt.Sprintf("invalid column: %s", string(e))
}

// ErrCircularDependency represents a circular dependency error.
type ErrCircularDependency string

func (e ErrCircularDependency) Error() string {
	return fmt.Sprintf("circular dependency: %s", string(e))
}

// ErrColumnNotFound represents a column not found error.
type ErrColumnNotFound string

func (e ErrColumnNotFound) Error() string {
	return fmt.Sprintf("column not found: %s", string(e))
}

// ErrEvaluationFailed represents an evaluation failure error.
type ErrEvaluationFailed string

func (e ErrEvaluationFailed) Error() string {
	return fmt.Sprintf("evaluation failed: %s", string(e))
}
