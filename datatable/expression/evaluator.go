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
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// extractArrowValue extracts a Go value from an Arrow array at the given row index.
// Returns nil for null values.
func extractArrowValue(arr arrow.Array, row int) any {
	if arr.IsNull(row) {
		return nil
	}

	switch a := arr.(type) {
	// Integer types
	case *array.Int8:
		return int64(a.Value(row))
	case *array.Int16:
		return int64(a.Value(row))
	case *array.Int32:
		return int64(a.Value(row))
	case *array.Int64:
		return a.Value(row)

	// Unsigned integer types
	case *array.Uint8:
		return uint64(a.Value(row))
	case *array.Uint16:
		return uint64(a.Value(row))
	case *array.Uint32:
		return uint64(a.Value(row))
	case *array.Uint64:
		return a.Value(row)

	// Float types
	case *array.Float32:
		return float64(a.Value(row))
	case *array.Float64:
		return a.Value(row)

	// String types
	case *array.String:
		return a.Value(row)

	// Boolean type
	case *array.Boolean:
		return a.Value(row)

	// Date types
	case *array.Date32:
		return a.Value(row).ToTime()
	case *array.Date64:
		return a.Value(row).ToTime()

	// Timestamp types
	case *array.Timestamp:
		return a.Value(row).ToTime(a.DataType().(*arrow.TimestampType).Unit)

	default:
		// For unsupported types, return nil
		// This could be enhanced to support more types as needed
		return nil
	}
}

// appendToBuilder appends a value to an Arrow builder based on the target type.
// Handles type conversion and null values.
func appendToBuilder(builder array.Builder, value any, targetType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	// Check if the value is an error message string
	if str, ok := value.(string); ok && strings.HasPrefix(str, "Error: ") {
		// For error messages, we need to handle them based on the target type
		return appendErrorToBuilder(builder, fmt.Errorf(strings.TrimPrefix(str, "Error: ")), targetType)
	}

	switch targetType.ID() {
	case arrow.INT8:
		b := builder.(*array.Int8Builder)
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.Append(int8(v))

	case arrow.INT16:
		b := builder.(*array.Int16Builder)
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.Append(int16(v))

	case arrow.INT32:
		b := builder.(*array.Int32Builder)
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.Append(int32(v))

	case arrow.INT64:
		b := builder.(*array.Int64Builder)
		v, err := toInt64(value)
		if err != nil {
			return err
		}
		b.Append(v)

	case arrow.UINT8:
		b := builder.(*array.Uint8Builder)
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.Append(uint8(v))

	case arrow.UINT16:
		b := builder.(*array.Uint16Builder)
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.Append(uint16(v))

	case arrow.UINT32:
		b := builder.(*array.Uint32Builder)
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.Append(uint32(v))

	case arrow.UINT64:
		b := builder.(*array.Uint64Builder)
		v, err := toUint64(value)
		if err != nil {
			return err
		}
		b.Append(v)

	case arrow.FLOAT32:
		b := builder.(*array.Float32Builder)
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		b.Append(float32(v))

	case arrow.FLOAT64:
		b := builder.(*array.Float64Builder)
		v, err := toFloat64(value)
		if err != nil {
			return err
		}
		b.Append(v)

	case arrow.STRING:
		b := builder.(*array.StringBuilder)
		v := toString(value)
		// Check if this is an error message
		if strings.HasPrefix(v, "Error: ") {
			b.Append(v) // Append the error message as-is
		} else {
			b.Append(v)
		}

	case arrow.BOOL:
		b := builder.(*array.BooleanBuilder)
		v, err := toBool(value)
		if err != nil {
			return err
		}
		b.Append(v)

	default:
		return fmt.Errorf("unsupported output type: %v", targetType)
	}

	return nil
}

// appendErrorToBuilder appends an error value to a builder.
// This creates a special error value that can be displayed in the UI.
func appendErrorToBuilder(builder array.Builder, err error, targetType arrow.DataType) error {
	switch targetType.ID() {
	case arrow.STRING:
		b := builder.(*array.StringBuilder)
		b.Append(fmt.Sprintf("Error: %s", err.Error()))
	case arrow.INT64:
		b := builder.(*array.Int64Builder)
		b.Append(-1) // Use -1 as error marker for int64
	case arrow.FLOAT64:
		b := builder.(*array.Float64Builder)
		b.Append(-1.0) // Use -1.0 as error marker for float64
	case arrow.BOOL:
		b := builder.(*array.BooleanBuilder)
		b.AppendNull() // Use null for boolean error values
	case arrow.DATE64:
		b := builder.(*array.Date64Builder)
		b.Append(arrow.Date64(-1)) // Use -1 as error marker for date64
	case arrow.TIMESTAMP:
		b := builder.(*array.TimestampBuilder)
		b.Append(arrow.Timestamp(-1)) // Use -1 as error marker for timestamp
	default:
		// For unknown types, try to append as string
		if stringBuilder, ok := builder.(*array.StringBuilder); ok {
			stringBuilder.Append(fmt.Sprintf("Error: %s", err.Error()))
		} else {
			return fmt.Errorf("cannot append error to builder of type %v", targetType)
		}
	}
	return nil
}

// arrowToReflectKind converts an Arrow data type to a reflect.Kind for expr compilation.
func arrowToReflectKind(dt arrow.DataType) reflect.Kind {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return reflect.Int64
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return reflect.Uint64
	case arrow.FLOAT32, arrow.FLOAT64:
		return reflect.Float64
	case arrow.STRING:
		return reflect.String
	case arrow.BOOL:
		return reflect.Bool
	default:
		return reflect.Interface
	}
}

// Type conversion helper functions

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

func toUint64(value any) (uint64, error) {
	switch v := value.(type) {
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

func toString(value any) string {
	return fmt.Sprintf("%v", value)
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return v != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}
