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

package functions

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	computepkg "github.com/magpierre/fyne-datatable/compute"
)

// CastFunction converts array to different data type.
type CastFunction struct {
	computepkg.BaseVectorFunction
	targetType arrow.DataType
}

// NewCastFunction creates a new cast function for a specific target type.
func NewCastFunction(targetType arrow.DataType, name string) *CastFunction {
	return &CastFunction{
		BaseVectorFunction: computepkg.NewBaseVectorFunction(
			name,
			fmt.Sprintf("Cast to %s", targetType.String()),
			computepkg.CategoryCast,
			[]arrow.DataType{}, // Accept any type
		),
		targetType: targetType,
	}
}

// OutputType returns the target type.
func (f *CastFunction) OutputType(inputType arrow.DataType) (arrow.DataType, error) {
	return f.targetType, nil
}

// Execute performs the type cast.
func (f *CastFunction) Execute(input arrow.Array, mem memory.Allocator, inPlace bool) (arrow.Array, error) {
	return performCast(input, f.targetType, mem)
}

// Validate checks if cast is possible.
func (f *CastFunction) Validate(inputType arrow.DataType) error {
	// Arrow's cast function will validate at execution time
	// We could add more strict validation here if needed
	return nil
}

// Register common cast functions
func init() {
	// Cast to int64
	computepkg.MustRegister(NewCastFunction(
		arrow.PrimitiveTypes.Int64,
		"cast_int",
	))

	// Cast to float64
	computepkg.MustRegister(NewCastFunction(
		arrow.PrimitiveTypes.Float64,
		"cast_float",
	))

	// Cast to string
	computepkg.MustRegister(NewCastFunction(
		arrow.BinaryTypes.String,
		"cast_string",
	))

	// Cast to boolean
	computepkg.MustRegister(NewCastFunction(
		arrow.FixedWidthTypes.Boolean,
		"cast_bool",
	))

	// Cast to date
	computepkg.MustRegister(NewCastFunction(
		arrow.FixedWidthTypes.Date64,
		"cast_date",
	))

	// Cast to timestamp
	computepkg.MustRegister(NewCastFunction(
		arrow.FixedWidthTypes.Timestamp_us,
		"cast_timestamp",
	))
}

// performCast performs the actual type conversion
func performCast(input arrow.Array, targetType arrow.DataType, mem memory.Allocator) (arrow.Array, error) {
	targetID := targetType.ID()

	// Cast to Int64
	if targetID == arrow.INT64 {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.Float64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(int64(arr.Value(i)))
				}
			}
		case *array.String:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					val, err := strconv.ParseInt(arr.Value(i), 10, 64)
					if err != nil {
						return nil, fmt.Errorf("cannot cast %q to int64", arr.Value(i))
					}
					builder.Append(val)
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to int64 not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	// Cast to Float64
	if targetID == arrow.FLOAT64 {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.Int64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(float64(arr.Value(i)))
				}
			}
		case *array.String:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					val, err := strconv.ParseFloat(arr.Value(i), 64)
					if err != nil {
						return nil, fmt.Errorf("cannot cast %q to float64", arr.Value(i))
					}
					builder.Append(val)
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to float64 not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	// Cast to String
	if targetID == arrow.STRING {
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.Int64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(strconv.FormatInt(arr.Value(i), 10))
				}
			}
		case *array.Float64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(strconv.FormatFloat(arr.Value(i), 'f', -1, 64))
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to string not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	// Cast to Boolean
	if targetID == arrow.BOOL {
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.Int64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					builder.Append(arr.Value(i) != 0)
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to bool not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	// Cast to Date64
	if targetID == arrow.DATE64 {
		builder := array.NewDate64Builder(mem)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.String:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Try to parse common date formats
					dateStr := arr.Value(i)
					date, err := parseDate(dateStr)
					if err != nil {
						return nil, fmt.Errorf("cannot cast %q to date: %v", dateStr, err)
					}
					builder.Append(arrow.Date64(date.UnixMilli()))
				}
			}
		case *array.Int64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Treat int64 as Unix timestamp in milliseconds
					builder.Append(arrow.Date64(arr.Value(i)))
				}
			}
		case *array.Float64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Treat float64 as Unix timestamp in milliseconds
					builder.Append(arrow.Date64(int64(arr.Value(i))))
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to date not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	// Cast to Timestamp
	if targetID == arrow.TIMESTAMP {
		timestampType := targetType.(*arrow.TimestampType)
		builder := array.NewTimestampBuilder(mem, timestampType)
		defer builder.Release()

		switch arr := input.(type) {
		case *array.String:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Try to parse common timestamp formats
					timeStr := arr.Value(i)
					timestamp, err := parseTimestamp(timeStr)
					if err != nil {
						return nil, fmt.Errorf("cannot cast %q to timestamp: %v", timeStr, err)
					}
					builder.Append(arrow.Timestamp(timestamp.UnixMicro()))
				}
			}
		case *array.Int64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Treat int64 as Unix timestamp in microseconds
					builder.Append(arrow.Timestamp(arr.Value(i)))
				}
			}
		case *array.Float64:
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					builder.AppendNull()
				} else {
					// Treat float64 as Unix timestamp in microseconds
					builder.Append(arrow.Timestamp(int64(arr.Value(i))))
				}
			}
		default:
			return nil, fmt.Errorf("cast from %v to timestamp not implemented", input.DataType())
		}
		return builder.NewArray(), nil
	}

	return nil, fmt.Errorf("cast to %v not implemented", targetType)
}

// parseDate attempts to parse a string into a date using common formats
func parseDate(dateStr string) (time.Time, error) {
	// Common date formats to try
	formats := []string{
		"2006-01-02",           // ISO date
		"2006/01/02",           // Alternative separator
		"01/02/2006",           // US format
		"02/01/2006",           // European format
		"2006-01-02T15:04:05Z", // ISO datetime with timezone
		"2006-01-02T15:04:05",  // ISO datetime without timezone
		"2006-01-02 15:04:05",  // Space separator
		"Jan 2, 2006",          // Text format
		"January 2, 2006",      // Full month name
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

// parseTimestamp attempts to parse a string into a timestamp using common formats
func parseTimestamp(timeStr string) (time.Time, error) {
	// Common timestamp formats to try
	formats := []string{
		"2006-01-02T15:04:05.000000Z", // ISO with microseconds
		"2006-01-02T15:04:05.000Z",    // ISO with milliseconds
		"2006-01-02T15:04:05Z",        // ISO with seconds
		"2006-01-02T15:04:05.000000",  // ISO without timezone
		"2006-01-02T15:04:05.000",     // ISO with milliseconds, no timezone
		"2006-01-02T15:04:05",         // ISO without timezone
		"2006-01-02 15:04:05.000000",  // Space separator with microseconds
		"2006-01-02 15:04:05.000",     // Space separator with milliseconds
		"2006-01-02 15:04:05",         // Space separator
		"2006/01/02 15:04:05",         // Alternative separators
		"01/02/2006 15:04:05",         // US format
		"02/01/2006 15:04:05",         // European format
		"Jan 2, 2006 15:04:05",        // Text format
		"January 2, 2006 15:04:05",    // Full month name
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	// Try parsing as Unix timestamp (seconds or milliseconds)
	if timestamp, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		// If it looks like milliseconds (13+ digits), convert to seconds
		if timestamp > 1e12 {
			return time.Unix(timestamp/1000, (timestamp%1000)*1e6), nil
		}
		// Otherwise treat as seconds
		return time.Unix(timestamp, 0), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", timeStr)
}
