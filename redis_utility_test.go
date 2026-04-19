package tsdb

import (
	"testing"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/os/gtime"
)

func TestParseStreamResult(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *RedisDataPoint
		wantNil bool
	}{
		{
			name:    "valid input",
			input:   `["1762828300498-0",["value","20"]]`,
			want:    &RedisDataPoint{Value: 20, Timestamp: gtime.NewFromTimeStamp(1762828300498), IsFilled: false},
			wantNil: false,
		},
		{
			name:    "invalid json",
			input:   `not json`,
			want:    nil,
			wantNil: true,
		},
		{
			name:    "wrong array length",
			input:   `["1762828300498-0"]`,
			want:    nil,
			wantNil: true,
		},
		{
			name:    "wrong value array length",
			input:   `["1762828300498-0",["value"]]`,
			want:    nil,
			wantNil: true,
		},
		{
			name:    "invalid timestamp format",
			input:   `["invalid",["value","20"]]`,
			want:    nil,
			wantNil: true,
		},
		{
			name:    "timestamp only one part",
			input:   `["1762828300498",["value","20"]]`,
			want:    nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseStreamResult(tt.input)
			if tt.wantNil {
				if got != nil {
					t.Errorf("ParseStreamResult(%q) = %v, want nil", tt.input, got)
				}
				return
			}
			if got == nil {
				t.Errorf("ParseStreamResult(%q) = nil, want %v", tt.input, tt.want)
				return
			}
			if got.Value != tt.want.Value {
				t.Errorf("ParseStreamResult(%q).Value = %v, want %v", tt.input, got.Value, tt.want.Value)
			}
			if got.Timestamp.UnixMilli() != tt.want.Timestamp.UnixMilli() {
				t.Errorf("ParseStreamResult(%q).Timestamp = %v, want %v", tt.input, got.Timestamp, tt.want.Timestamp)
			}
			if got.IsFilled != tt.want.IsFilled {
				t.Errorf("ParseStreamResult(%q).IsFilled = %v, want %v", tt.input, got.IsFilled, tt.want.IsFilled)
			}
		})
	}
}

func TestFindValueWithIndex(t *testing.T) {
	tests := []struct {
		name        string
		pointValues []*RedisDataPoint
		startIdx    int
		start       *gtime.Time
		end         *gtime.Time
		wantValue   *int64
		wantIdx     int
	}{
		{
			name:        "empty point values",
			pointValues: []*RedisDataPoint{},
			startIdx:    0,
			start:       gtime.NewFromTimeStamp(0),
			end:         gtime.NewFromTimeStamp(1000),
			wantValue:   nil,
			wantIdx:     0,
		},
		{
			name:        "value before start",
			pointValues: []*RedisDataPoint{{Value: 10, Timestamp: gtime.NewFromTimeStamp(500)}},
			startIdx:    0,
			start:       gtime.NewFromTimeStamp(1000),
			end:         gtime.NewFromTimeStamp(2000),
			wantValue:   nil,
			wantIdx:     1,
		},
		{
			name:        "value in window",
			pointValues: []*RedisDataPoint{{Value: 10, Timestamp: gtime.NewFromTimeStamp(1500)}},
			startIdx:    0,
			start:       gtime.NewFromTimeStamp(1000),
			end:         gtime.NewFromTimeStamp(2000),
			wantValue:   new(int64(10)),
			wantIdx:     1,
		},
		{
			name:        "value at window boundary - start",
			pointValues: []*RedisDataPoint{{Value: 10, Timestamp: gtime.NewFromTimeStamp(1000)}},
			startIdx:    0,
			start:       gtime.NewFromTimeStamp(1000),
			end:         gtime.NewFromTimeStamp(2000),
			wantValue:   new(int64(10)),
			wantIdx:     1,
		},
		{
			name: "value at window boundary - end (should not be included)",
			// Point is at exactly end time (2000), which is >= end, so it returns before processing
			// Returns lastValue (nil since no previous in-window value) and current index i=0
			pointValues: []*RedisDataPoint{{Value: 10, Timestamp: gtime.NewFromTimeStamp(2000)}},
			startIdx:    0,
			start:       gtime.NewFromTimeStamp(1000),
			end:         gtime.NewFromTimeStamp(2000),
			wantValue:   nil,
			wantIdx:     0,
		},
		{
			name: "multiple values - last one before end is returned",
			pointValues: []*RedisDataPoint{
				{Value: 10, Timestamp: gtime.NewFromTimeStamp(1200)},
				{Value: 20, Timestamp: gtime.NewFromTimeStamp(1500)},
				{Value: 30, Timestamp: gtime.NewFromTimeStamp(1800)},
			},
			startIdx:  0,
			start:     gtime.NewFromTimeStamp(1000),
			end:       gtime.NewFromTimeStamp(2000),
			wantValue: new(int64(30)),
			wantIdx:   3,
		},
		{
			name: "start index in middle - continues from there",
			pointValues: []*RedisDataPoint{
				{Value: 10, Timestamp: gtime.NewFromTimeStamp(1200)},
				{Value: 20, Timestamp: gtime.NewFromTimeStamp(1500)},
				{Value: 30, Timestamp: gtime.NewFromTimeStamp(1800)},
			},
			startIdx:  1,
			start:     gtime.NewFromTimeStamp(1300),
			end:       gtime.NewFromTimeStamp(2000),
			wantValue: new(int64(30)),
			wantIdx:   3,
		},
		{
			name: "all values before window",
			// All values are before start, so no value found in window
			// Returns nil and length of array (2)
			pointValues: []*RedisDataPoint{
				{Value: 10, Timestamp: gtime.NewFromTimeStamp(100)},
				{Value: 20, Timestamp: gtime.NewFromTimeStamp(200)},
			},
			startIdx:  0,
			start:     gtime.NewFromTimeStamp(1000),
			end:       gtime.NewFromTimeStamp(2000),
			wantValue: nil,
			wantIdx:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotIdx := findValueWithIndex(tt.pointValues, tt.startIdx, tt.start, tt.end)
			if !ptrInt64Equal(gotValue, tt.wantValue) {
				t.Errorf("findValueWithIndex() value = %v, want %v", ptrStr(gotValue), ptrStr(tt.wantValue))
			}
			if gotIdx != tt.wantIdx {
				t.Errorf("findValueWithIndex() idx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestFindAllNullIndex(t *testing.T) {
	tests := []struct {
		name      string
		dataArray *garray.IntArray
		count     int
		wantLen   int
	}{
		{
			name:      "empty array",
			dataArray: garray.NewIntArray(),
			count:     5,
			wantLen:   0,
		},
		{
			name:      "no matches",
			dataArray: garray.NewIntArrayFrom([]int{1, 2, 3}),
			count:     5,
			wantLen:   0,
		},
		{
			name:      "one match",
			dataArray: garray.NewIntArrayFrom([]int{1, 5, 3}),
			count:     5,
			wantLen:   1,
		},
		{
			name:      "multiple matches",
			dataArray: garray.NewIntArrayFrom([]int{5, 2, 5, 4, 5}),
			count:     5,
			wantLen:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findAllNullIndex(tt.dataArray, tt.count)
			if got.Len() != tt.wantLen {
				t.Errorf("findAllNullIndex() len = %v, want %v", got.Len(), tt.wantLen)
			}
		})
	}
}

func TestRemoveItemByIndex(t *testing.T) {
	tests := []struct {
		name        string
		data        *garray.Array
		idxToRemove *garray.IntArray
		want        []any
	}{
		{
			name:        "nil idx to remove",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: nil,
			want:        []any{1, 2, 3},
		},
		{
			name:        "empty idx to remove",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: garray.NewIntArray(),
			want:        []any{1, 2, 3},
		},
		{
			name:        "remove first",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: garray.NewIntArrayFrom([]int{0}),
			want:        []any{2, 3},
		},
		{
			name:        "remove last",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: garray.NewIntArrayFrom([]int{2}),
			want:        []any{1, 2},
		},
		{
			name:        "remove middle",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: garray.NewIntArrayFrom([]int{1}),
			want:        []any{1, 3},
		},
		{
			name:        "remove multiple",
			data:        garray.NewArrayFrom([]any{1, 2, 3, 4, 5}),
			idxToRemove: garray.NewIntArrayFrom([]int{1, 3}),
			want:        []any{1, 3, 5},
		},
		{
			name:        "remove out of bounds",
			data:        garray.NewArrayFrom([]any{1, 2, 3}),
			idxToRemove: garray.NewIntArrayFrom([]int{10}),
			want:        []any{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeItemByIndex(tt.data, tt.idxToRemove)
			gotSlice := got.Slice()
			if len(gotSlice) != len(tt.want) {
				t.Errorf("removeItemByIndex() len = %v, want %v", len(gotSlice), len(tt.want))
				return
			}
			for i := range gotSlice {
				if gotSlice[i] != tt.want[i] {
					t.Errorf("removeItemByIndex()[%d] = %v, want %v", i, gotSlice[i], tt.want[i])
				}
			}
		})
	}
}

func ptrStr(v *int64) string {
	if v == nil {
		return "nil"
	}
	return string(rune(*v))
}

func ptrInt64Equal(a, b *int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
