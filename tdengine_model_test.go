package tsdb

import (
	"strings"
	"testing"

	"github.com/gogf/gf/v2/os/gtime"
)

func TestSerialize(t *testing.T) {
	tests := []struct {
		name    string
		metrics []*Metric
		want    string
	}{
		{
			name:    "empty metrics",
			metrics: []*Metric{},
			want:    "",
		},
		{
			name:    "nil tag list",
			metrics: []*Metric{{Name: "m1", TagList: nil, FieldList: []*MetricField{{Key: "f1", Value: 1}}}},
			want:    "m1",
		},
		{
			name:    "empty tag list",
			metrics: []*Metric{{Name: "m1", TagList: []*MetricTag{}, FieldList: []*MetricField{{Key: "f1", Value: 1}}}},
			want:    "m1",
		},
		{
			name:    "nil field list",
			metrics: []*Metric{{Name: "m1", TagList: []*MetricTag{{Key: "t1", Value: "v1"}}, FieldList: nil}},
			want:    "m1",
		},
		{
			name:    "empty field list",
			metrics: []*Metric{{Name: "m1", TagList: []*MetricTag{{Key: "t1", Value: "v1"}}, FieldList: []*MetricField{}}},
			want:    "m1",
		},
		{
			name: "single metric",
			metrics: []*Metric{
				{
					Name:      "m1",
					TagList:   []*MetricTag{{Key: "device", Value: "d1"}, {Key: "project", Value: "p1"}},
					FieldList: []*MetricField{{Key: "temperature", Value: 25.5}},
					Time:      gtime.NewFromTimeStamp(1711808425450),
				},
			},
			want: "m1,device=d1,project=p1 temperature=25.5 1711808425450000000",
		},
		{
			name: "multiple metrics",
			metrics: []*Metric{
				{
					Name:      "m1",
					TagList:   []*MetricTag{{Key: "device", Value: "d1"}},
					FieldList: []*MetricField{{Key: "f1", Value: 1}},
					Time:      gtime.NewFromTimeStamp(1000000000),
				},
				{
					Name:      "m2",
					TagList:   []*MetricTag{{Key: "device", Value: "d2"}},
					FieldList: []*MetricField{{Key: "f2", Value: 2}},
					Time:      gtime.NewFromTimeStamp(2000000000),
				},
			},
			want: "m1,device=d1 f1=1 1000000000000000000\nm2,device=d2 f2=2 2000000000000000000",
		},
		{
			name: "multiple fields",
			metrics: []*Metric{
				{
					Name:      "m1",
					TagList:   []*MetricTag{{Key: "device", Value: "d1"}},
					FieldList: []*MetricField{{Key: "f1", Value: 1}, {Key: "f2", Value: 2}},
					Time:      gtime.NewFromTimeStamp(1000000000),
				},
			},
			want: "m1,device=d1 f1=1,f2=2 1000000000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Serialize(tt.metrics)
			if got.String() != tt.want {
				t.Errorf("Serialize() = %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestWrapWithQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "simple string",
			in:   "abc",
			want: "`abc`",
		},
		{
			name: "string with spaces",
			in:   "a b c",
			want: "`a b c`",
		},
		{
			name: "empty string",
			in:   "",
			want: "``",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WrapWithQuote(tt.in); got != tt.want {
				t.Errorf("WrapWithQuote(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestWrapWithQuoteFromSlice(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		useFunc string
		want    string
	}{
		{
			name:    "empty slice",
			in:      []string{},
			useFunc: "",
			want:    "",
		},
		{
			name:    "single element no func",
			in:      []string{"col1"},
			useFunc: "",
			want:    "`col1`",
		},
		{
			name:    "multiple elements no func",
			in:      []string{"col1", "col2", "col3"},
			useFunc: "",
			want:    "`col1`, `col2`, `col3`",
		},
		{
			name:    "single element with func",
			in:      []string{"col1"},
			useFunc: "last",
			want:    "last(`col1`) as `col1`",
		},
		{
			name:    "multiple elements with func",
			in:      []string{"col1", "col2"},
			useFunc: "last",
			want:    "last(`col1`) as `col1`, last(`col2`) as `col2`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WrapWithQuoteFromSlice(tt.in, tt.useFunc); got != tt.want {
				t.Errorf("WrapWithQuoteFromSlice(%v, %q) = %q, want %q", tt.in, tt.useFunc, got, tt.want)
			}
		})
	}
}

func TestWrapDevicesWithSingleQuote(t *testing.T) {
	tests := []struct {
		name    string
		device  []string
		want    string
	}{
		{
			name:   "empty slice",
			device: []string{},
			want:   "",
		},
		{
			name:   "single device",
			device: []string{"d1"},
			want:   "'d1'",
		},
		{
			name:   "multiple devices",
			device: []string{"d1", "d2", "d3"},
			want:   "'d1', 'd2', 'd3'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WrapDevicesWithSingleQuote(tt.device); got != tt.want {
				t.Errorf("WrapDevicesWithSingleQuote(%v) = %q, want %q", tt.device, got, tt.want)
			}
		})
	}
}

func TestWrapColumnsWithBackQuote(t *testing.T) {
	tests := []struct {
		name          string
		column        []string
		useFunc       string
		withTimestamp bool
		withDevice    bool
		withProject   bool
		want          string
	}{
		{
			name:          "only columns no func",
			column:        []string{"col1", "col2"},
			useFunc:       "",
			withTimestamp: false,
			withDevice:    false,
			withProject:   false,
			want:          "`col1`, `col2`",
		},
		{
			name:          "columns with last func",
			column:        []string{"col1"},
			useFunc:       "last",
			withTimestamp: false,
			withDevice:    false,
			withProject:   false,
			want:          "last(`col1`) as `col1`",
		},
		{
			name:          "with timestamp",
			column:        []string{"col1"},
			useFunc:       "",
			withTimestamp: true,
			withDevice:    false,
			withProject:   false,
			want:          "`_ts`, `col1`",
		},
		{
			name:          "with timestamp and func",
			column:        []string{"col1"},
			useFunc:       "last",
			withTimestamp: true,
			withDevice:    false,
			withProject:   false,
			want:          "last(`_ts`) as `_ts`, last(`col1`) as `col1`",
		},
		{
			name:          "with device",
			column:        []string{"col1"},
			useFunc:       "",
			withTimestamp: false,
			withDevice:    true,
			withProject:   false,
			want:          "`device` as `deviceId`, `col1`",
		},
		{
			name:          "with project",
			column:        []string{"col1"},
			useFunc:       "",
			withTimestamp: false,
			withDevice:    false,
			withProject:   true,
			want:          "`project` as `projectId`, `col1`",
		},
		{
			name:          "all options",
			column:        []string{"col1"},
			useFunc:       "last",
			withTimestamp: true,
			withDevice:    true,
			withProject:   true,
			want:          "last(`_ts`) as `_ts`, `device` as `deviceId`, `project` as `projectId`, last(`col1`) as `col1`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WrapColumnsWithBackQuote(tt.column, tt.useFunc, tt.withTimestamp, tt.withDevice, tt.withProject)
			if got != tt.want {
				t.Errorf("WrapColumnsWithBackQuote() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWrapPointsWithDataType(t *testing.T) {
	tests := []struct {
		caseName string
		in       []TdengineColumn
		want     string
	}{
		{
			caseName: "empty",
			in:       []TdengineColumn{},
			want:     "",
		},
		{
			caseName: "single column double",
			in:       []TdengineColumn{{ColumnName: "temp", DataType: "10"}},
			want:     "`temp` DOUBLE",
		},
		{
			caseName: "multiple columns",
			in: []TdengineColumn{
				{ColumnName: "temp", DataType: "10"},
				{ColumnName: "humidity", DataType: "10"},
			},
			want: "`temp` DOUBLE, `humidity` DOUBLE",
		},
		{
			caseName: "bool type",
			in:       []TdengineColumn{{ColumnName: "online", DataType: "12"}},
			want:     "`online` BOOL",
		},
		{
			caseName: "string type",
			in:       []TdengineColumn{{ColumnName: "name", DataType: "13"}},
			want:     "`name` NCHAR(32)",
		},
		{
			caseName: "all int types map to double",
			in: []TdengineColumn{
				{ColumnName: "i8", DataType: "1"},
				{ColumnName: "u8", DataType: "2"},
				{ColumnName: "i16", DataType: "3"},
				{ColumnName: "u16", DataType: "4"},
				{ColumnName: "i32", DataType: "5"},
				{ColumnName: "u32", DataType: "6"},
				{ColumnName: "i64", DataType: "7"},
				{ColumnName: "u64", DataType: "8"},
			},
			want: "`i8` DOUBLE, `u8` DOUBLE, `i16` DOUBLE, `u16` DOUBLE, `i32` DOUBLE, `u32` DOUBLE, `i64` DOUBLE, `u64` DOUBLE",
		},
		{
			caseName: "float and double",
			in: []TdengineColumn{
				{ColumnName: "f", DataType: "9"},
				{ColumnName: "d", DataType: "10"},
			},
			want: "`f` DOUBLE, `d` DOUBLE",
		},
		{
			caseName: "bit type",
			in:       []TdengineColumn{{ColumnName: "flag", DataType: "11"}},
			want:     "`flag` DOUBLE",
		},
		{
			caseName: "unknown type defaults to double",
			in:       []TdengineColumn{{ColumnName: "unknown", DataType: "999"}},
			want:     "`unknown` DOUBLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			got := WrapPointsWithDataType(tt.in)
			// Remove trailing space for comparison since implementation may have trailing space
			got = strings.TrimRight(got, " ")
			if got != tt.want {
				t.Errorf("WrapPointsWithDataType() = %q, want %q", got, tt.want)
			}
		})
	}
}