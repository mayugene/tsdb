package tsdb

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/util/gconv"
)

func Serialize(metrics []*Metric) *bytes.Buffer {
	/*
		influxdb line protocol format:
		measurement,tag_set field_set timestamp
	*/
	var buffer bytes.Buffer
	for idxM, metric := range metrics {
		if idxM > 0 {
			buffer.WriteByte('\n')
		}
		buffer.WriteString(metric.Name)
		// tags and fields of a valid metric should not be empty
		if metric.TagList == nil || len(metric.TagList) == 0 || metric.FieldList == nil || len(metric.FieldList) == 0 {
			continue
		}
		// write tags
		for _, tag := range metric.TagList {
			buffer.WriteByte(',')
			buffer.WriteString(tag.Key)
			buffer.WriteByte('=')
			buffer.WriteString(tag.Value)
		}
		buffer.WriteByte(' ')
		// write fields
		for idxF, field := range metric.FieldList {
			if idxF > 0 {
				buffer.WriteByte(',')
			}
			buffer.WriteString(field.Key)
			buffer.WriteByte('=')
			buffer.WriteString(gconv.String(field.Value))
		}
		buffer.WriteByte(' ')
		buffer.WriteString(gconv.String(metric.Time.UnixNano()))
	}
	return &buffer
}

func WrapWithQuote(in string) (out string) {
	return fmt.Sprintf("`%s`", in)
}

func WrapColumnsWithBackQuote(column []string, useFunc string, withTimestamp bool, withDevice bool, withProject bool) string {
	out := WrapWithQuoteFromSlice(column, useFunc)
	if withProject {
		out = fmt.Sprintf("`%s` as `%s`, ", tdengineColumnProject, tdengineColumnAliasProject) + out
	}
	if withDevice {
		out = fmt.Sprintf("`%s` as `%s`, ", tdengineColumnDevice, tdengineColumnAliasDevice) + out
	}
	if withTimestamp {
		if useFunc != "" {
			out = fmt.Sprintf("%s(`%s`) as `%s`, ", useFunc, tdengineColumnTimestamp, tdengineColumnTimestamp) + out
		} else {
			out = fmt.Sprintf("`%s`, ", tdengineColumnTimestamp) + out
		}
	}
	return out
}

func WrapWithQuoteFromSlice(in []string, useFunc string) (out string) {
	var strBuilder strings.Builder
	for _, v := range in {
		if useFunc != "" {
			strBuilder.WriteString(fmt.Sprintf("%s(`%s`) as `%s`, ", useFunc, v, v))
		} else {
			strBuilder.WriteString(fmt.Sprintf("`%s`, ", v))
		}
	}
	return strings.TrimRight(strBuilder.String(), ", ")
}

func WrapDevicesWithSingleQuote(device []string) (out string) {
	var strBuilder strings.Builder
	for _, v := range device {
		strBuilder.WriteString(fmt.Sprintf("'%s', ", v))
	}
	return strings.TrimRight(strBuilder.String(), ", ")
}

func WrapPointsWithDataType(in []TdengineColumn) (out string) {
	/*
		if we use schemaless line protocol to write data and data types are not wrapped
		tdengine will consider it as double by default
		but for "t, T, true, True, TRUE, f, F, false, False", they will be recognized as bool
		although telegraf can handle suffixes in line protocol, but fields must be previously written in its config file
		it is not suitable for dynamic fields
		so here, we map all int/bit type to double, or tdengine will report an err: [0x3002] Invalid data format
	*/
	dataTypeMap := map[string]string{
		"1":  tdengineDefaultDataType, // INT8
		"2":  tdengineDefaultDataType, // UINT8
		"3":  tdengineDefaultDataType, // INT16
		"4":  tdengineDefaultDataType, // UINT16
		"5":  tdengineDefaultDataType, // INT32
		"6":  tdengineDefaultDataType, // UINT32
		"7":  tdengineDefaultDataType, // INT64
		"8":  tdengineDefaultDataType, // UINT64
		"9":  tdengineDefaultDataType, // FLOAT
		"10": tdengineDefaultDataType, // DOUBLE
		"11": tdengineDefaultDataType, // BIT
		"12": "BOOL",                  // BOOL
		"13": "NCHAR(32)",             // STRING
	}
	var strBuilder strings.Builder
	for _, v := range in {
		dataType, ok := dataTypeMap[v.DataType]
		if !ok {
			dataType = tdengineDefaultDataType
		}
		strBuilder.WriteString(fmt.Sprintf("`%s` %s, ", v.ColumnName, dataType))
	}
	return strings.TrimRight(strBuilder.String(), ", ")
}
