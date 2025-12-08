package tsdb

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/util/gconv"
)

func Serialize(metrics []Metric) *bytes.Buffer {
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
