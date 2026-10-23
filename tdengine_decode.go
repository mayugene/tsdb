package tsdb

import (
	"fmt"
	"strconv"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/util/gconv"
)

func decodeTdengineResponse(body string) (*TdengineHttpOutput, error) {
	// Preserve the numeric token before Scan: converting a rounded float64
	// back to an integer cannot recover the original BIGINT value.
	data, err := gjson.DecodeToJson(body, gjson.Options{StrNumber: true})
	if err != nil {
		return nil, err
	}
	out := &TdengineHttpOutput{}
	if err := data.Scan(out); err != nil {
		return nil, err
	}
	for rowIndex, row := range out.Data {
		if len(row) != len(out.ColumnMeta) {
			return nil, fmt.Errorf("TDengine row %d has %d values for %d columns", rowIndex, len(row), len(out.ColumnMeta))
		}
		for columnIndex, value := range row {
			column := out.ColumnMeta[columnIndex]
			converted, err := tdengineColumnValue(value, gconv.String(column[1]))
			if err != nil {
				return nil, fmt.Errorf("TDengine row %d column %q: %w", rowIndex, gconv.String(column[0]), err)
			}
			row[columnIndex] = converted
		}
	}
	return out, nil
}

func tdengineColumnValue(value any, columnType string) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch columnType {
	case "TINYINT":
		return strconv.ParseInt(gconv.String(value), 10, 8)
	case "SMALLINT":
		return strconv.ParseInt(gconv.String(value), 10, 16)
	case "INT":
		return strconv.ParseInt(gconv.String(value), 10, 32)
	case "BIGINT":
		return strconv.ParseInt(gconv.String(value), 10, 64)
	case "TINYINT UNSIGNED":
		return strconv.ParseUint(gconv.String(value), 10, 8)
	case "SMALLINT UNSIGNED":
		return strconv.ParseUint(gconv.String(value), 10, 16)
	case "INT UNSIGNED":
		return strconv.ParseUint(gconv.String(value), 10, 32)
	case "BIGINT UNSIGNED":
		return strconv.ParseUint(gconv.String(value), 10, 64)
	case "FLOAT", "DOUBLE":
		return strconv.ParseFloat(gconv.String(value), 64)
	default:
		// Keep unknown types lossless rather than guessing float64.
		return value, nil
	}
}
