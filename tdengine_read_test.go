package tsdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

func newTdengineReadTestClient(body string) *tdengine {
	c := newTdengineWriteTestClient(func(*http.Request) (*http.Response, error) {
		return tdengineWriteTestResponse(http.StatusOK, body), nil
	})
	c.uri = "http://tsdb.test/sql/test"
	c.uriNoDb = "http://tsdb.test/sql"
	c.realTimeWindow = "1m"
	return c
}

func TestTdengineReadPreservesBigInt(t *testing.T) {
	for _, tt := range []struct {
		name, columnType, value string
		want                    any
	}{
		{"above float precision", "BIGINT", "9007199254740993", int64(9007199254740993)},
		{"large integer", "BIGINT", "1700000000000000001", int64(1700000000000000001)},
		{"signed max", "BIGINT", "9223372036854775807", int64(9223372036854775807)},
		{"signed min", "BIGINT", "-9223372036854775808", int64(-9223372036854775808)},
		{"unsigned max", "BIGINT UNSIGNED", "18446744073709551615", uint64(18446744073709551615)},
		{"quoted integer", "BIGINT", `"9007199254740993"`, int64(9007199254740993)},
		{"null", "BIGINT", "null", nil},
		{"int", "INT", "42", int64(42)},
		{"tinyint", "TINYINT", "-128", int64(-128)},
		{"smallint", "SMALLINT", "32767", int64(32767)},
		{"unsigned tinyint", "TINYINT UNSIGNED", "255", uint64(255)},
		{"unsigned smallint", "SMALLINT UNSIGNED", "65535", uint64(65535)},
		{"unsigned int", "INT UNSIGNED", "42", uint64(42)},
		{"double", "DOUBLE", "1.25e2", float64(125)},
		{"float", "FLOAT", "1.25", float64(1.25)},
		{"bool", "BOOL", "true", true},
		{"string", "VARCHAR", `"9007199254740993"`, "9007199254740993"},
		{"unknown numeric type", "FUTURE_TYPE", "9007199254740993", json.Number("9007199254740993")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"code":0,"column_meta":[["_ts","TIMESTAMP",8],["value",%q,8]],"data":[["2024-03-30T14:20:25.450Z",%s]],"rows":1}`, tt.columnType, tt.value)
			client := newTdengineReadTestClient(body)
			maps, _, err := client.ReadToMap(t.Context(), ReadDeviceLatestDataInput{DeviceModelName: "sensor", Points: []PointWithRange{{PointCode: "value"}}})
			if err != nil {
				t.Fatal(err)
			}
			if got := maps[0]["value"]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadToMap = %T(%v), want %T(%v)", got, got, tt.want, tt.want)
			}
			series, timestamps, err := client.ReadToSeries(t.Context(), ReadDeviceSeriesDataInput{DeviceModelName: "sensor", DeviceIds: []string{"d1"}, PointCodes: []string{"value"}, Interval: "1s"})
			if err != nil {
				t.Fatal(err)
			}
			if got := series[0][0]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadToSeries = %T(%v), want %T(%v)", got, got, tt.want, tt.want)
			}
			if timestamps[0] != 1711808425450 {
				t.Errorf("timestamp = %d", timestamps[0])
			}
			out, err := client.operateDb(t.Context(), "SELECT value")
			if err != nil {
				t.Fatal(err)
			}
			if got := out.Data[0][1]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("operateDb = %T(%v), want %T(%v)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestTdengineReadRejectsInvalidNumbers(t *testing.T) {
	for _, tt := range []struct{ columnType, value string }{
		{"BIGINT", "9223372036854775808"},
		{"BIGINT UNSIGNED", "18446744073709551616"},
		{"BIGINT UNSIGNED", "-1"},
		{"BIGINT", "1.5"},
		{"BIGINT", `"invalid"`},
		{"BIGINT", "true"},
		{"TINYINT", "128"},
		{"SMALLINT", "32768"},
		{"INT", "2147483648"},
		{"TINYINT UNSIGNED", "256"},
		{"SMALLINT UNSIGNED", "65536"},
		{"INT UNSIGNED", "4294967296"},
		{"DOUBLE", "1e400"},
	} {
		t.Run(tt.columnType+"/"+tt.value, func(t *testing.T) {
			client := newTdengineReadTestClient(fmt.Sprintf(`{"code":0,"column_meta":[["value",%q,8]],"data":[[%s]],"rows":1}`, tt.columnType, tt.value))
			if _, err := client.post(t.Context(), "SELECT value"); err == nil {
				t.Error("post accepted invalid number")
			}
			if _, err := client.operateDb(t.Context(), "SELECT value"); err == nil {
				t.Error("operateDb accepted invalid number")
			}
		})
	}
}

func TestTdengineDecodeEmptyAndMalformedResponses(t *testing.T) {
	for _, body := range []string{
		`{"code":0,"column_meta":[["value","BIGINT",8]],"data":[],"rows":0}`,
		`{"code":9728,"desc":"syntax error"}`,
	} {
		if _, err := newTdengineReadTestClient(body).post(t.Context(), "SELECT value"); err != nil {
			t.Fatalf("valid response %s: %v", body, err)
		}
	}
	for _, body := range []string{
		`not json`,
		`{"code":0,"column_meta":[["value","BIGINT",8]],"data":[[]],"rows":1}`,
		`{"code":0,"column_meta":[],"data":[[1]],"rows":1}`,
	} {
		if _, err := newTdengineReadTestClient(body).post(t.Context(), "SELECT value"); err == nil {
			t.Fatalf("accepted malformed response %s", body)
		}
	}
}
