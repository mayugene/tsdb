package tsdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/gogf/gf/v2/os/gtime"
)

func TestRedisWriteReadToMapKeepsPerPointSampleTimes(t *testing.T) {
	newRedisWriteTestClient(t)
	client := &redis{realTimeWindow: 60}
	const (
		t0 int64 = 1711808425450
		t1       = t0 + 5001
		t2       = t1 + 1000
	)
	write := func(stamp int64, fields ...*MetricField) {
		t.Helper()
		metric := redisWriteTestMetrics()[0]
		metric.FieldList = fields
		metric.Time = gtime.New(time.UnixMilli(stamp))
		if err := client.Write(t.Context(), []*Metric{metric}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(deviceTime, powerTime, soc, socTime int64) {
		t.Helper()
		rows, pointCodes, err := client.ReadToMap(t.Context(), ReadDeviceLatestDataInput{
			DeviceModelName: "energy",
			DeviceIds:       []string{"device-1"},
			Points:          []PointWithRange{{PointCode: "Power"}, {PointCode: "SOC"}, {PointCode: "CommunicationStatus"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []map[string]any{{
			"deviceId": "device-1", "_ts": deviceTime, "Power": int64(1250), "SOC": soc, "CommunicationStatus": int64(1),
			"_pointTs": map[string]int64{"Power": powerTime, "SOC": socTime, "CommunicationStatus": t0},
		}}
		if !reflect.DeepEqual(rows, want) {
			t.Errorf("rows = %#v, want %#v", rows, want)
		}
		if want := [][]string{{"Power", "SOC", "CommunicationStatus"}}; !reflect.DeepEqual(pointCodes, want) {
			t.Errorf("point codes = %v, want %v", pointCodes, want)
		}
	}

	write(t0, &MetricField{Key: "Power", Value: 1250}, &MetricField{Key: "SOC", Value: 828}, &MetricField{Key: "CommunicationStatus", Value: 1})
	check(t0, t0, 828, t0)
	check(t0, t0, 828, t0)

	write(t1, &MetricField{Key: "SOC", Value: 835})
	check(t1, t0, 835, t1)

	write(t2, &MetricField{Key: "Power", Value: 1250})
	check(t2, t2, 835, t1)
}

func TestRedisReadToMapReturnsSampleTimestamp(t *testing.T) {
	client := newRedisWriteTestClient(t)
	client.hashes = map[string]map[string]string{
		"sensor:device-1_latest": {
			"value": "42", "_ts": "1711808425450", "_pointTs:value": "1711808425449",
			"other": "46", "_pointTs:other": "1711808425450", "_pointTs:absent": "1711808425450",
		},
		"sensor:epoch_latest":            {"value": "43", "_ts": "0", "_pointTs:value": "0"},
		"sensor:legacy_latest":           {"value": "44", "_ts": "1711808425450"},
		"sensor:malformed-device_latest": {"value": "45", "_ts": "invalid"},
		"sensor:malformed-point_latest":  {"value": "46", "_ts": "1711808425450", "_pointTs:value": "invalid"},
		"sensor:other_latest":            {"other": "47", "_ts": "1711808425451"},
	}
	wantRows := []map[string]any{
		{"deviceId": "device-1", "value": int64(42), "_ts": int64(1711808425450), "_pointTs": map[string]int64{"value": 1711808425449}},
		{"deviceId": "epoch", "value": int64(43), "_ts": int64(0), "_pointTs": map[string]int64{"value": 0}},
		{"deviceId": "legacy", "value": int64(44), "_ts": int64(1711808425450)},
		{"deviceId": "malformed-device", "value": int64(45)},
		{"deviceId": "malformed-point", "value": int64(46), "_ts": int64(1711808425450)},
	}
	wantPointCodes := [][]string{{"value"}, {"value"}, {"value"}, {"value"}, {"value"}}
	for _, tt := range []struct {
		name   string
		points []PointWithRange
	}{
		{"implicit timestamp", []PointWithRange{{PointCode: "value"}}},
		{"explicit metadata and missing point", []PointWithRange{
			{PointCode: "value"}, {PointCode: "_ts"}, {PointCode: "_pointTs"}, {PointCode: "_pointTs:value"}, {PointCode: "absent"},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rows, pointCodes, err := NewRedisClient().ReadToMap(t.Context(), ReadDeviceLatestDataInput{
				DeviceModelName: "sensor",
				DeviceIds:       []string{"device-1", "epoch", "legacy", "malformed-device", "malformed-point", "other", "missing"},
				Points:          tt.points,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(rows, wantRows) {
				t.Errorf("rows = %#v, want %#v", rows, wantRows)
			}
			if !reflect.DeepEqual(pointCodes, wantPointCodes) {
				t.Errorf("point codes = %v, want %v", pointCodes, wantPointCodes)
			}
		})
	}
}

func TestPointValueMatchesRange(t *testing.T) {
	minValue := 10.0
	maxValue := 20.0
	equalValue := 15.0
	tests := []struct {
		name  string
		value any
		point PointWithRange
		want  bool
	}{
		{name: "no filter accepts value", value: 1, point: PointWithRange{PointCode: "p1"}, want: true},
		{name: "minimum is inclusive", value: 10, point: PointWithRange{MinValue: &minValue}, want: true},
		{name: "below minimum is rejected", value: 9, point: PointWithRange{MinValue: &minValue}, want: false},
		{name: "maximum is inclusive", value: 20, point: PointWithRange{MaxValue: &maxValue}, want: true},
		{name: "above maximum is rejected", value: 21, point: PointWithRange{MaxValue: &maxValue}, want: false},
		{name: "equal value matches", value: 15, point: PointWithRange{EqualValue: &equalValue}, want: true},
		{name: "different value is rejected", value: 16, point: PointWithRange{EqualValue: &equalValue}, want: false},
		{name: "all configured conditions must match", value: 15, point: PointWithRange{MinValue: &minValue, MaxValue: &maxValue, EqualValue: &equalValue}, want: true},
		{name: "missing filtered value is rejected", value: nil, point: PointWithRange{MinValue: &minValue}, want: false},
		{name: "missing unfiltered value is accepted", value: nil, point: PointWithRange{}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pointValueMatchesRange(tt.value, tt.point); got != tt.want {
				t.Fatalf("pointValueMatchesRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
