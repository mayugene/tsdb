package tsdb

import (
	"reflect"
	"testing"
)

func TestRedisReadToMapReturnsSampleTimestamp(t *testing.T) {
	client := newRedisWriteTestClient(t)
	client.hashes = map[string]map[string]string{
		"sensor:device-1_latest":  {"value": "42", "_ts": "1711808425450"},
		"sensor:epoch_latest":     {"value": "43", "_ts": "0"},
		"sensor:legacy_latest":    {"value": "44"},
		"sensor:malformed_latest": {"value": "45", "_ts": "invalid"},
		"sensor:other_latest":     {"other": "46", "_ts": "1711808425451"},
	}
	wantRows := []map[string]any{
		{"deviceId": "device-1", "value": int64(42), "_ts": int64(1711808425450)},
		{"deviceId": "epoch", "value": int64(43), "_ts": int64(0)},
		{"deviceId": "legacy", "value": int64(44)},
		{"deviceId": "malformed", "value": int64(45)},
	}
	wantPointCodes := [][]string{{"value"}, {"value"}, {"value"}, {"value"}}
	for _, tt := range []struct {
		name   string
		points []PointWithRange
	}{
		{"implicit timestamp", []PointWithRange{{PointCode: "value"}}},
		{"explicit timestamp", []PointWithRange{{PointCode: "value"}, {PointCode: "_ts"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rows, pointCodes, err := NewRedisClient().ReadToMap(t.Context(), ReadDeviceLatestDataInput{
				DeviceModelName: "sensor",
				DeviceIds:       []string{"device-1", "epoch", "legacy", "malformed", "other", "missing"},
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
