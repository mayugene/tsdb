package tsdb

import "testing"

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
