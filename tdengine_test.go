package tsdb

import "testing"

func TestResolveRealTimeWindow(t *testing.T) {
	tests := []struct {
		name      string
		override  string
		want      string
		wantError bool
	}{
		{name: "global fallback", override: "", want: "1m"},
		{name: "request override", override: "5m", want: "5m"},
		{name: "too large", override: "100h", wantError: true},
		{name: "SQL injection", override: "1m OR 1=1", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveRealTimeWindow("1m", tt.override)
			if tt.wantError {
				if err == nil {
					t.Fatal("expected invalid realTimeWindow to be rejected")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("realTimeWindow = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildTdengineLatestQueryFiltersAggregatedValues(t *testing.T) {
	status := 1.0
	minTemperature := 10.0
	maxTemperature := 30.0
	in := ReadDeviceLatestDataInput{
		DeviceModelName: "sensor",
		ProjectId:       "project-1",
		DeviceIds:       []string{"device-1", "device-2"},
		Points: []PointWithRange{
			{PointCode: "status", EqualValue: &status},
			{PointCode: "temperature", MinValue: &minTemperature, MaxValue: &maxTemperature},
		},
		HaveProjectIdInResult: true,
	}
	want := "SELECT * FROM (SELECT last(`_ts`) as `_ts`, `device` as `deviceId`, `project` as `projectId`, last(`status`) as `status`, last(`temperature`) as `temperature` FROM `sensor` WHERE `project`='project-1' AND `device` IN ('device-1', 'device-2') AND `_ts`>NOW-1m PARTITION BY `device`, `project`) AS latest WHERE `status`=1 AND `temperature`>=10 AND `temperature`<=30"

	if got := buildTdengineLatestQuery(in, "1m"); got != want {
		t.Fatalf("query mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestBuildTdengineLatestQueryWithoutFiltersAvoidsSubquery(t *testing.T) {
	in := ReadDeviceLatestDataInput{
		DeviceModelName: "sensor",
		Points:          []PointWithRange{{PointCode: "temperature"}},
	}
	want := "SELECT last(`_ts`) as `_ts`, `device` as `deviceId`, last(`temperature`) as `temperature` FROM `sensor` WHERE `_ts`>NOW-1m PARTITION BY `device`, `project`"

	if got := buildTdengineLatestQuery(in, "1m"); got != want {
		t.Fatalf("query mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestBuildTdengineLatestQueryUsesRequestRealTimeWindow(t *testing.T) {
	in := ReadDeviceLatestDataInput{
		DeviceModelName: "sensor",
		Points:          []PointWithRange{{PointCode: "temperature"}},
		RealTimeWindow:  "5m",
	}
	want := "SELECT last(`_ts`) as `_ts`, `device` as `deviceId`, last(`temperature`) as `temperature` FROM `sensor` WHERE `_ts`>NOW-5m PARTITION BY `device`, `project`"
	realTimeWindow, err := resolveRealTimeWindow("1m", in.RealTimeWindow)
	if err != nil {
		t.Fatal(err)
	}

	if got := buildTdengineLatestQuery(in, realTimeWindow); got != want {
		t.Fatalf("query mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}
