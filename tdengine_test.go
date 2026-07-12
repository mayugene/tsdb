package tsdb

import "testing"

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
