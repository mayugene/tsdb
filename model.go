package tsdb

import (
	"github.com/gogf/gf/v2/os/gtime"
)

type Config struct {
	Host           string
	Port           int
	Username       string
	Password       string
	Database       string
	DataKeep       string
	RealTimeWindow string
}

type ReadDeviceLatestDataInput struct {
	DeviceIds       []string
	DeviceModelName string
	PointCodes      []string
}
type ReadDeviceSeriesDataInput struct {
	DeviceIds       []string
	DeviceModelName string
	PointCodes      []string
	StartTime       int64
	EndTime         int64
	Interval        string
	FillOption      string
}

type MetricTag struct {
	Key   string
	Value string
}

type MetricField struct {
	Key   string
	Value interface{}
}

type Metric struct {
	Name      string
	TagList   []MetricTag
	FieldList []MetricField
	Time      *gtime.Time
}
