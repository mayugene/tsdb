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
	DeviceModelName             string   `v:"required"`
	PointCodes                  []string `v:"required"`
	ProjectId                   string
	DeviceIds                   []string
	HaveProjectIdInResult       bool
	HaveDeviceModelNameInResult bool
}
type ReadDeviceSeriesDataInput struct {
	DeviceIds       []string `v:"required"`
	DeviceModelName string   `v:"required"`
	PointCodes      []string `v:"required"`
	StartTime       int64    `v:"required"`
	EndTime         int64    `v:"required"`
	Interval        string   `v:"required"`
	FillOption      string
}

type MetricTag struct {
	Key   string
	Value string
}

type MetricField struct {
	Key   string
	Value any
}

type Metric struct {
	Name      string
	TagList   []MetricTag
	FieldList []MetricField
	Time      *gtime.Time
}
