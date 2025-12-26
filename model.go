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
	TagList   []*MetricTag
	FieldList []*MetricField
	Time      *gtime.Time
}

func (s *Metric) AddTag(key, value string) {
	// make tag keys in ascending order
	for i, tag := range s.TagList {
		if key > tag.Key {
			continue
		}

		if key == tag.Key {
			tag.Value = value
			return
		}

		s.TagList = append(s.TagList, nil)
		copy(s.TagList[i+1:], s.TagList[i:])
		s.TagList[i] = &MetricTag{Key: key, Value: value}
		return
	}

	s.TagList = append(s.TagList, &MetricTag{Key: key, Value: value})
}

func (s *Metric) HasTag(key string) bool {
	for _, tag := range s.TagList {
		if tag.Key == key {
			return true
		}
	}
	return false
}

func (s *Metric) GetTag(key string) (string, bool) {
	for _, tag := range s.TagList {
		if tag.Key == key {
			return tag.Value, true
		}
	}
	return "", false
}

func (s *Metric) RemoveTag(key string) {
	for i, tag := range s.TagList {
		if tag.Key == key {
			copy(s.TagList[i:], s.TagList[i+1:])
			s.TagList[len(s.TagList)-1] = nil
			s.TagList = s.TagList[:len(s.TagList)-1]
			return
		}
	}
}

func (s *Metric) AddField(key string, value any) {
	for i, field := range s.FieldList {
		if key == field.Key {
			s.FieldList[i] = &MetricField{Key: key, Value: value}
			return
		}
	}
	s.FieldList = append(s.FieldList, &MetricField{Key: key, Value: value})
}

func (s *Metric) HasField(key string) bool {
	for _, field := range s.FieldList {
		if field.Key == key {
			return true
		}
	}
	return false
}

func (s *Metric) GetField(key string) (any, bool) {
	for _, field := range s.FieldList {
		if field.Key == key {
			return field.Value, true
		}
	}
	return nil, false
}

func (s *Metric) RemoveField(key string) {
	for i, field := range s.FieldList {
		if field.Key == key {
			copy(s.FieldList[i:], s.FieldList[i+1:])
			s.FieldList[len(s.FieldList)-1] = nil
			s.FieldList = s.FieldList[:len(s.FieldList)-1]
			return
		}
	}
}
