package tsdb

import "github.com/gogf/gf/v2/os/gtime"

type RedisZSetPointData struct {
	PointCode  string
	PointValue int64
	Timestamp  int64
}

type RedisDataPoint struct {
	Value     int64
	Timestamp *gtime.Time
	idx       int // for sliding window
	IsFilled  bool
}
