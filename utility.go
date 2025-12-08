package tsdb

import (
	"fmt"
	"time"

	"github.com/gogf/gf/v2/os/gtime"
)

func mustGetDataKeepFromConfig(config Config, clientType ClientType) (outStr string, outDuration time.Duration) {
	var defaultDataKeepStr string
	var defaultDataKeepDuration time.Duration
	switch clientType {
	case ClientTypeRedis:
		defaultDataKeepStr = redisDataKeepDefaultStr
		defaultDataKeepDuration = redisDataKeepDefaultDuration
	case ClientTypeTdengine:
		defaultDataKeepStr = tdengineDataKeepMinimumStr
		defaultDataKeepDuration = tdengineDataKeepMinimumDuration
	}
	if config.DataKeep != "" {
		dataKeepDuration, innErr := gtime.ParseDuration(config.DataKeep)
		if innErr != nil || dataKeepDuration == 0 || dataKeepDuration.Hours() < 24 {
			outStr = defaultDataKeepStr
			outDuration = defaultDataKeepDuration
		} else {
			outStr = fmt.Sprintf("%dh", int64(dataKeepDuration.Hours()))
			outDuration = dataKeepDuration
		}
	} else {
		outStr = defaultDataKeepStr
		outDuration = defaultDataKeepDuration
	}
	return
}

func mustGetRealTimeWindowFromConfig(config Config) (outStr string, outDuration time.Duration) {
	if config.RealTimeWindow != "" {
		realTimeWindowDuration, innErr := gtime.ParseDuration(config.RealTimeWindow)
		if innErr != nil || realTimeWindowDuration == 0 {
			outStr = RealTimeWindowDefaultStr
			outDuration = RealTimeWindowDefaultDuration
		} else {
			// parse success, this string is valid
			outStr = config.RealTimeWindow
			outDuration = realTimeWindowDuration
		}
	} else {
		outStr = RealTimeWindowDefaultStr
		outDuration = RealTimeWindowDefaultDuration
	}
	return
}
