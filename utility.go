package tsdb

import (
	"time"

	"github.com/gogf/gf/v2/os/gtime"
)

func mustGetDataKeepFromConfig(config Config, clientType ClientType) (string, time.Duration) {
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
		dataKeepDuration, innErr := gtime.ParseDuration(config.DataKeep) // support parsing days from "100d"
		if innErr != nil || dataKeepDuration == 0 || int64(dataKeepDuration.Hours()) < int64(defaultDataKeepDuration) {
			return defaultDataKeepStr, defaultDataKeepDuration
		} else {
			// parse success, this string is valid
			return config.DataKeep, dataKeepDuration
		}
	} else {
		return defaultDataKeepStr, defaultDataKeepDuration
	}
}

func mustGetRealTimeWindowFromConfig(config Config) (string, time.Duration) {
	if config.RealTimeWindow != "" {
		realTimeWindowDuration, innErr := gtime.ParseDuration(config.RealTimeWindow)
		if innErr != nil || realTimeWindowDuration == 0 || int64(realTimeWindowDuration) < int64(RealTimeWindowMinDuration) {
			return RealTimeWindowDefaultStr, RealTimeWindowDefaultDuration
		} else {
			// parse success, this string is valid
			return config.RealTimeWindow, realTimeWindowDuration
		}
	} else {
		return RealTimeWindowDefaultStr, RealTimeWindowDefaultDuration
	}
}
