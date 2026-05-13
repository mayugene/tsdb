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
		if innErr != nil || dataKeepDuration == 0 || int64(dataKeepDuration.Hours()) < int64(defaultDataKeepDuration.Hours()) {
			return defaultDataKeepStr, defaultDataKeepDuration
		}

		// parse success, this string is valid
		return config.DataKeep, dataKeepDuration
	}

	return defaultDataKeepStr, defaultDataKeepDuration
}

func mustGetRealTimeWindowFromConfig(config Config) (string, time.Duration) {
	if config.RealTimeWindow != "" {
		realTimeWindowDuration, innErr := gtime.ParseDuration(config.RealTimeWindow)
		if innErr != nil || realTimeWindowDuration == 0 || int64(realTimeWindowDuration) < int64(RealTimeWindowMinDuration) {
			return RealTimeWindowDefaultStr, RealTimeWindowDefaultDuration
		}

		// parse success, this string is valid
		return config.RealTimeWindow, realTimeWindowDuration
	}

	return RealTimeWindowDefaultStr, RealTimeWindowDefaultDuration
}

func getValidFillOption(in string) string {
	switch in {
	case fillNone, fillNull, fillLinear:
		return in
	default:
		return fillNone
	}
}
