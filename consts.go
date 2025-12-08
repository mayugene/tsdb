package tsdb

import "time"

type ClientType string

const (
	ClientTypeTdengine           ClientType = "tdengine"
	ClientTypeRedis              ClientType = "redis"
	ClientTypeInfluxdbOfficialV1 ClientType = "influxdb_official_v1"
	ClientTypeInfluxdbV1         ClientType = "influxdb_v1"
)

const (
	RealTimeWindowDefaultStr      = "1m"
	RealTimeWindowDefaultDuration = time.Minute
	RealTimeWindowMinDuration     = time.Second
	fillNone                      = "NONE"
	fillNull                      = "NULL"
)

const (
	tdengineColumnTimestamp         = "_ts" // todo cannot use ts, return smlBuildCol error, do not know why
	tdengineColumnDevice            = "device"
	tdengineColumnAliasDevice       = "deviceId"
	tdengineColumnProject           = "project"
	tdengineColumnAliasProject      = "projectId"
	tdengineColumnAlarmOnline       = "online"
	tdengineColumnPseudoWindowStart = "_wstart" // it's a pseudo column, so back quote is not needed, or it will cause an error
	tdengineColumnPseudoWindowEnd   = "_wend"   // it's a pseudo column, so back quote is not needed, or it will cause an error
	tdengineTableTagsDevice         = "device"
	tdengineTableTagsProject        = "project"
	tdengineTableTagsType           = "NCHAR(16)"
	tdengineTableNameAlarm          = "alarm"
	tdengineTableNameKey            = "tableName"
	tdengineDataKeepMinimumStr      = "1d"
	tdengineDataKeepMinimumDuration = time.Hour * 24
	tdengineDefaultPassword         = "taosdata"
	tdengineDefaultDataType         = "DOUBLE"
)
const (
	redisKeyLatest               = "latest"
	redisKeyTimestamp            = "_ts"
	redisAutoExpireCronName      = "RedisAutoExpireCron"
	redisDataKeepDefaultStr      = "1h"
	redisDataKeepDefaultDuration = time.Hour
)
