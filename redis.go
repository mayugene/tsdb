package tsdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcron"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
)

/*
	!!! redis here is not for multiple projects !!!
	!!! when apis don't pass deviceIds, redis will return untrusted results !!!

	create 1 hash set for the latest data of device points, with TTL
	create 1 stream for time series data of devices
	we need to remove the outdated data manually using cron

	zset is currently commented, since for the same member of zset, although with different scores,
	zadd will update the score instead of insert a new record, that is not what we need
*/

type redis struct {
	dataKeep       time.Duration
	realTimeWindow int64 // seconds
	sync.Mutex
}

func NewRedisClient() Client {
	return &redis{}
}

func (s *redis) Init(ctx context.Context, config Config) (err error) {
	/*
		we must use g.Redis() here
		it's not a best idea to create a singleton instance of gredis here
		since redis instance should be managed globally by GoFrame
	*/
	s.Lock()
	defer s.Unlock()

	redisClient := g.Redis()
	if redisClient == nil {
		return fmt.Errorf("redis is not initialized because of no configs")
	}
	isHealthy := s.IsHealthy(ctx)
	if !isHealthy {
		return fmt.Errorf("we cannot connect to the redis server now")
	}

	_, s.dataKeep = mustGetDataKeepFromConfig(config, ClientTypeRedis)
	realTimeWindowString, realTimeWindowDuration := mustGetRealTimeWindowFromConfig(config)
	s.realTimeWindow = gconv.Int64(realTimeWindowDuration.Seconds())

	cronPattern := fmt.Sprintf("@every %s", realTimeWindowString)
	_, err = gcron.AddSingleton(ctx, cronPattern, func(ctx context.Context) {
		s.streamAutoExpire(ctx)
	}, redisAutoExpireCronName)
	return
}

func (s *redis) IsHealthy(ctx context.Context) bool {
	res, err := g.Redis().Do(ctx, "PING")
	if err != nil {
		return false
	}
	return !res.IsEmpty()
}

func (s *redis) Write(ctx context.Context, metrics []*Metric) error {
	for _, metric := range metrics {
		// tags and fields of a valid metric should not be empty
		if metric.TagList == nil || len(metric.TagList) == 0 || metric.FieldList == nil || len(metric.FieldList) == 0 {
			continue
		}
		// get deviceId from tag
		var deviceId string
		for _, tag := range metric.TagList {
			if tag.Key == tdengineColumnDevice {
				deviceId = tag.Value
			}
		}
		if deviceId == "" {
			// deviceId is a must
			continue
		}
		latestDataKey := fmt.Sprintf("%s:%s_%s", metric.Name, deviceId, redisKeyLatest)
		// millisecond
		timestamp := metric.Time.UnixMilli()

		devicePointDataMap := gmap.NewStrAnyMap()
		devicePointDataMap.Set(redisKeyTimestamp, timestamp)
		for _, field := range metric.FieldList {
			devicePointDataMap.Set(field.Key, field.Value)
			seriesDataKey := fmt.Sprintf("%s:%s", deviceId, field.Key)
			err := s.xAdd(ctx, seriesDataKey, timestamp, field.Value)
			if err != nil {
				g.Log().Errorf(ctx, "%s: %v", "xadd error", err)
			}
		}
		// update latest data
		_, _ = g.Redis().HSet(ctx, latestDataKey, devicePointDataMap.Map())
		_, _ = g.Redis().Expire(ctx, latestDataKey, s.realTimeWindow)
	}

	return nil
}

func (s *redis) ReadToMap(
	ctx context.Context,
	in ReadDeviceLatestDataInput,
	dataFilterMap map[string]float64,
) (pointCodeValueMaps []map[string]any, pointCodes [][]string, err error) {
	/*
		caution: projectId will not be used here
	*/
	pointCodeValueMaps = make([]map[string]any, 0)
	pointCodes = make([][]string, 0)
	targetDeviceIds := in.DeviceIds
	if len(in.DeviceIds) == 0 {
		keys, innErr := useRedisScan(ctx, gredis.ScanOption{Match: fmt.Sprintf("%s:*", in.DeviceModelName)})
		if innErr != nil {
			return nil, nil, innErr
		}
		targetDeviceIds = keys
	}
	for _, deviceId := range targetDeviceIds {
		latestDataKey := fmt.Sprintf("%s:%s_%s", in.DeviceModelName, deviceId, redisKeyLatest)
		rawZSet, loopErr := g.Redis().HGetAll(ctx, latestDataKey)
		if loopErr != nil {
			return nil, nil, loopErr
		}
		if rawZSet == nil {
			continue
		}
		zSetMap := rawZSet.Map()
		newMap := make(map[string]any)
		pointCodesInOneTimestamp := make([]string, 0)
		isPassedFilter := true // whether equals the value given by the filter data map
		for _, pointCode := range in.PointCodes {
			valueNow := zSetMap[pointCode]
			// dataFilterMap must not be nil and key must be contained
			// then compare value
			// if one point value is not equaled to the given value in filter map, this device will be omitted
			if dataFilterMap != nil {
				if pointValue, ok := dataFilterMap[pointCode]; ok {
					if gconv.Float64(valueNow) != pointValue {
						isPassedFilter = false
						break
					}
				}
			}
			if valueNow != nil {
				newMap[pointCode] = gconv.Int64(valueNow)
				pointCodesInOneTimestamp = append(pointCodesInOneTimestamp, pointCode)
			}
		}
		if isPassedFilter && !(len(newMap) == 0) {
			newMap[tdengineColumnAliasDevice] = deviceId
			pointCodeValueMaps = append(pointCodeValueMaps, newMap)
			pointCodes = append(pointCodes, pointCodesInOneTimestamp)
		}
	}

	return
}

func (s *redis) ReadToSeries(
	ctx context.Context,
	in ReadDeviceSeriesDataInput,
) (seriesData [][]any, timestamps []int64, err error) {
	allDeviceData, totalPointsCount := s.batchQueryDeviceData(ctx, in.DeviceIds, in.PointCodes, in.StartTime, in.EndTime)
	return ApplyTimeWindowAndFill(allDeviceData, totalPointsCount, in.DeviceModelName, in.StartTime, in.EndTime, in.Interval, in.FillOption)
}

func (s *redis) CreateSTable(ctx context.Context, stableName string, columns []TdengineColumn) error {
	panic("this is only for tdengine, redis does not have stables")
}

func (s *redis) batchQueryDeviceData(
	ctx context.Context,
	deviceIds []string,
	pointCodes []string,
	start int64,
	end int64,
) (allDeviceData map[string]map[string][]*RedisDataPoint, totalPointsCount int) {
	allDeviceData = make(map[string]map[string][]*RedisDataPoint)
	for _, deviceId := range deviceIds {
		deviceData := make(map[string][]*RedisDataPoint)
		for _, pointCode := range pointCodes {
			seriesDataKey := fmt.Sprintf("%s:%s", deviceId, pointCode)
			resultArray, err := s.xRange(ctx, seriesDataKey, start, end)
			if err != nil || len(resultArray) == 0 {
				continue
			}
			for _, res := range resultArray {
				parsedDataPoint := ParseStreamResult(res)
				deviceData[pointCode] = append(deviceData[pointCode], parsedDataPoint)
			}
			totalPointsCount++
		}
		allDeviceData[deviceId] = deviceData
	}
	return
}

func (s *redis) xAdd(ctx context.Context, key string, timestamp int64, value any) error {
	_, err := g.Redis().Do(ctx, "XADD", key, fmt.Sprintf("%d-*", timestamp), "value", value)
	return err
}

func (s *redis) xRange(ctx context.Context, key string, startTime int64, endTime int64) ([]string, error) {
	res, err := g.Redis().Do(ctx, "XRANGE", key, gconv.String(startTime), gconv.String(endTime))
	if err != nil {
		return nil, err
	}
	return res.Strings(), nil
}

func (s *redis) xTrim(ctx context.Context, key string, endTime int64) error {
	_, err := g.Redis().Do(ctx, "XTRIM", fmt.Sprintf("%s", key), "MINID", gconv.String(endTime))
	return err
}

func (s *redis) streamAutoExpire(ctx context.Context) {
	keys, err := useRedisScan(ctx, gredis.ScanOption{Type: "stream"})
	if err != nil {
		return
	}
	endTime := gtime.Now().Add(-1 * s.dataKeep).UnixMilli()
	for _, streamKey := range keys {
		err = s.xTrim(ctx, streamKey, endTime)
		if err != nil {
			g.Log().Errorf(ctx, "xtrim error: %v", err)
		}
	}
}
