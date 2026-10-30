package tsdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/container/gmap"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcron"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gutil"
	goredis "github.com/redis/go-redis/v9"
)

/*
	!!! redis here is not for multiple projects !!!
	!!! when APIs don't pass deviceIds, redis will return untrusted results !!!

	create 1 hash set for the latest data of device points, with TTL
	create 1 stream for time series data of devices
	we need to remove the outdated data manually using cron

	ZSET is currently commented, since for the same member of ZSET, although with different scores,
	ZADD will update the score instead of insert a new record, that is not what we need
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
	const pipelineBatchSize = 128
	if ctx == nil {
		ctx = context.Background()
	}
	var (
		writeErrors []error
		redisClient *gredis.Redis
		pipeline    goredis.Pipeliner
	)
	flush := func() {
		if pipeline == nil || pipeline.Len() == 0 {
			return
		}
		commands, execErr := pipeline.Exec(ctx)
		execErrorReported := false
		for _, command := range commands {
			if err := command.Err(); err != nil {
				writeErrors = append(writeErrors, fmt.Errorf("redis %s: %w", strings.ToUpper(command.Name()), err))
				if errors.Is(err, execErr) {
					execErrorReported = true
				}
			}
		}
		if execErr != nil && !execErrorReported {
			writeErrors = append(writeErrors, fmt.Errorf("redis pipeline: %w", execErr))
		}
	}
	queue := func(command string, args ...any) {
		pipelineSafe := true
		// Match GoFrame's Conn.Do encoding before bypassing it for the native pipeline.
		for i, value := range args {
			switch gutil.OriginTypeAndKind(value).OriginKind {
			case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
				if _, ok := value.([]byte); !ok {
					encoded, err := gjson.Marshal(value)
					if err != nil {
						// Collect earlier replies first to retain command error ordering.
						flush()
						writeErrors = append(writeErrors, fmt.Errorf("redis %s: %w", command, err))
						return
					}
					args[i] = encoded
				}
			}
			switch args[i].(type) {
			case nil, string, *string, []byte, bool, *bool,
				int, *int, int8, *int8, int16, *int16, int32, *int32, int64, *int64,
				uint, *uint, uint8, *uint8, uint16, *uint16, uint32, *uint32, uint64, *uint64,
				float32, *float32, float64, *float64, time.Duration, *time.Duration:
			default:
				pipelineSafe = false
			}
		}
		if !pipelineSafe {
			// A custom encoder or unsupported type must not abort unrelated commands in a batch.
			flush()
			if _, err := redisClient.Do(ctx, command, args...); err != nil {
				writeErrors = append(writeErrors, fmt.Errorf("redis %s: %w", command, err))
			}
			return
		}
		pipeline.Do(ctx, append([]any{command}, args...)...)
		if pipeline.Len() >= pipelineBatchSize {
			flush()
		}
	}
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
		if redisClient == nil {
			redisClient = g.Redis()
			// Cluster pipelines can reorder cross-shard commands, so retain their synchronous path.
			if client, ok := redisClient.Client().(*goredis.Client); ok {
				pipeline = client.Pipeline()
			}
		}
		latestDataKey := fmt.Sprintf("%s:%s_%s", metric.Name, deviceId, redisKeyLatest)
		// millisecond
		timestamp := metric.Time.UnixMilli()

		devicePointDataMap := gmap.NewStrAnyMap()
		devicePointDataMap.Set(redisKeyTimestamp, timestamp)
		for _, field := range metric.FieldList {
			devicePointDataMap.Set(field.Key, field.Value)
			seriesDataKey := fmt.Sprintf("%s:%s", deviceId, field.Key)
			if pipeline != nil {
				queue("XADD", seriesDataKey, fmt.Sprintf("%d-*", timestamp), "value", field.Value)
				continue
			}
			err := s.xAdd(ctx, seriesDataKey, timestamp, field.Value)
			if err != nil {
				writeErrors = append(writeErrors, fmt.Errorf("redis XADD: %w", err))
			}
		}
		// update latest data
		if pipeline != nil {
			args := []any{latestDataKey}
			for key, value := range devicePointDataMap.Map() {
				args = append(args, key, value)
			}
			queue("HSET", args...)
			queue("EXPIRE", latestDataKey, s.realTimeWindow)
			continue
		}
		if _, err := redisClient.HSet(ctx, latestDataKey, devicePointDataMap.Map()); err != nil {
			writeErrors = append(writeErrors, fmt.Errorf("redis HSET: %w", err))
		}
		if _, err := redisClient.Expire(ctx, latestDataKey, s.realTimeWindow); err != nil {
			writeErrors = append(writeErrors, fmt.Errorf("redis EXPIRE: %w", err))
		}
	}
	flush()

	return errors.Join(writeErrors...)
}

func (s *redis) ReadToMap(
	ctx context.Context,
	in ReadDeviceLatestDataInput,
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
		isPassedFilter := true
		for _, point := range in.Points {
			valueNow := zSetMap[point.PointCode]
			if !pointValueMatchesRange(valueNow, point) {
				isPassedFilter = false
				break
			}
			if valueNow != nil {
				newMap[point.PointCode] = gconv.Int64(valueNow)
				pointCodesInOneTimestamp = append(pointCodesInOneTimestamp, point.PointCode)
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

func pointValueMatchesRange(value any, point PointWithRange) bool {
	hasFilter := point.MinValue != nil || point.MaxValue != nil || point.EqualValue != nil
	if value == nil {
		return !hasFilter
	}
	floatValue := gconv.Float64(value)
	if point.MinValue != nil && floatValue < *point.MinValue {
		return false
	}
	if point.MaxValue != nil && floatValue > *point.MaxValue {
		return false
	}
	if point.EqualValue != nil && floatValue != *point.EqualValue {
		return false
	}
	return true
}

func (s *redis) ReadToSeries(
	ctx context.Context,
	in ReadDeviceSeriesDataInput,
) (seriesData [][]any, timestamps []int64, err error) {
	allDeviceData, totalPointsCount := s.batchQueryDeviceData(ctx, in.DeviceIds, in.PointCodes, in.StartTime, in.EndTime)
	return ApplyTimeWindowAndFill(allDeviceData, totalPointsCount, in.DeviceModelName, in.StartTime, in.EndTime, in.Interval, getValidFillOption(in.FillOption))
}

func pointCodesFromRanges(points []PointWithRange) []string {
	pointCodes := make([]string, 0, len(points))
	for _, point := range points {
		pointCodes = append(pointCodes, point.PointCode)
	}
	return pointCodes
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
