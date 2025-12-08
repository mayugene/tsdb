package tsdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
)

func ParseStreamResult(input string) *RedisDataPoint {
	/*
		data format: ["1762828300498-0",["value","20"]]
	*/
	var decoded []any
	var valuePart []string
	jsonData, err := gjson.DecodeToJson(input)
	if err != nil {
		return nil
	}
	err = jsonData.Scan(&decoded)
	if err != nil {
		return nil
	}
	if len(decoded) != 2 {
		return nil
	}
	err = gconv.Scan(decoded[1], &valuePart)
	if err != nil {
		return nil
	}
	if len(valuePart) != 2 {
		return nil
	}
	timestamp := strings.Split(gconv.String(decoded[0]), "-")
	if len(timestamp) != 2 {
		return nil
	}
	return &RedisDataPoint{
		Value:     gconv.Int64(valuePart[1]),
		Timestamp: gtime.NewFromTimeStamp(gconv.Int64(timestamp[0])),
		IsFilled:  false,
	}
}

func ApplyTimeWindowAndFill(
	allDeviceData map[string]map[string][]*RedisDataPoint,
	totalPointsCount int,
	deviceModelName string,
	start int64, // unix time, seconds
	end int64, // unix time, seconds
	interval string,
	fillType string,
) (seriesData [][]any, timestamps []int64, err error) {
	// seriesData [][]any, timestamps []int64,
	timestampsAny := garray.NewArray()
	seriesData = make([][]any, 0)
	seriesDataMap := make(map[string]*garray.Array)
	// used internally for accelerating looping
	searchIndexMap := make(map[string]map[string]int)
	// used for fill NONE
	noValueCountsArray := garray.NewIntArray()

	duration, err := gtime.ParseDuration(interval)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid interval: %s", interval)
	}

	startTime := gtime.NewFromTimeStamp(start)
	endTime := gtime.NewFromTimeStamp(end)
	currentWindowStart := startTime
	for currentWindowStart.Before(endTime) || currentWindowStart.Equal(endTime) {
		currentWindowEnd := currentWindowStart.Add(duration)

		timestampsAny.Append(currentWindowEnd.UnixMilli())
		noValueCounts := 0

		// handle device data
		for deviceId, deviceData := range allDeviceData {
			if searchIndexMap[deviceId] == nil {
				searchIndexMap[deviceId] = make(map[string]int)
			}
			for pointCode, pointValues := range deviceData {
				mapKey := fmt.Sprintf("%s:%s_%s", deviceModelName, deviceId, pointCode)
				if _, ok := seriesDataMap[mapKey]; !ok {
					seriesDataMap[mapKey] = garray.NewArray()
				}
				currentIdx := searchIndexMap[deviceId][pointCode] // if pointCode not in indexMap[deviceId], we got 0
				// to find a proper value in this window
				windowValue, newIdx := findValueWithIndex(pointValues, currentIdx, currentWindowStart, currentWindowEnd)
				// By default, fill null if not find a proper value
				// to fill none, fill null first and then delete the all null time
				switch fillType {
				case fillNull:
					// we cannot directly use seriesDataMap[mapKey].Append(windowValue)
					// since windowValue is nil(*int64) which is different from nil during json marshal
					if windowValue == nil {
						seriesDataMap[mapKey].Append(nil)
					} else {
						seriesDataMap[mapKey].Append(windowValue)
					}
				default:
					seriesDataMap[mapKey].Append(windowValue)
				}
				if windowValue == nil {
					noValueCounts++
				}
				// next window, we will search from the newIdx
				searchIndexMap[deviceId][pointCode] = newIdx
			}
		}
		noValueCountsArray.Append(noValueCounts)

		if !currentWindowEnd.Before(endTime) {
			break
		} else {
			currentWindowStart = currentWindowEnd // for next loop
		}
	}
	// find all timestamps that null count == totalPointsCount
	allNullIndex := findAllNullIndex(noValueCountsArray, totalPointsCount)
	// remove timestamps that all data are null
	if fillType == fillNone {
		removeItemByIndex(timestampsAny, allNullIndex)
	}
	// format series data
	for _, mapItem := range seriesDataMap {
		// remove timestamps that all data are null
		if fillType == fillNone {
			removeItemByIndex(mapItem, allNullIndex)
		}
		seriesData = append(seriesData, mapItem.Slice())
	}

	return seriesData, gconv.Int64s(timestampsAny), nil
}

func findValueWithIndex(pointValues []*RedisDataPoint, startIdx int, start *gtime.Time, end *gtime.Time) (*int64, int) {
	// optimized, will search from the index recorded before to prevent duplicated search
	var lastValue *int64

	for i := startIdx; i < len(pointValues); i++ {
		// before start, continue
		if pointValues[i].Timestamp.Before(start) {
			continue
		}
		// >= end return
		if !pointValues[i].Timestamp.Before(end) {
			// "i" here indicates the next index of lastValue
			// so no duplicated search
			return lastValue, i
		}
		// in window
		value := pointValues[i].Value
		lastValue = &value
	}
	// when looping the array, find nothing
	// return the length of array
	// and next time, it will still return nil(*int64) for lastValue
	// caution: nil(*int64) is different from nil during json marshal
	return lastValue, len(pointValues)
}

func findAllNullIndex(dataArray *garray.IntArray, count int) *garray.IntArray {
	allNullIndex := garray.NewIntArray()
	dataArray.Iterator(func(k int, v int) bool {
		if v == count {
			allNullIndex.Append(k)
		}
		return true
	})
	return allNullIndex
}

func removeItemByIndex(data *garray.Array, idxToRemove *garray.IntArray) *garray.Array {
	if idxToRemove == nil || idxToRemove.Len() == 0 {
		return data
	}
	data.IteratorDesc(func(k int, v any) bool {
		if idxToRemove.Contains(k) {
			data.Remove(k)
		}
		return true
	})
	return data
}

func useRedisScan(ctx context.Context, scanOption gredis.ScanOption) ([]string, error) {
	out := make([]string, 0)
	var cursor uint64
	var keys []string
	var err error
	for {
		cursor, keys, err = g.Redis().Scan(ctx, cursor, scanOption)
		if err != nil {
			break
		}
		out = append(out, keys...)
		if cursor == 0 {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
