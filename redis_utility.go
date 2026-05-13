package tsdb

import (
	"context"
	"fmt"
	"math"
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
	start int64, // Unix time, seconds
	end int64,   // Unix time, seconds
	interval string,
	fillOption string,
) (seriesData [][]any, timestamps []int64, err error) {
	timestampsAny := garray.NewArray()
	seriesData = make([][]any, 0)
	seriesDataMap := make(map[string]*garray.Array)
	// used internally for accelerating looping
	searchIndexMap := make(map[string]map[string]int)
	// used for fill NONE
	noValueCountsArray := garray.NewIntArray()

	// for fillOption=LINEAR
	lastKnownIndexMap := make(map[string]int) // mapKey -> index in pointValues

	duration, err := gtime.ParseDuration(interval)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid interval: %s", interval)
	}

	startTime := gtime.NewFromTimeStamp(start)
	endTime := gtime.NewFromTimeStamp(end)
	currentWindowStart := startTime

	for deviceId, deviceData := range allDeviceData {
		if searchIndexMap[deviceId] == nil {
			searchIndexMap[deviceId] = make(map[string]int)
		}
		for pointCode, pointValues := range deviceData {
			mapKey := fmt.Sprintf("%s:%s_%s", deviceModelName, deviceId, pointCode)
			// 找到 start 之前的最后一个点的索引
			lastIdx := -1
			for i, pt := range pointValues {
				if pt.Timestamp.Before(startTime) {
					lastIdx = i
				} else {
					break
				}
			}
			lastKnownIndexMap[mapKey] = lastIdx
			// 搜索从该点之后开始
			searchIndexMap[deviceId][pointCode] = lastIdx + 1
		}
	}

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
				switch fillOption {
				case fillNull:
					// we cannot directly use seriesDataMap[mapKey].Append(windowValue)
					// since windowValue is nil(*int64) which is different from nil during JSON marshal
					if windowValue == nil {
						seriesDataMap[mapKey].Append(nil)
					} else {
						seriesDataMap[mapKey].Append(windowValue)
					}

				case fillLinear:
					// get last known index, if not exist, it will be -1
					lastIdx, hasLast := lastKnownIndexMap[mapKey]
					if !hasLast {
						lastIdx = -1
					}
					if windowValue != nil {
						// if there are data in this window, use it and update lastKnownIndex
						lastKnownIndexMap[mapKey] = newIdx - 1
					} else {
						// no data, execute linear fill
						var prevPoint, nextPoint *RedisDataPoint
						if lastIdx >= 0 && lastIdx < len(pointValues) {
							prevPoint = pointValues[lastIdx]
						}
						if newIdx < len(pointValues) {
							nextPoint = pointValues[newIdx]
						}

						if prevPoint != nil && nextPoint != nil {
							targetMs := currentWindowEnd.UnixMilli()
							prevMs := prevPoint.Timestamp.UnixMilli()
							nextMs := nextPoint.Timestamp.UnixMilli()

							if nextMs == prevMs {
								windowValue = &prevPoint.Value
							} else {
								ratio := float64(targetMs-prevMs) / float64(nextMs-prevMs)
								interP := float64(prevPoint.Value) + ratio*float64(nextPoint.Value-prevPoint.Value)
								windowValue = new(int64(math.Round(interP)))
							}
						} else if prevPoint != nil {
							// only previous exists, use prev to fill
							windowValue = &prevPoint.Value
						} else if nextPoint != nil {
							// only next exists, use next to fill
							windowValue = &nextPoint.Value
						}
					}
					seriesDataMap[mapKey].Append(windowValue)

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
	if fillOption == fillNone {
		removeItemByIndex(timestampsAny, allNullIndex)
	}
	// format series data
	for _, mapItem := range seriesDataMap {
		// remove timestamps that all data are null
		if fillOption == fillNone {
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
		lastValue = new(pointValues[i].Value)
	}
	// when looping the array, find nothing
	// return the length of array
	// and next time, it will still return nil(*int64) for lastValue
	// caution: nil(*int64) is different from nil during JSON marshal
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
