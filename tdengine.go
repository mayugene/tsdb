package tsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gogf/gf/v2/container/garray"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/net/gclient"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/gogf/gf/v2/util/gconv"
)

type tdengine struct {
	uri            string
	uriNoDb        string
	writeUri       string
	client         *gclient.Client
	host           string
	port           int
	username       string
	password       string
	database       string
	realTimeWindow string
	sync.Mutex
}

func (s *tdengine) IsHealthy(ctx context.Context) bool {
	qs := "SELECT SERVER_STATUS()"
	serializedData, err := s.httpRead(ctx, qs)
	if err != nil || serializedData == nil {
		return false
	}
	if serializedData.Rows == 1 && len(serializedData.Data) == 1 && len(serializedData.Data[0]) == 1 && gconv.Int(serializedData.Data[0][0]) == 1 {
		return true
	}
	return false
}

func NewTdengineClient() Client {
	return &tdengine{
		client: gclient.New(), // gclient.Client should always be reused for better performance and it is GC friendly
	}
}

func (s *tdengine) Init(ctx context.Context, config Config) (err error) {
	s.Lock()
	defer s.Unlock()

	if config.Host != "" {
		s.host = config.Host
	} else {
		return errors.New("host is required")
	}
	if config.Port > 0 {
		s.port = config.Port
	} else {
		return errors.New("port is required")
	}
	if config.Username != "" {
		s.username = config.Username
	} else {
		return errors.New("username is required")
	}
	if config.Password != "" {
		s.password = config.Password
	} else {
		return errors.New("password is required")
	}
	if config.Database != "" {
		s.database = config.Database
	} else {
		return errors.New("database is required")
	}
	dataKeep, _ := mustGetDataKeepFromConfig(config, ClientTypeTdengine)
	s.realTimeWindow, _ = mustGetRealTimeWindowFromConfig(config)

	s.uri = fmt.Sprintf("http://%s:%d/rest/sql/%s", s.host, s.port, s.database)
	s.uriNoDb = fmt.Sprintf("http://%s:%d/rest/sql", s.host, s.port)
	s.writeUri = fmt.Sprintf("http://%s:%d/influxdb/v1/write?db=%s", s.host, s.port, s.database)
	// originally try to use new password
	s.client.SetBasicAuth(s.username, s.password)

	// if no database, create one first
	dbInfo, err := s.operateDb(ctx, fmt.Sprintf("SHOW CREATE DATABASE `%s`", s.database))
	if err != nil {
		return err
	}
	if dbInfo.Code == 855 {
		// 855 means: 0x80000357, Authentication failure
		// auth fail, it must be the first time to use tdengine
		// so we use default password to connect
		// we need to:
		// 1. change default password
		// 2. create db
		s.client.SetBasicAuth(s.username, tdengineDefaultPassword)
		qs := fmt.Sprintf("ALTER USER root PASS '%s'", s.password)
		changePasswordRes, innerErr := s.operateDb(ctx, qs)
		if innerErr != nil || changePasswordRes.Code != 0 {
			return fmt.Errorf("failed to change password")
		}
		g.Log().Info(ctx, "tdengine password has been changed!")
		// password have changed, so use new password
		s.client.SetBasicAuth(s.username, s.password)
		qs = fmt.Sprintf("CREATE DATABASE `%s` BUFFER 48 PAGES 128 DURATION 6h KEEP %s", s.database, dataKeep)
		createDbRes, innerErr := s.operateDb(ctx, qs)
		g.Log().Info(ctx, "tdengine database has been created!")
		if innerErr != nil || (createDbRes.Code != 0 && createDbRes.Code != 897) {
			// code 897 means: Database already exists
			return fmt.Errorf("failed to create database")
		}
	} else if dbInfo.Code == 904 {
		// 904 means: 0x80000388, Database not exist
		// in this case, password is right, but db not exits
		// we only need to create db
		qs := fmt.Sprintf("CREATE DATABASE `%s` BUFFER 48 PAGES 128 DURATION 6h KEEP %s", s.database, dataKeep)
		createDbRes, innerErr := s.operateDb(ctx, qs)
		g.Log().Info(ctx, "tdengine database has been created!")
		if innerErr != nil || (createDbRes.Code != 0 && createDbRes.Code != 897) {
			// code 897 means: Database already exists
			return fmt.Errorf("failed to create database")
		}
	}

	// try to release client after init
	s.client.CloseIdleConnections()
	isHealthy := s.IsHealthy(ctx)
	if !isHealthy {
		return fmt.Errorf("we cannot connect to the tdengine server or the server is unhealthy")
	}
	return
}

func (s *tdengine) Write(ctx context.Context, metrics []Metric) (err error) {
	buffer := Serialize(metrics)
	if buffer.Len() == 0 {
		return
	}
	res, err := s.client.Post(ctx, s.writeUri, buffer.Bytes())
	defer res.Close() // res need to be closed to prevent oom
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		g.Log().Error(ctx, res.ReadAllString())
	}
	return err
}

func (s *tdengine) ReadToMap(
	ctx context.Context,
	in ReadDeviceLatestDataInput,
	dataFilterMap map[string]float64,
) (pointCodeValueMaps []map[string]any, pointCodes [][]string, err error) {
	qs := fmt.Sprintf(
		"SELECT %s FROM `%s` WHERE `%s` IN (%s) AND `%s`>NOW-%s PARTITION BY `%s`",
		WrapColumnsWithBackQuote(
			in.PointCodes,
			"last",
			true,
			true,
			false,
		),
		in.DeviceModelName,
		tdengineColumnDevice,
		WrapDevicesWithSingleQuote(in.DeviceIds),
		tdengineColumnTimestamp,
		s.realTimeWindow,
		tdengineColumnDevice,
	)
	serializedData, err := s.httpRead(ctx, qs)
	if err != nil {
		return nil, nil, err
	}
	pointCodeValueMaps = make([]map[string]any, 0)
	pointCodes = make([][]string, 0)
	// todo, filter data in memory because I cannot find a better SQL to do filter in tdengine
	constColumns := garray.NewStrArrayFrom([]string{
		tdengineColumnProject,
		tdengineColumnAliasProject,
		tdengineColumnDevice,
		tdengineColumnAliasDevice,
	})
	for _, dv := range serializedData.Data {
		m := make(map[string]any)
		pointCodesInOneTimestamp := make([]string, 0)
		isPassedFilter := true // whether equals the value given by the filter data map
		for i, cv := range serializedData.ColumnMeta {
			currentColumn := gconv.String(cv[0])
			if currentColumn == tdengineColumnTimestamp {
				// transform "2024-03-30T14:20:25.450Z" to unix time 1711808425450
				m[currentColumn] = gtime.New(dv[i]).UnixMilli()
			} else if constColumns.Contains(currentColumn) {
				// device/deviceId/project/projectId
				m[currentColumn] = dv[i]
			} else {
				// point code
				// dataFilterMap must not be nil and key must be contained
				// then compare value
				// if one point value is not equaled to the given value in filter map, this device will be omitted
				if dataFilterMap != nil {
					if currentColumnValue, ok := dataFilterMap[currentColumn]; ok {
						if gconv.Float64(dv[i]) != currentColumnValue {
							isPassedFilter = false
							break
						}
					}
				}
				m[currentColumn] = dv[i]
				pointCodesInOneTimestamp = append(pointCodesInOneTimestamp, currentColumn)
			}
		}
		if isPassedFilter {
			pointCodeValueMaps = append(pointCodeValueMaps, m)
			pointCodes = append(pointCodes, pointCodesInOneTimestamp)
		}
	}
	return
}

func (s *tdengine) ReadToSeries(
	ctx context.Context,
	in ReadDeviceSeriesDataInput,
) (seriesData [][]any, timestamps []int64, err error) {
	if in.FillOption == "" {
		in.FillOption = fillNone
	}
	var deviceId string
	if len(in.DeviceIds) <= 1 {
		return nil, nil, fmt.Errorf("data series for multiple devices is not supportted now")
	} else {
		deviceId = in.DeviceIds[0]
	}
	// select `p1`,`p2` from xxx order by `ts`
	qs := fmt.Sprintf(
		"SELECT %s,%s FROM `%s` WHERE `%s`='%s' AND `%s` >= %d AND `%s`<= %d INTERVAL(%s) FILL(%s)",
		tdengineColumnPseudoWindowStart,
		WrapColumnsWithBackQuote(
			in.PointCodes,
			"last",
			false,
			false,
			false,
		),
		in.DeviceModelName,
		tdengineTableTagsDevice,
		deviceId,
		tdengineColumnTimestamp,
		in.StartTime,
		tdengineColumnTimestamp,
		in.EndTime,
		in.Interval,
		in.FillOption,
	)

	serializedData, err := s.httpRead(ctx, qs)
	if err != nil {
		return nil, nil, err
	}
	if len(serializedData.Data) == 0 {
		return
	}
	tsIndex := 0 // ts must be the first column
	tsColumns := garray.NewStrArrayFrom([]string{tdengineColumnTimestamp, tdengineColumnPseudoWindowStart})
	//var series []*garray.Array
	series := make([][]any, 0)
	for _, dv := range serializedData.Data {
		for i, cv := range serializedData.ColumnMeta {
			if len(series) <= i {
				emptyAnyArray := make([]any, 0)
				series = append(series, emptyAnyArray)
			}
			if tsColumns.Contains(gconv.String(cv[0])) {
				// transform "2024-03-30T14:20:25.450Z" to unix time 1711808425450
				series[i] = append(series[i], gtime.New(dv[i]).UnixMilli())
			} else {
				series[i] = append(series[i], dv[i])
			}
		}
	}
	timestamps = gconv.Int64s(series[tsIndex])
	seriesData = series[tsIndex+1:]
	return
}

func (s *tdengine) httpRead(ctx context.Context, qs string) (*TdengineHttpOutput, error) {
	tdHttpRes, err := s.client.Post(ctx, s.uri, qs)
	defer tdHttpRes.Close() // res need to be closed to prevent oom
	if err != nil {
		return nil, err
	}
	jsonData, err := gjson.DecodeToJson(tdHttpRes.ReadAllString())
	if err != nil {
		return nil, err
	}
	serializedData := &TdengineHttpOutput{}
	err = jsonData.Scan(serializedData)
	if err != nil {
		return nil, err
	}
	return serializedData, err
}

func (s *tdengine) operateDb(ctx context.Context, qs string) (out *TdengineHttpOutput, err error) {
	tdHttpRes, err := s.client.Post(ctx, s.uriNoDb, qs)
	defer tdHttpRes.Close() // res need to be closed to prevent oom
	if err != nil {
		return nil, err
	}
	resDecode, err := gjson.DecodeToJson(tdHttpRes.ReadAllString())
	if err != nil {
		return nil, err
	}
	out = &TdengineHttpOutput{}
	err = resDecode.Scan(out)
	if err != nil {
		return nil, err
	}
	return
}
