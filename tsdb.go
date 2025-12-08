package tsdb

import (
	"context"
	"fmt"
	"sync"
)

var (
	instance     Client
	instanceType ClientType
	isCreated    bool
	once         sync.Once
)

type Client interface {
	Init(context.Context, Config) error
	IsHealthy(context.Context) bool
	Write(context.Context, []Metric) error
	ReadToMap(
		ctx context.Context,
		in ReadDeviceLatestDataInput,
		dataFilterMap map[string]float64,
	) (pointCodeValueMaps []map[string]any, pointCodes [][]string, err error)
	ReadToSeries(ctx context.Context, in ReadDeviceSeriesDataInput) (seriesData [][]any, timestamps []int64, err error)
	CreateSTable(ctx context.Context, stableName string, columns []TdengineColumn) error
}

type ClientCreator func() Client

type ClientFactory struct {
	creators map[ClientType]ClientCreator
}

func NewClientFactory() *ClientFactory {
	return &ClientFactory{
		creators: map[ClientType]ClientCreator{
			ClientTypeTdengine: NewTdengineClient,
			ClientTypeRedis:    NewRedisClient,
		},
	}
}

func GetClient() Client {
	return instance
}

func (f *ClientFactory) CreateClient(clientType ClientType) (Client, error) {
	once.Do(func() {
		instanceType = clientType
		creator, ok := f.creators[clientType]
		if !ok {
			instance = nil
			isCreated = false
			return
		}
		instance = creator()
		isCreated = true
	})
	if isCreated {
		return instance, nil
	} else {
		supportedTypes := fmt.Sprintf("[ %s ], [ %s ]", ClientTypeTdengine, ClientTypeRedis)
		return nil, fmt.Errorf("initial tsdb type [ %s ] is not in the support list: %s", instanceType, supportedTypes)
	}
}
