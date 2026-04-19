package tsdb

import (
	"testing"
	"time"
)

func TestMustGetDataKeepFromConfig(t *testing.T) {
	tests := []struct {
		name            string
		config          Config
		clientType      ClientType
		wantStr         string
		wantDuration    time.Duration
	}{
		{
			name:            "redis default when empty config",
			config:          Config{},
			clientType:      ClientTypeRedis,
			wantStr:         redisDataKeepDefaultStr,
			wantDuration:    redisDataKeepDefaultDuration,
		},
		{
			name:            "tdengine default when empty config",
			config:          Config{},
			clientType:      ClientTypeTdengine,
			wantStr:         tdengineDataKeepMinimumStr,
			wantDuration:    tdengineDataKeepMinimumDuration,
		},
		{
			name:            "redis accepts valid data keep",
			config:          Config{DataKeep: "2h"},
			clientType:      ClientTypeRedis,
			wantStr:         "2h",
			wantDuration:    2 * time.Hour,
		},
		{
			name:            "redis rejects too short data keep",
			config:          Config{DataKeep: "30m"},
			clientType:      ClientTypeRedis,
			wantStr:         redisDataKeepDefaultStr,
			wantDuration:    redisDataKeepDefaultDuration,
		},
		{
			name:            "redis rejects invalid format",
			config:          Config{DataKeep: "invalid"},
			clientType:      ClientTypeRedis,
			wantStr:         redisDataKeepDefaultStr,
			wantDuration:    redisDataKeepDefaultDuration,
		},
		{
			name:            "redis accepts 1d (minimum for tdengine but valid for redis)",
			config:          Config{DataKeep: "1d"},
			clientType:      ClientTypeRedis,
			wantStr:         "1d",
			wantDuration:    24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStr, gotDuration := mustGetDataKeepFromConfig(tt.config, tt.clientType)
			if gotStr != tt.wantStr {
				t.Errorf("mustGetDataKeepFromConfig() str = %v, want %v", gotStr, tt.wantStr)
			}
			if gotDuration != tt.wantDuration {
				t.Errorf("mustGetDataKeepFromConfig() duration = %v, want %v", gotDuration, tt.wantDuration)
			}
		})
	}
}

func TestMustGetRealTimeWindowFromConfig(t *testing.T) {
	tests := []struct {
		name         string
		config       Config
		wantStr      string
		wantDuration time.Duration
	}{
		{
			name:         "default when empty config",
			config:       Config{},
			wantStr:      RealTimeWindowDefaultStr,
			wantDuration: RealTimeWindowDefaultDuration,
		},
		{
			name:         "accepts valid real time window",
			config:       Config{RealTimeWindow: "5m"},
			wantStr:      "5m",
			wantDuration: 5 * time.Minute,
		},
		{
			name:         "rejects too short real time window",
			config:       Config{RealTimeWindow: "500ms"},
			wantStr:      RealTimeWindowDefaultStr,
			wantDuration: RealTimeWindowDefaultDuration,
		},
		{
			name:         "rejects invalid format",
			config:       Config{RealTimeWindow: "invalid"},
			wantStr:      RealTimeWindowDefaultStr,
			wantDuration: RealTimeWindowDefaultDuration,
		},
		{
			name:         "accepts 1s (minimum valid)",
			config:       Config{RealTimeWindow: "1s"},
			wantStr:      "1s",
			wantDuration: 1 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStr, gotDuration := mustGetRealTimeWindowFromConfig(tt.config)
			if gotStr != tt.wantStr {
				t.Errorf("mustGetRealTimeWindowFromConfig() str = %v, want %v", gotStr, tt.wantStr)
			}
			if gotDuration != tt.wantDuration {
				t.Errorf("mustGetRealTimeWindowFromConfig() duration = %v, want %v", gotDuration, tt.wantDuration)
			}
		})
	}
}