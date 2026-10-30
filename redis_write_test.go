package tsdb

import (
	"context"
	"errors"
	"testing"

	"github.com/gogf/gf/v2/container/gvar"
	"github.com/gogf/gf/v2/database/gredis"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gtime"
)

func TestRedisWriteAggregatesXAddHSetAndExpireErrors(t *testing.T) {
	xaddErr := errors.New("xadd failed")
	hsetErr := errors.New("hset failed")
	expireErr := errors.New("expire failed")
	client := newRedisWriteTestClient(t)
	client.errors = map[string]error{
		"XADD":   xaddErr,
		"HSET":   hsetErr,
		"EXPIRE": expireErr,
	}
	writer := &redis{realTimeWindow: 60}

	err := writer.Write(t.Context(), redisWriteTestMetrics())
	if err == nil {
		t.Error("Write returned nil after XADD, HSET, and Expire errors")
	}
	for _, wantErr := range []error{xaddErr, hsetErr, expireErr} {
		if !errors.Is(err, wantErr) {
			t.Errorf("Write error %v does not contain %v", err, wantErr)
		}
	}
	for _, operation := range []string{"XADD", "HSET", "EXPIRE"} {
		if got := client.calls[operation]; got != 1 {
			t.Errorf("%s call count = %d, want 1", operation, got)
		}
	}

	client.errors = nil
	client.calls = make(map[string]int)
	if err := writer.Write(t.Context(), redisWriteTestMetrics()); err != nil {
		t.Fatalf("Write returned error when all Redis operations succeeded: %v", err)
	}
}

func newRedisWriteTestClient(t *testing.T) *redisWriteTestClient {
	t.Helper()
	// GoFrame caches the default client, so keep these tests serial and reuse its offline adapter.
	gredis.RegisterAdapterFunc(func(*gredis.Config) gredis.Adapter { return &redisWriteTestClient{} })
	gredis.SetConfig(&gredis.Config{})
	client := g.Redis().GetAdapter().(*redisWriteTestClient)
	*client = redisWriteTestClient{calls: make(map[string]int)}
	t.Cleanup(func() { *client = redisWriteTestClient{} })
	return client
}

func redisWriteTestMetrics() []*Metric {
	return []*Metric{{
		Name: "energy",
		TagList: []*MetricTag{{
			Key:   tdengineColumnDevice,
			Value: "device-1",
		}},
		FieldList: []*MetricField{{
			Key:   "value",
			Value: 1,
		}},
		Time: gtime.Now(),
	}}
}

type redisWriteTestClient struct {
	gredis.AdapterOperation
	gredis.IGroupGeneric
	gredis.IGroupHash
	errors      map[string]error
	calls       map[string]int
	client      gredis.RedisRawClient
	commands    [][]any
	batches     []int
	execError   error
	directError error
}

func (c *redisWriteTestClient) Client() gredis.RedisRawClient { return c.client }

func (c *redisWriteTestClient) Do(_ context.Context, command string, args ...any) (*gvar.Var, error) {
	err := c.record(command, args...)
	if c.directError != nil {
		err = c.directError
	}
	return nil, err
}

func (c *redisWriteTestClient) HSet(_ context.Context, key string, fields map[string]any) (int64, error) {
	return 1, c.record("HSET", key, fields)
}

func (c *redisWriteTestClient) Expire(_ context.Context, key string, seconds int64, _ ...gredis.ExpireOption) (int64, error) {
	return 1, c.record("EXPIRE", key, seconds)
}

func (c *redisWriteTestClient) record(operation string, args ...any) error {
	c.calls[operation]++
	c.commands = append(c.commands, append([]any{operation}, args...))
	return c.errors[operation]
}

func (c *redisWriteTestClient) GroupGeneric() gredis.IGroupGeneric     { return c }
func (c *redisWriteTestClient) GroupHash() gredis.IGroupHash           { return c }
func (c *redisWriteTestClient) GroupList() gredis.IGroupList           { return nil }
func (c *redisWriteTestClient) GroupPubSub() gredis.IGroupPubSub       { return nil }
func (c *redisWriteTestClient) GroupScript() gredis.IGroupScript       { return nil }
func (c *redisWriteTestClient) GroupSet() gredis.IGroupSet             { return nil }
func (c *redisWriteTestClient) GroupSortedSet() gredis.IGroupSortedSet { return nil }
func (c *redisWriteTestClient) GroupString() gredis.IGroupString       { return nil }
