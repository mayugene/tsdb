package tsdb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gogf/gf/v2/os/gtime"
	goredis "github.com/redis/go-redis/v9"
)

func TestRedisWritePipelinesCommandsAndPreservesData(t *testing.T) {
	client := newRedisPipelineWriteTestClient(t)
	first := redisPipelineTestMetric(
		&MetricField{Key: "p1", Value: 1},
		&MetricField{Key: "p2", Value: int64(9223372036854775807)},
		&MetricField{Key: "p1", Value: 3},
	)
	second := redisPipelineTestMetric(&MetricField{Key: "p2", Value: 4})

	if err := (&redis{realTimeWindow: 60}).Write(t.Context(), []*Metric{first, second}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.batches, []int{8}) {
		t.Fatalf("pipeline batches = %v, want one batch of 8 commands", client.batches)
	}
	want := [][]any{
		{"XADD", "device-1:p1", "1700000000123-*", "value", 1},
		{"XADD", "device-1:p2", "1700000000123-*", "value", int64(9223372036854775807)},
		{"XADD", "device-1:p1", "1700000000123-*", "value", 3},
		{"HSET", "energy:device-1_latest", map[string]any{"_ts": int64(1700000000123), "p1": 3, "p2": int64(9223372036854775807)}},
		{"EXPIRE", "energy:device-1_latest", int64(60)},
		{"XADD", "device-1:p2", "1700000000123-*", "value", 4},
		{"HSET", "energy:device-1_latest", map[string]any{"_ts": int64(1700000000123), "p2": 4}},
		{"EXPIRE", "energy:device-1_latest", int64(60)},
	}
	for i, command := range client.commands {
		if command[0] == "HSET" {
			fields := make(map[string]any)
			for j := 2; j < len(command); j += 2 {
				fields[command[j].(string)] = command[j+1]
			}
			command = []any{command[0], command[1], fields}
		}
		if !reflect.DeepEqual(command, want[i]) {
			t.Errorf("command %d = %#v, want %#v", i, command, want[i])
		}
	}
}

func TestRedisWritePipelinePreservesGoFrameValueEncoding(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
		want  any
	}{
		{name: "nil", value: nil, want: nil},
		{name: "boolean", value: true, want: true},
		{name: "bytes", value: []byte{0, 1, 255}, want: []byte{0, 1, 255}},
		{name: "map", value: map[string]int{"n": 1}, want: []byte(`{"n":1}`)},
		{name: "slice", value: []int{1, 2}, want: []byte(`[1,2]`)},
		{name: "array", value: [2]int{3, 4}, want: []byte(`[3,4]`)},
		{name: "struct pointer", value: &struct{ N int }{N: 5}, want: []byte(`{"N":5}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newRedisPipelineWriteTestClient(t)
			metric := redisPipelineTestMetric(&MetricField{Key: "p1", Value: tt.value})
			if err := (&redis{realTimeWindow: 60}).Write(t.Context(), []*Metric{metric}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(client.batches, []int{3}) {
				t.Fatalf("pipeline batches = %v, want [3]", client.batches)
			}
			if got := client.commands[0][4]; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("stream value = %#v, want %#v", got, tt.want)
			}
			latest := client.commands[1]
			for i := 2; i < len(latest); i += 2 {
				if latest[i] == "p1" && !reflect.DeepEqual(latest[i+1], tt.want) {
					t.Errorf("latest value = %#v, want %#v", latest[i+1], tt.want)
				}
			}
		})
	}
}

func TestRedisWritePipelineSkipsInvalidMetrics(t *testing.T) {
	client := newRedisPipelineWriteTestClient(t)
	metrics := []*Metric{
		{},
		{TagList: []*MetricTag{{Key: "device", Value: "device-1"}}},
		{FieldList: []*MetricField{{Key: "p1", Value: 1}}},
		{TagList: []*MetricTag{{Key: "other", Value: "device-1"}}, FieldList: []*MetricField{{Key: "p1", Value: 1}}},
		{TagList: []*MetricTag{{Key: "device", Value: ""}}, FieldList: []*MetricField{{Key: "p1", Value: 1}}},
	}
	if err := (&redis{}).Write(t.Context(), metrics); err != nil {
		t.Fatal(err)
	}
	if len(client.commands) != 0 || len(client.batches) != 0 {
		t.Fatalf("invalid metrics issued commands: %v, batches: %v", client.commands, client.batches)
	}
}

func TestRedisWritePipelineAggregatesCommandErrors(t *testing.T) {
	client := newRedisPipelineWriteTestClient(t)
	xaddErr := errors.New("xadd failed")
	hsetErr := errors.New("hset failed")
	expireErr := errors.New("expire failed")
	client.errors = map[string]error{"XADD": xaddErr, "HSET": hsetErr, "EXPIRE": expireErr}
	err := (&redis{realTimeWindow: 60}).Write(t.Context(), redisWriteTestMetrics())
	for _, want := range []error{xaddErr, hsetErr, expireErr} {
		if !errors.Is(err, want) {
			t.Errorf("Write error %v does not contain %v", err, want)
		}
	}
	if !reflect.DeepEqual(client.batches, []int{3}) {
		t.Fatalf("pipeline batches = %v, want [3]", client.batches)
	}
	if got, want := err.Error(), "redis XADD: xadd failed\nredis HSET: hset failed\nredis EXPIRE: expire failed"; got != want {
		t.Errorf("error = %q, want %q", got, want)
	}
}

func TestRedisWritePipelineContinuesAfterSerializationErrors(t *testing.T) {
	client := newRedisPipelineWriteTestClient(t)
	encodeErr := errors.New("cannot encode value")
	metric := redisPipelineTestMetric(
		&MetricField{Key: "p1", Value: 1},
		&MetricField{Key: "bad", Value: redisPipelineBadValue{err: encodeErr}},
		&MetricField{Key: "p2", Value: 2},
	)
	err := (&redis{realTimeWindow: 60}).Write(t.Context(), []*Metric{metric})
	if !errors.Is(err, encodeErr) {
		t.Fatalf("Write error = %v, want serialization error", err)
	}
	if !strings.Contains(err.Error(), "redis XADD:") || !strings.Contains(err.Error(), "redis HSET:") {
		t.Errorf("Write error does not identify both failed commands: %v", err)
	}
	want := [][]any{
		{"XADD", "device-1:p1", "1700000000123-*", "value", 1},
		{"XADD", "device-1:p2", "1700000000123-*", "value", 2},
		{"EXPIRE", "energy:device-1_latest", int64(60)},
	}
	if !reflect.DeepEqual(client.commands, want) {
		t.Errorf("commands = %#v, want %#v", client.commands, want)
	}
}

func TestRedisWritePipelineIsolatesNonScalarValues(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
		{name: "complex", value: complex(1, 2)},
		{name: "named scalar", value: redisPipelineNamedInt(1)},
		{name: "binary marshaler", value: redisPipelineBinaryValue("bad")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := newRedisPipelineWriteTestClient(t)
			wantErr := errors.New("single command encoding failed")
			client.directError = wantErr
			metric := redisPipelineTestMetric(
				&MetricField{Key: "p1", Value: 1},
				&MetricField{Key: "bad", Value: tt.value},
				&MetricField{Key: "p2", Value: 2},
			)
			err := (&redis{realTimeWindow: 60}).Write(t.Context(), []*Metric{metric})
			if !errors.Is(err, wantErr) {
				t.Errorf("Write error = %v, want single command error", err)
			}
			if !reflect.DeepEqual(client.batches, []int{1, 1, 1}) {
				t.Errorf("pipeline batches = %v, want each valid XADD and EXPIRE isolated from invalid commands", client.batches)
			}
			if len(client.commands) != 5 || client.calls["XADD"] != 3 || client.calls["HSET"] != 1 || client.calls["EXPIRE"] != 1 {
				t.Errorf("commands = %d, operation counts = %v; want all five attempted", len(client.commands), client.calls)
			}
		})
	}
}

func TestRedisWritePreservesSynchronousClusterOrdering(t *testing.T) {
	client := newRedisWriteTestClient(t)
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{MaxRetries: -1})
	cluster.AddHook(redisPipelineTestHook{client: client})
	client.client = cluster
	t.Cleanup(func() { _ = cluster.Close() })

	if err := (&redis{realTimeWindow: 60}).Write(t.Context(), redisWriteTestMetrics()); err != nil {
		t.Fatal(err)
	}
	if len(client.batches) != 0 {
		t.Errorf("cluster write used pipeline batches %v, which can reorder commands across shards", client.batches)
	}
	if len(client.commands) != 3 || client.commands[0][0] != "XADD" || client.commands[1][0] != "HSET" || client.commands[2][0] != "EXPIRE" {
		t.Errorf("cluster commands = %v, want XADD then HSET then EXPIRE", client.commands)
	}
}

func TestRedisWritePipelineBoundsBatchesAndContinuesAfterErrors(t *testing.T) {
	client := newRedisPipelineWriteTestClient(t)
	xaddErr := errors.New("xadd failed")
	client.errors = map[string]error{"XADD": xaddErr}
	fields := make([]*MetricField, 300)
	for i := range fields {
		fields[i] = &MetricField{Key: fmt.Sprintf("p%d", i), Value: i}
	}
	err := (&redis{realTimeWindow: 60}).Write(t.Context(), []*Metric{redisPipelineTestMetric(fields...)})
	if !errors.Is(err, xaddErr) {
		t.Errorf("Write error = %v, want XADD error", err)
	}
	if !reflect.DeepEqual(client.batches, []int{128, 128, 46}) {
		t.Errorf("pipeline batches = %v, want [128 128 46]", client.batches)
	}
	if len(client.commands) != 302 || client.calls["XADD"] != 300 || client.calls["HSET"] != 1 || client.calls["EXPIRE"] != 1 {
		t.Errorf("commands = %d, operation counts = %v; want 300 XADD, 1 HSET, 1 EXPIRE", len(client.commands), client.calls)
	}
}

func TestRedisWritePipelineReturnsExecError(t *testing.T) {
	for _, commandErr := range []error{nil, errors.New("xadd failed")} {
		t.Run(fmt.Sprintf("command_error_%v", commandErr), func(t *testing.T) {
			client := newRedisPipelineWriteTestClient(t)
			want := errors.New("pipeline transport failed")
			client.execError = want
			client.errors = map[string]error{"XADD": commandErr}
			err := (&redis{realTimeWindow: 60}).Write(t.Context(), redisWriteTestMetrics())
			if !errors.Is(err, want) {
				t.Errorf("Write error = %v, want pipeline error", err)
			}
			if commandErr != nil && !errors.Is(err, commandErr) {
				t.Errorf("Write error = %v, want command error", err)
			}
		})
	}
}

func redisPipelineTestMetric(fields ...*MetricField) *Metric {
	metric := redisWriteTestMetrics()[0]
	metric.Time = gtime.New(time.UnixMilli(1700000000123))
	metric.FieldList = fields
	return metric
}

func newRedisPipelineWriteTestClient(t *testing.T) *redisWriteTestClient {
	t.Helper()
	client := newRedisWriteTestClient(t)
	raw := goredis.NewClient(&goredis.Options{MaxRetries: -1})
	raw.AddHook(redisPipelineTestHook{client: client})
	client.client = raw
	t.Cleanup(func() { _ = raw.Close() })
	return client
}

type redisPipelineTestHook struct{ client *redisWriteTestClient }

func (h redisPipelineTestHook) DialHook(goredis.DialHook) goredis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("unexpected network connection in Redis write test")
	}
}

func (h redisPipelineTestHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return next
}

func (h redisPipelineTestHook) ProcessPipelineHook(goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(_ context.Context, commands []goredis.Cmder) error {
		h.client.batches = append(h.client.batches, len(commands))
		var firstErr error
		for _, command := range commands {
			err := h.client.record(strings.ToUpper(command.Name()), command.Args()[1:]...)
			command.SetErr(err)
			if firstErr == nil {
				firstErr = err
			}
		}
		if h.client.execError != nil {
			return h.client.execError
		}
		return firstErr
	}
}

type redisPipelineBadValue struct{ err error }

func (v redisPipelineBadValue) MarshalJSON() ([]byte, error) { return nil, v.err }

type redisPipelineNamedInt int

type redisPipelineBinaryValue string

func (v redisPipelineBinaryValue) MarshalBinary() ([]byte, error) {
	return nil, errors.New("cannot encode binary value")
}
