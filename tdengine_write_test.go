package tsdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/gogf/gf/v2/net/gclient"
	"github.com/gogf/gf/v2/os/gtime"
)

func TestTdengineWriteReturnsErrorForHTTPFailure(t *testing.T) {
	const responseBody = "test-sql-payload"
	for _, status := range []int{http.StatusMultipleChoices, http.StatusBadRequest, http.StatusInternalServerError} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client := newTdengineWriteTestClient(func(*http.Request) (*http.Response, error) {
				return tdengineWriteTestResponse(status, responseBody), nil
			})
			err := client.Write(t.Context(), tdengineWriteTestMetrics())
			if err == nil || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Fatalf("Write error = %v, want HTTP status %d", err, status)
			}
			if strings.Contains(err.Error(), responseBody) {
				t.Fatalf("Write error exposes the HTTP response body: %q", err)
			}
		})
	}
}

func TestTdengineWriteReturnsContextError(t *testing.T) {
	client := newTdengineWriteTestClient(func(request *http.Request) (*http.Response, error) {
		return nil, request.Context().Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.Write(ctx, tdengineWriteTestMetrics()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write error = %v, want context.Canceled", err)
	}
}

func TestTdengineWriteAcceptsSuccessfulHTTPStatus(t *testing.T) {
	client := newTdengineWriteTestClient(func(*http.Request) (*http.Response, error) {
		return tdengineWriteTestResponse(http.StatusNoContent, ""), nil
	})

	if err := client.Write(t.Context(), tdengineWriteTestMetrics()); err != nil {
		t.Fatalf("Write returned error for HTTP 204: %v", err)
	}
}

func TestTdengineWriteReturnsTransportError(t *testing.T) {
	transportErr := errors.New("transport failed")
	client := newTdengineWriteTestClient(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	})

	err := client.Write(t.Context(), tdengineWriteTestMetrics())
	if !errors.Is(err, transportErr) {
		t.Fatalf("Write error = %v, want wrapped transport error %v", err, transportErr)
	}
}

func newTdengineWriteTestClient(roundTrip func(*http.Request) (*http.Response, error)) *tdengine {
	httpClient := gclient.New()
	httpClient.Transport = tdengineWriteRoundTripFunc(roundTrip)

	return &tdengine{
		client:   httpClient,
		writeUri: "http://tsdb.test/write",
	}
}

func tdengineWriteTestResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

type tdengineWriteRoundTripFunc func(*http.Request) (*http.Response, error)

func (f tdengineWriteRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func tdengineWriteTestMetrics() []*Metric {
	return []*Metric{{
		Name: "energy",
		TagList: []*MetricTag{{
			Key:   "device_id",
			Value: "device-1",
		}},
		FieldList: []*MetricField{{
			Key:   "value",
			Value: 1,
		}},
		Time: gtime.Now(),
	}}
}
