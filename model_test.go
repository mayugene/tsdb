package tsdb

import (
	"testing"

	"github.com/gogf/gf/v2/os/gtime"
)

func TestMetric_AddTag(t *testing.T) {
	tests := []struct {
		name     string
		metric   *Metric
		key      string
		value    string
		wantTags []*MetricTag
	}{
		{
			name:     "add tag to empty list",
			metric:   &Metric{},
			key:      "key1",
			value:    "value1",
			wantTags: []*MetricTag{{Key: "key1", Value: "value1"}},
		},
		{
			name:     "add tag with key that sorts first",
			metric:   &Metric{TagList: []*MetricTag{{Key: "z", Value: "z1"}}},
			key:      "a",
			value:    "a1",
			wantTags: []*MetricTag{{Key: "a", Value: "a1"}, {Key: "z", Value: "z1"}},
		},
		{
			name:     "add tag with key that sorts last",
			metric:   &Metric{TagList: []*MetricTag{{Key: "a", Value: "a1"}}},
			key:      "z",
			value:    "z1",
			wantTags: []*MetricTag{{Key: "a", Value: "a1"}, {Key: "z", Value: "z1"}},
		},
		{
			name:     "add tag with key that sorts in middle",
			metric:   &Metric{TagList: []*MetricTag{{Key: "a", Value: "a1"}, {Key: "z", Value: "z1"}}},
			key:      "m",
			value:    "m1",
			wantTags: []*MetricTag{{Key: "a", Value: "a1"}, {Key: "m", Value: "m1"}, {Key: "z", Value: "z1"}},
		},
		{
			name:     "update existing tag",
			metric:   &Metric{TagList: []*MetricTag{{Key: "key1", Value: "old"}}},
			key:      "key1",
			value:    "new",
			wantTags: []*MetricTag{{Key: "key1", Value: "new"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metric.AddTag(tt.key, tt.value)
			if len(tt.metric.TagList) != len(tt.wantTags) {
				t.Errorf("AddTag() got %d tags, want %d tags", len(tt.metric.TagList), len(tt.wantTags))
				return
			}
			for i, wantTag := range tt.wantTags {
				gotTag := tt.metric.TagList[i]
				if gotTag.Key != wantTag.Key || gotTag.Value != wantTag.Value {
					t.Errorf("AddTag() tag[%d] = (%s, %s), want (%s, %s)", i, gotTag.Key, gotTag.Value, wantTag.Key, wantTag.Value)
				}
			}
		})
	}
}

func TestMetric_HasTag(t *testing.T) {
	tests := []struct {
		name   string
		metric *Metric
		key    string
		want   bool
	}{
		{
			name:   "has tag in empty list",
			metric: &Metric{},
			key:    "key1",
			want:   false,
		},
		{
			name:   "has tag that exists",
			metric: &Metric{TagList: []*MetricTag{{Key: "key1", Value: "value1"}}},
			key:    "key1",
			want:   true,
		},
		{
			name:   "does not have tag",
			metric: &Metric{TagList: []*MetricTag{{Key: "key1", Value: "value1"}}},
			key:    "key2",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.metric.HasTag(tt.key); got != tt.want {
				t.Errorf("HasTag(%s) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestMetric_GetTag(t *testing.T) {
	tests := []struct {
		name      string
		metric    *Metric
		key       string
		wantValue string
		wantOk    bool
	}{
		{
			name:      "get tag from empty list",
			metric:    &Metric{},
			key:       "key1",
			wantValue: "",
			wantOk:    false,
		},
		{
			name:      "get tag that exists",
			metric:    &Metric{TagList: []*MetricTag{{Key: "key1", Value: "value1"}}},
			key:       "key1",
			wantValue: "value1",
			wantOk:    true,
		},
		{
			name:      "get tag that does not exist",
			metric:    &Metric{TagList: []*MetricTag{{Key: "key1", Value: "value1"}}},
			key:       "key2",
			wantValue: "",
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotOk := tt.metric.GetTag(tt.key)
			if gotValue != tt.wantValue {
				t.Errorf("GetTag(%s) value = %v, want %v", tt.key, gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetTag(%s) ok = %v, want %v", tt.key, gotOk, tt.wantOk)
			}
		})
	}
}

func TestMetric_RemoveTag(t *testing.T) {
	tests := []struct {
		name      string
		metric    *Metric
		key       string
		wantLen   int
		wantTags  []*MetricTag
	}{
		{
			name:     "remove tag from empty list",
			metric:   &Metric{},
			key:      "key1",
			wantLen:  0,
			wantTags: nil,
		},
		{
			name:     "remove first tag",
			metric:   &Metric{TagList: []*MetricTag{{Key: "key1", Value: "v1"}, {Key: "key2", Value: "v2"}}},
			key:      "key1",
			wantLen:  1,
			wantTags: []*MetricTag{{Key: "key2", Value: "v2"}},
		},
		{
			name:     "remove middle tag",
			metric:   &Metric{TagList: []*MetricTag{{Key: "key1", Value: "v1"}, {Key: "key2", Value: "v2"}, {Key: "key3", Value: "v3"}}},
			key:      "key2",
			wantLen:  2,
			wantTags: []*MetricTag{{Key: "key1", Value: "v1"}, {Key: "key3", Value: "v3"}},
		},
		{
			name:     "remove last tag",
			metric:   &Metric{TagList: []*MetricTag{{Key: "key1", Value: "v1"}, {Key: "key2", Value: "v2"}}},
			key:      "key2",
			wantLen:  1,
			wantTags: []*MetricTag{{Key: "key1", Value: "v1"}},
		},
		{
			name:     "remove non-existent tag",
			metric:   &Metric{TagList: []*MetricTag{{Key: "key1", Value: "v1"}}},
			key:      "key2",
			wantLen:  1,
			wantTags: []*MetricTag{{Key: "key1", Value: "v1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metric.RemoveTag(tt.key)
			if len(tt.metric.TagList) != tt.wantLen {
				t.Errorf("RemoveTag(%s) got %d tags, want %d tags", tt.key, len(tt.metric.TagList), tt.wantLen)
				return
			}
			for i, wantTag := range tt.wantTags {
				gotTag := tt.metric.TagList[i]
				if gotTag.Key != wantTag.Key || gotTag.Value != wantTag.Value {
					t.Errorf("RemoveTag(%s) tag[%d] = (%s, %s), want (%s, %s)", tt.key, i, gotTag.Key, gotTag.Value, wantTag.Key, wantTag.Value)
				}
			}
		})
	}
}

func TestMetric_AddField(t *testing.T) {
	tests := []struct {
		name       string
		metric     *Metric
		key        string
		value      any
		wantFields []*MetricField
	}{
		{
			name:   "add field to empty list",
			metric: &Metric{},
			key:    "field1",
			value:  123,
			wantFields: []*MetricField{
				{Key: "field1", Value: 123},
			},
		},
		{
			name:   "add field to non-empty list",
			metric: &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:    "field2",
			value:  456,
			wantFields: []*MetricField{
				{Key: "field1", Value: 123},
				{Key: "field2", Value: 456},
			},
		},
		{
			name:   "update existing field",
			metric: &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:    "field1",
			value:  999,
			wantFields: []*MetricField{
				{Key: "field1", Value: 999},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metric.AddField(tt.key, tt.value)
			if len(tt.metric.FieldList) != len(tt.wantFields) {
				t.Errorf("AddField() got %d fields, want %d fields", len(tt.metric.FieldList), len(tt.wantFields))
				return
			}
			for i, wantField := range tt.wantFields {
				gotField := tt.metric.FieldList[i]
				if gotField.Key != wantField.Key || gotField.Value != wantField.Value {
					t.Errorf("AddField() field[%d] = (%s, %v), want (%s, %v)", i, gotField.Key, gotField.Value, wantField.Key, wantField.Value)
				}
			}
		})
	}
}

func TestMetric_HasField(t *testing.T) {
	tests := []struct {
		name   string
		metric *Metric
		key    string
		want   bool
	}{
		{
			name:   "has field in empty list",
			metric: &Metric{},
			key:    "field1",
			want:   false,
		},
		{
			name:   "has field that exists",
			metric: &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:    "field1",
			want:   true,
		},
		{
			name:   "does not have field",
			metric: &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:    "field2",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.metric.HasField(tt.key); got != tt.want {
				t.Errorf("HasField(%s) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestMetric_GetField(t *testing.T) {
	tests := []struct {
		name      string
		metric    *Metric
		key       string
		wantValue any
		wantOk    bool
	}{
		{
			name:      "get field from empty list",
			metric:    &Metric{},
			key:       "field1",
			wantValue: nil,
			wantOk:    false,
		},
		{
			name:      "get field that exists",
			metric:    &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:       "field1",
			wantValue: 123,
			wantOk:    true,
		},
		{
			name:      "get field that does not exist",
			metric:    &Metric{FieldList: []*MetricField{{Key: "field1", Value: 123}}},
			key:       "field2",
			wantValue: nil,
			wantOk:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotOk := tt.metric.GetField(tt.key)
			if gotValue != tt.wantValue {
				t.Errorf("GetField(%s) value = %v, want %v", tt.key, gotValue, tt.wantValue)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetField(%s) ok = %v, want %v", tt.key, gotOk, tt.wantOk)
			}
		})
	}
}

func TestMetric_RemoveField(t *testing.T) {
	tests := []struct {
		name        string
		metric      *Metric
		key         string
		wantLen     int
		wantFields  []*MetricField
	}{
		{
			name:       "remove field from empty list",
			metric:     &Metric{},
			key:        "field1",
			wantLen:    0,
			wantFields: nil,
		},
		{
			name:       "remove first field",
			metric:     &Metric{FieldList: []*MetricField{{Key: "field1", Value: 1}, {Key: "field2", Value: 2}}},
			key:        "field1",
			wantLen:    1,
			wantFields: []*MetricField{{Key: "field2", Value: 2}},
		},
		{
			name:       "remove middle field",
			metric:     &Metric{FieldList: []*MetricField{{Key: "field1", Value: 1}, {Key: "field2", Value: 2}, {Key: "field3", Value: 3}}},
			key:        "field2",
			wantLen:    2,
			wantFields: []*MetricField{{Key: "field1", Value: 1}, {Key: "field3", Value: 3}},
		},
		{
			name:       "remove last field",
			metric:     &Metric{FieldList: []*MetricField{{Key: "field1", Value: 1}, {Key: "field2", Value: 2}}},
			key:        "field2",
			wantLen:    1,
			wantFields: []*MetricField{{Key: "field1", Value: 1}},
		},
		{
			name:       "remove non-existent field",
			metric:     &Metric{FieldList: []*MetricField{{Key: "field1", Value: 1}}},
			key:        "field2",
			wantLen:    1,
			wantFields: []*MetricField{{Key: "field1", Value: 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.metric.RemoveField(tt.key)
			if len(tt.metric.FieldList) != tt.wantLen {
				t.Errorf("RemoveField(%s) got %d fields, want %d fields", tt.key, len(tt.metric.FieldList), tt.wantLen)
				return
			}
			for i, wantField := range tt.wantFields {
				gotField := tt.metric.FieldList[i]
				if gotField.Key != wantField.Key || gotField.Value != wantField.Value {
					t.Errorf("RemoveField(%s) field[%d] = (%s, %v), want (%s, %v)", tt.key, i, gotField.Key, gotField.Value, wantField.Key, wantField.Value)
				}
			}
		})
	}
}

func TestMetric_AddTag_AscendingOrder(t *testing.T) {
	// Test that tags are maintained in ascending key order
	m := &Metric{}
	m.AddTag("z", "z")
	m.AddTag("a", "a")
	m.AddTag("m", "m")
	m.AddTag("b", "b")

	expectedOrder := []string{"a", "b", "m", "z"}
	for i, expected := range expectedOrder {
		if m.TagList[i].Key != expected {
			t.Errorf("AddTag ascending order: tag[%d] = %s, want %s", i, m.TagList[i].Key, expected)
		}
	}
}

func TestMetric_Integration(t *testing.T) {
	// Test a full workflow
	m := &Metric{
		Name:      "test_metric",
		Time:      gtime.New(),
		TagList:   []*MetricTag{},
		FieldList: []*MetricField{},
	}

	// Add tags
	m.AddTag("device", "d1")
	m.AddTag("project", "p1")

	if !m.HasTag("device") {
		t.Error("Expected to have tag 'device'")
	}
	if !m.HasTag("project") {
		t.Error("Expected to have tag 'project'")
	}

	deviceVal, ok := m.GetTag("device")
	if !ok || deviceVal != "d1" {
		t.Errorf("GetTag('device') = (%s, %v), want ('d1', true)", deviceVal, ok)
	}

	// Add fields
	m.AddField("temperature", 25.5)
	m.AddField("humidity", 60.0)

	if !m.HasField("temperature") {
		t.Error("Expected to have field 'temperature'")
	}

	tempVal, ok := m.GetField("temperature")
	if !ok || tempVal != 25.5 {
		t.Errorf("GetField('temperature') = (%v, %v), want (25.5, true)", tempVal, ok)
	}

	// Remove tag
	m.RemoveTag("project")
	if m.HasTag("project") {
		t.Error("Expected tag 'project' to be removed")
	}

	// Remove field
	m.RemoveField("humidity")
	if m.HasField("humidity") {
		t.Error("Expected field 'humidity' to be removed")
	}
}