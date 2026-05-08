// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package subscriptionfilter

import (
	"bytes"
	"errors"
	"testing"

	gojson "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// recordingUnmarshaler is a stub plog.Unmarshaler that records the bytes it
// received and returns canned plog.Logs. It is the test seam for routed
// encoding extensions.
type recordingUnmarshaler struct {
	calls [][]byte
	build func(buf []byte) plog.Logs
	err   error
}

func (r *recordingUnmarshaler) UnmarshalLogs(buf []byte) (plog.Logs, error) {
	cp := make([]byte, len(buf))
	copy(cp, buf)
	r.calls = append(r.calls, cp)
	if r.err != nil {
		return plog.Logs{}, r.err
	}
	if r.build != nil {
		return r.build(cp), nil
	}
	return plog.NewLogs(), nil
}

// makeRecord serializes a CloudWatch DATA_MESSAGE record with the given
// log group / stream / events.
func makeRecord(t *testing.T, owner, logGroup, logStream string, events ...cloudwatchLogsLogEvent) []byte {
	t.Helper()
	rec := cloudwatchLogsData{
		Owner:       owner,
		LogGroup:    logGroup,
		LogStream:   logStream,
		MessageType: "DATA_MESSAGE",
		LogEvents:   events,
	}
	buf, err := gojson.Marshal(rec)
	require.NoError(t, err)
	return buf
}

func TestRouting_FallbackWhenNoRulesMatch(t *testing.T) {
	t.Parallel()

	stub := &recordingUnmarshaler{}
	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{
		LogGroup: "/never/match/*",
		Encoding: stub,
	}})

	rec := makeRecord(t, "111", "/myapp/json", "stream-1", cloudwatchLogsLogEvent{
		ID:        "id-1",
		Timestamp: 1700000000000,
		Message:   "hello",
	})

	logs, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.NoError(t, err)
	require.Empty(t, stub.calls, "routed encoding should not be invoked when no rule matches")
	require.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)
	require.Equal(t, 1, rl.ScopeLogs().Len())
	sl := rl.ScopeLogs().At(0)
	require.Equal(t, 1, sl.LogRecords().Len())
	require.Equal(t, "hello", sl.LogRecords().At(0).Body().Str())
}

func TestRouting_GlobMatch(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ruleGroup, ruleStream string
		group, stream         string
		shouldMatch           bool
	}{
		"group_glob_match":         {ruleGroup: "/aws/json/*", group: "/aws/json/foo", stream: "s", shouldMatch: true},
		"group_glob_no_match":      {ruleGroup: "/aws/json/*", group: "/other/foo", stream: "s", shouldMatch: false},
		"stream_glob_match":        {ruleStream: "stream-?", group: "g", stream: "stream-1", shouldMatch: true},
		"stream_glob_no_match":     {ruleStream: "stream-?", group: "g", stream: "stream-22", shouldMatch: false},
		"both_glob_match":          {ruleGroup: "/g/*", ruleStream: "s/*", group: "/g/x", stream: "s/y", shouldMatch: true},
		"both_glob_one_no_match":   {ruleGroup: "/g/*", ruleStream: "s/*", group: "/g/x", stream: "other", shouldMatch: false},
		"empty_pattern_matches":    {ruleGroup: "", ruleStream: "", group: "g", stream: "s", shouldMatch: true},
		"empty_group_only_matches": {ruleStream: "stream-1", group: "anything", stream: "stream-1", shouldMatch: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			stub := &recordingUnmarshaler{}
			u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
			u.SetRoutes([]Route{{
				LogGroup:  tt.ruleGroup,
				LogStream: tt.ruleStream,
				Encoding:  stub,
			}})
			rec := makeRecord(t, "111", tt.group, tt.stream, cloudwatchLogsLogEvent{
				ID:        "1",
				Timestamp: 1700000000000,
				Message:   "x",
			})
			_, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
			require.NoError(t, err)
			if tt.shouldMatch {
				require.Len(t, stub.calls, 1, "routed encoding should have been invoked")
			} else {
				require.Empty(t, stub.calls, "routed encoding should not have been invoked")
			}
		})
	}
}

func TestRouting_FirstMatchWins(t *testing.T) {
	t.Parallel()

	first := &recordingUnmarshaler{}
	second := &recordingUnmarshaler{}

	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{
		{LogGroup: "/aws/*", Encoding: first},
		{LogGroup: "/aws/specific", Encoding: second},
	})

	rec := makeRecord(t, "111", "/aws/specific", "s",
		cloudwatchLogsLogEvent{ID: "1", Timestamp: 1700000000000, Message: "x"})
	_, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.NoError(t, err)
	require.Len(t, first.calls, 1)
	require.Empty(t, second.calls, "later rules should not run when an earlier one matches")
}

func TestRouting_MessageMode_PerEventDispatchAndAttrs(t *testing.T) {
	t.Parallel()

	// Stub returns one ResourceLogs/ScopeLogs/LogRecord per call, with
	// the body set to the input bytes so we can verify dispatch.
	stub := &recordingUnmarshaler{
		build: func(buf []byte) plog.Logs {
			out := plog.NewLogs()
			rl := out.ResourceLogs().AppendEmpty()
			sl := rl.ScopeLogs().AppendEmpty()
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(string(buf))
			return out
		},
	}

	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{
		LogGroup: "/myapp/*",
		Encoding: stub,
		Payload:  PayloadMessage,
	}})

	rec := makeRecord(t, "111111111111", "/myapp/json", "stream-A",
		cloudwatchLogsLogEvent{ID: "a", Timestamp: 1_700_000_000_000, Message: "msg-a"},
		cloudwatchLogsLogEvent{ID: "b", Timestamp: 1_700_000_000_001, Message: "msg-b"},
	)

	logs, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.NoError(t, err)

	// Stub called once per event.
	require.Len(t, stub.calls, 2)
	require.Equal(t, "msg-a", string(stub.calls[0]))
	require.Equal(t, "msg-b", string(stub.calls[1]))

	// One ResourceLogs per event in the merged output.
	require.Equal(t, 2, logs.ResourceLogs().Len())
	for i, expectedTS := range []pcommon.Timestamp{
		pcommon.Timestamp(1_700_000_000_000 * 1_000_000),
		pcommon.Timestamp(1_700_000_000_001 * 1_000_000),
	} {
		rl := logs.ResourceLogs().At(i)
		attrs := rl.Resource().Attributes()
		val, ok := attrs.Get(string(conventions.CloudProviderKey))
		require.True(t, ok)
		require.Equal(t, conventions.CloudProviderAWS.Value.AsString(), val.Str())
		val, ok = attrs.Get(string(conventions.CloudAccountIDKey))
		require.True(t, ok)
		require.Equal(t, "111111111111", val.Str())
		val, ok = attrs.Get(string(conventions.AWSLogGroupNamesKey))
		require.True(t, ok)
		require.Equal(t, "/myapp/json", val.Slice().At(0).Str())
		val, ok = attrs.Get(string(conventions.AWSLogStreamNamesKey))
		require.True(t, ok)
		require.Equal(t, "stream-A", val.Slice().At(0).Str())

		require.Equal(t, 1, rl.ScopeLogs().Len())
		lr := rl.ScopeLogs().At(0).LogRecords().At(0)
		require.Equal(t, expectedTS, lr.Timestamp())
	}
}

func TestRouting_MessageMode_PreservesRoutedTimestamp(t *testing.T) {
	t.Parallel()

	preset := pcommon.Timestamp(42)
	stub := &recordingUnmarshaler{
		build: func(buf []byte) plog.Logs {
			out := plog.NewLogs()
			lr := out.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.Body().SetStr(string(buf))
			lr.SetTimestamp(preset)
			return out
		},
	}
	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{LogGroup: "*", Encoding: stub}})

	rec := makeRecord(t, "1", "g", "s",
		cloudwatchLogsLogEvent{ID: "a", Timestamp: 1_700_000_000_000, Message: "msg"},
	)
	logs, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.NoError(t, err)
	require.Equal(t, 1, logs.ResourceLogs().Len())
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, preset, lr.Timestamp(), "non-zero timestamps from the routed encoding must not be overwritten")
}

func TestRouting_EnvelopeMode(t *testing.T) {
	t.Parallel()

	// Envelope mode passes the original record bytes once. The stub will
	// emit one log record carrying the bytes so we can verify.
	stub := &recordingUnmarshaler{
		build: func(buf []byte) plog.Logs {
			out := plog.NewLogs()
			rl := out.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("routed.attr", "set-by-routed")
			lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.Body().SetStr(string(buf))
			return out
		},
	}

	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{
		LogGroup: "/aws/vpc/*",
		Encoding: stub,
		Payload:  PayloadEnvelope,
	}})

	rec := makeRecord(t, "111", "/aws/vpc/foo", "s-1",
		cloudwatchLogsLogEvent{ID: "1", Timestamp: 1_700_000_000_000, Message: "ev1"},
		cloudwatchLogsLogEvent{ID: "2", Timestamp: 1_700_000_000_001, Message: "ev2"},
	)

	logs, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.NoError(t, err)

	// Stub called exactly once with the full envelope.
	require.Len(t, stub.calls, 1)
	require.JSONEq(t, string(rec), string(stub.calls[0]))

	// Output is the routed extension's output verbatim — no CloudWatch
	// resource attributes added on top.
	require.Equal(t, 1, logs.ResourceLogs().Len())
	rl := logs.ResourceLogs().At(0)
	val, ok := rl.Resource().Attributes().Get("routed.attr")
	require.True(t, ok)
	require.Equal(t, "set-by-routed", val.Str())
	_, hasGroup := rl.Resource().Attributes().Get(string(conventions.AWSLogGroupNamesKey))
	require.False(t, hasGroup, "envelope mode trusts the routed extension; CW attrs must not be merged")
}

// TestRouting_EnvelopeMode_SkipsEventArrayDecode proves the optimization:
// when an envelope-mode route matches, the unmarshaler must not require the
// logEvents array to be a valid cloudwatchLogsLogEvent shape — only the
// envelope fields (messageType / owner / logGroup / logStream) are inspected.
func TestRouting_EnvelopeMode_SkipsEventArrayDecode(t *testing.T) {
	t.Parallel()

	// logEvents is a JSON shape that cannot decode into
	// []cloudwatchLogsLogEvent (timestamp is a string, not an int64) but is
	// still valid JSON syntactically, so the header-only decode succeeds.
	raw := []byte(`{
		"owner": "111",
		"logGroup": "/aws/vpc/foo",
		"logStream": "s",
		"messageType": "DATA_MESSAGE",
		"logEvents": [{"id":"1","timestamp":"not-an-int","message":"x"}]
	}`)

	stub := &recordingUnmarshaler{
		build: func(buf []byte) plog.Logs {
			out := plog.NewLogs()
			out.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(string(buf))
			return out
		},
	}
	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{
		LogGroup: "/aws/vpc/*",
		Encoding: stub,
		Payload:  PayloadEnvelope,
	}})

	logs, err := u.UnmarshalAWSLogs(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Len(t, stub.calls, 1)
	require.Equal(t, 1, logs.ResourceLogs().Len())
}

func TestRouting_RoutedError(t *testing.T) {
	t.Parallel()

	stub := &recordingUnmarshaler{err: errors.New("boom")}
	u := NewSubscriptionFilterUnmarshaler(component.BuildInfo{})
	u.SetRoutes([]Route{{LogGroup: "*", Encoding: stub}})

	rec := makeRecord(t, "1", "g", "s",
		cloudwatchLogsLogEvent{ID: "a", Timestamp: 1, Message: "x"},
	)
	_, err := u.UnmarshalAWSLogs(bytes.NewReader(rec))
	require.Error(t, err)
	require.Contains(t, err.Error(), "routed encoding")
	require.Contains(t, err.Error(), "boom")
}
