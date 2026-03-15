// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkareceiver

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/open-telemetry/opentelemetry-collector/consumer/pdata"
	"github.com/open-telemetry/opentelemetry-collector/exporter/exportertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// newKafkaReceiver
// ---------------------------------------------------------------------------

func TestNewKafkaReceiver_NilConsumer(t *testing.T) {
	cfg := createTestConfig()
	_, err := newKafkaReceiver(cfg, nil, zap.NewNop())
	require.Error(t, err)
}

func TestNewKafkaReceiver_UnknownEncoding(t *testing.T) {
	cfg := createTestConfig()
	cfg.Encoding = "unknown_format"
	_, err := newKafkaReceiver(cfg, exportertest.NewNopTraceExporter(), zap.NewNop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown_format")
}

func TestNewKafkaReceiver_ValidConfig(t *testing.T) {
	for _, enc := range []string{encodingOTLPProto, encodingJaegerProto} {
		t.Run(enc, func(t *testing.T) {
			cfg := createTestConfig()
			cfg.Encoding = enc
			r, err := newKafkaReceiver(cfg, exportertest.NewNopTraceExporter(), zap.NewNop())
			require.NoError(t, err)
			assert.NotNil(t, r)
		})
	}
}

// ---------------------------------------------------------------------------
// injectKafkaAttributes
// ---------------------------------------------------------------------------

func TestInjectKafkaAttributes_AllFields(t *testing.T) {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	traces.ResourceSpans().At(0).Resource().InitEmpty()

	msg := &sarama.ConsumerMessage{
		Topic:     "my-topic",
		Partition: 3,
		Offset:    42,
		Key:       []byte("tenant-123"),
	}
	injectKafkaAttributes(traces, msg, "my-consumer-group", "my-client-id")

	attrs := traces.ResourceSpans().At(0).Resource().Attributes()

	assertStringAttr(t, attrs, attrMessagingSystem, "kafka")
	assertStringAttr(t, attrs, attrMessagingDestination, "my-topic")
	assertIntAttr(t, attrs, attrKafkaPartition, 3)
	assertIntAttr(t, attrs, attrKafkaOffset, 42)
	assertStringAttr(t, attrs, attrKafkaConsumerGroup, "my-consumer-group")
	assertStringAttr(t, attrs, attrKafkaClientID, "my-client-id")
	assertStringAttr(t, attrs, attrKafkaMessageKey, "tenant-123")
}

func TestInjectKafkaAttributes_EmptyKeyNotSet(t *testing.T) {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	traces.ResourceSpans().At(0).Resource().InitEmpty()

	msg := &sarama.ConsumerMessage{
		Topic:     "my-topic",
		Partition: 0,
		Offset:    0,
		Key:       nil, // no key
	}
	injectKafkaAttributes(traces, msg, "grp", "client")

	attrs := traces.ResourceSpans().At(0).Resource().Attributes()
	_, exists := attrs.Get(attrKafkaMessageKey)
	assert.False(t, exists, "message_key attribute must not be set for empty keys")
}

func TestInjectKafkaAttributes_MultipleResourceSpans(t *testing.T) {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(3)
	for i := 0; i < 3; i++ {
		traces.ResourceSpans().At(i).Resource().InitEmpty()
	}

	msg := &sarama.ConsumerMessage{
		Topic:     "events",
		Partition: 1,
		Offset:    100,
	}
	injectKafkaAttributes(traces, msg, "grp", "client")

	// All three ResourceSpans must carry the same Kafka attributes.
	for i := 0; i < 3; i++ {
		attrs := traces.ResourceSpans().At(i).Resource().Attributes()
		assertStringAttr(t, attrs, attrMessagingDestination, "events")
		assertIntAttr(t, attrs, attrKafkaPartition, 1)
		assertIntAttr(t, attrs, attrKafkaOffset, 100)
	}
}

// TestInjectKafkaAttributes_DoesNotOverwriteExistingServiceName verifies that
// the injected attributes do NOT clobber existing resource attributes set by
// the instrumented service (e.g. service.name).
func TestInjectKafkaAttributes_DoesNotOverwriteExistingServiceName(t *testing.T) {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	rs := traces.ResourceSpans().At(0)
	rs.Resource().InitEmpty()
	rs.Resource().Attributes().InsertString("service.name", "my-service")

	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 0}
	injectKafkaAttributes(traces, msg, "g", "c")

	// service.name must still be present.
	v, ok := traces.ResourceSpans().At(0).Resource().Attributes().Get("service.name")
	require.True(t, ok)
	assert.Equal(t, "my-service", v.StringVal())
}

// ---------------------------------------------------------------------------
// unmarshalerForEncoding
// ---------------------------------------------------------------------------

func TestUnmarshalerForEncoding(t *testing.T) {
	tests := []struct {
		encoding string
		wantType string
		wantErr  bool
	}{
		{encoding: encodingOTLPProto, wantType: encodingOTLPProto},
		{encoding: "", wantType: encodingOTLPProto}, // empty defaults to OTLP
		{encoding: encodingJaegerProto, wantType: encodingJaegerProto},
		{encoding: "bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.encoding, func(t *testing.T) {
			u, err := unmarshalerForEncoding(tt.encoding)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, u.Encoding())
		})
	}
}

// ---------------------------------------------------------------------------
// defaultConfig via Factory
// ---------------------------------------------------------------------------

func TestDefaultConfig(t *testing.T) {
	f := &Factory{}
	cfg := f.CreateDefaultConfig().(*Config)
	assert.Equal(t, []string{defaultBroker}, cfg.Brokers)
	assert.Equal(t, defaultTopic, cfg.Topic)
	assert.Equal(t, defaultGroupID, cfg.GroupID)
	assert.Equal(t, defaultClientID, cfg.ClientID)
	assert.Equal(t, defaultEncoding, cfg.Encoding)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func createTestConfig() *Config {
	return &Config{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		GroupID:  "test-group",
		ClientID: "test-client",
		Encoding: encodingJaegerProto,
	}
}

func assertStringAttr(t *testing.T, attrs pdata.AttributeMap, key, want string) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.Truef(t, ok, "attribute %q not found", key)
	assert.Equalf(t, want, v.StringVal(), "attribute %q value mismatch", key)
}

func assertIntAttr(t *testing.T, attrs pdata.AttributeMap, key string, want int64) {
	t.Helper()
	v, ok := attrs.Get(key)
	require.Truef(t, ok, "attribute %q not found", key)
	assert.Equalf(t, want, v.IntVal(), "attribute %q value mismatch", key)
}
