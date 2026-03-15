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
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/open-telemetry/opentelemetry-collector/component"
	"github.com/open-telemetry/opentelemetry-collector/component/componenterror"
	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/consumer/pdata"
	"github.com/open-telemetry/opentelemetry-collector/obsreport"
	"go.uber.org/zap"
)

const (
	// Encoding constants for supported message payload formats.
	encodingOTLPProto   = "otlp_proto"
	encodingJaegerProto = "jaeger_proto"
	encodingJaegerJSON  = "jaeger_json"

	// Resource attribute keys follow OpenTelemetry semantic conventions for
	// messaging systems (https://opentelemetry.io/docs/reference/specification/
	// trace/semantic_conventions/messaging/).
	//
	// These attributes are added to every ResourceSpans produced from a Kafka
	// message and are therefore accessible via OTTL expressions such as:
	//   resource.attributes["messaging.kafka.partition"]
	//   resource.attributes["messaging.destination"]

	// attrMessagingSystem identifies the messaging system. Always "kafka".
	attrMessagingSystem = "messaging.system"

	// attrMessagingDestination is the Kafka topic the message was consumed from.
	// OTTL path: resource.attributes["messaging.destination"]
	attrMessagingDestination = "messaging.destination"

	// attrKafkaPartition is the Kafka partition the message was consumed from.
	// OTTL path: resource.attributes["messaging.kafka.partition"]
	attrKafkaPartition = "messaging.kafka.partition"

	// attrKafkaOffset is the offset of the message within the partition.
	// Useful for deduplication, replay, and auditing.
	// OTTL path: resource.attributes["messaging.kafka.offset"]
	attrKafkaOffset = "messaging.kafka.offset"

	// attrKafkaConsumerGroup is the consumer group that consumed the message.
	// Useful when multiple consumer groups read from the same topic.
	// OTTL path: resource.attributes["messaging.kafka.consumer_group"]
	attrKafkaConsumerGroup = "messaging.kafka.consumer_group"

	// attrKafkaClientID is the Kafka client identifier.
	// OTTL path: resource.attributes["messaging.kafka.client_id"]
	attrKafkaClientID = "messaging.kafka.client_id"

	// attrKafkaMessageKey is the Kafka message key, when non-empty.
	// Keys are often used for partitioning and carry business identity (e.g.
	// tenant ID, trace ID). Skipped when the key is empty.
	// OTTL path: resource.attributes["messaging.kafka.message_key"]
	attrKafkaMessageKey = "messaging.kafka.message_key"
)

// TracesUnmarshaler deserialises a Kafka message payload into pdata.Traces.
// Implementations are selected by the Config.Encoding field.
type TracesUnmarshaler interface {
	// Unmarshal parses a raw Kafka message value and returns the decoded
	// traces.  The encoding name is used only in error messages.
	Unmarshal(data []byte) (pdata.Traces, error)

	// Encoding returns the canonical name for this format (e.g.
	// "jaeger_proto").  Must match the value accepted in Config.Encoding.
	Encoding() string
}

// kafkaReceiver consumes a single Kafka topic using a consumer group and
// forwards decoded trace data – enriched with Kafka message metadata – to
// the configured next consumer.
type kafkaReceiver struct {
	config       *Config
	nextConsumer consumer.TraceConsumer
	unmarshaler  TracesUnmarshaler
	logger       *zap.Logger

	cancelFunc context.CancelFunc
	goroutines sync.WaitGroup
	startOnce  sync.Once
	stopOnce   sync.Once
}

var _ component.TraceReceiver = (*kafkaReceiver)(nil)

// newKafkaReceiver creates a kafkaReceiver from configuration.
func newKafkaReceiver(
	cfg *Config,
	nextConsumer consumer.TraceConsumer,
	logger *zap.Logger,
) (*kafkaReceiver, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	u, err := unmarshalerForEncoding(cfg.Encoding)
	if err != nil {
		return nil, err
	}

	return &kafkaReceiver{
		config:       cfg,
		nextConsumer: nextConsumer,
		unmarshaler:  u,
		logger:       logger,
	}, nil
}

// Start begins consuming Kafka messages.
func (r *kafkaReceiver) Start(ctx context.Context, host component.Host) error {
	var startErr error
	r.startOnce.Do(func() {
		saramaConfig := sarama.NewConfig()
		saramaConfig.ClientID = r.config.ClientID
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

		cg, err := sarama.NewConsumerGroup(r.config.Brokers, r.config.GroupID, saramaConfig)
		if err != nil {
			startErr = fmt.Errorf("failed to create Kafka consumer group: %w", err)
			return
		}

		consumeCtx, cancel := context.WithCancel(context.Background())
		r.cancelFunc = cancel

		r.goroutines.Add(1)
		go func() {
			defer r.goroutines.Done()
			defer cg.Close()

			handler := &consumerGroupHandler{
				receiver: r,
				ready:    make(chan struct{}),
			}

			for {
				if err := cg.Consume(consumeCtx, []string{r.config.Topic}, handler); err != nil {
					if consumeCtx.Err() != nil {
						// Receiver is shutting down; exit cleanly.
						return
					}
					r.logger.Error("Kafka consumer group error", zap.Error(err))
					host.ReportFatalError(err)
					return
				}
				if consumeCtx.Err() != nil {
					return
				}
				// Reset after a rebalance so the handler is reusable.
				handler.ready = make(chan struct{})
			}
		}()
	})
	return startErr
}

// Shutdown stops the Kafka consumer and waits for the consuming goroutine.
func (r *kafkaReceiver) Shutdown(context.Context) error {
	var shutdownErr error
	r.stopOnce.Do(func() {
		if r.cancelFunc != nil {
			r.cancelFunc()
		}
		r.goroutines.Wait()
	})
	return shutdownErr
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.  One instance
// is created per consumer-group session (i.e. between rebalances).
type consumerGroupHandler struct {
	receiver *kafkaReceiver
	ready    chan struct{}
}

// Setup is called at the start of every new session.
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

// Cleanup is called at the end of every session.
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from a single partition claim.  For each
// message it:
//
//  1. Decodes the payload according to Config.Encoding.
//  2. Injects Kafka message metadata as resource attributes on every
//     ResourceSpans in the resulting pdata.Traces.
//  3. Forwards the enriched traces to the next consumer.
//  4. Marks the message offset as committed.
func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	r := h.receiver
	transport := "kafka"
	receiverName := r.config.Name()

	for msg := range claim.Messages() {
		ctx := obsreport.ReceiverContext(session.Context(), receiverName, transport, "")
		ctx = obsreport.StartTraceDataReceiveOp(ctx, receiverName, transport)

		traces, err := r.unmarshaler.Unmarshal(msg.Value)
		if err != nil {
			r.logger.Error(
				"Failed to unmarshal Kafka message",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			obsreport.EndTraceDataReceiveOp(ctx, r.unmarshaler.Encoding(), 0, err)
			// Mark the offset even on decode failure so the receiver does not
			// re-process a permanently malformed message.
			session.MarkMessage(msg, "")
			continue
		}

		injectKafkaAttributes(traces, msg, r.config.GroupID, r.config.ClientID)

		err = r.nextConsumer.ConsumeTraces(ctx, traces)
		obsreport.EndTraceDataReceiveOp(ctx, r.unmarshaler.Encoding(), traces.SpanCount(), err)
		if err != nil {
			r.logger.Error(
				"Failed to forward Kafka traces to next consumer",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			// Do not mark the offset; the next run will retry.
			continue
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// injectKafkaAttributes adds Kafka message metadata as resource attributes to
// every ResourceSpans contained in traces.
//
// The following attributes are always set:
//   - messaging.system           = "kafka"
//   - messaging.destination      = topic name
//   - messaging.kafka.partition  = partition id (int)
//   - messaging.kafka.offset     = message offset (int)
//   - messaging.kafka.consumer_group = consumer group id
//   - messaging.kafka.client_id  = client id
//
// The following attribute is set only when the message key is non-empty:
//   - messaging.kafka.message_key = message key (string)
//
// All attributes are accessible via OTTL, for example:
//
//	resource.attributes["messaging.kafka.partition"]
//	resource.attributes["messaging.destination"]
func injectKafkaAttributes(
	traces pdata.Traces,
	msg *sarama.ConsumerMessage,
	consumerGroup string,
	clientID string,
) {
	rss := traces.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if rs.Resource().IsNil() {
			rs.Resource().InitEmpty()
		}
		attrs := rs.Resource().Attributes()
		attrs.UpsertString(attrMessagingSystem, "kafka")
		attrs.UpsertString(attrMessagingDestination, msg.Topic)
		attrs.UpsertInt(attrKafkaPartition, int64(msg.Partition))
		attrs.UpsertInt(attrKafkaOffset, msg.Offset)
		attrs.UpsertString(attrKafkaConsumerGroup, consumerGroup)
		attrs.UpsertString(attrKafkaClientID, clientID)
		if len(msg.Key) > 0 {
			attrs.UpsertString(attrKafkaMessageKey, string(msg.Key))
		}
	}
}

// unmarshalerForEncoding returns the TracesUnmarshaler for the given encoding
// name, or an error if the encoding is not supported.
func unmarshalerForEncoding(encoding string) (TracesUnmarshaler, error) {
	switch encoding {
	case encodingJaegerProto:
		return &jaegerProtoUnmarshaler{}, nil
	case encodingOTLPProto, "":
		return &otlpProtoUnmarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q; supported values are %q, %q",
			encoding, encodingOTLPProto, encodingJaegerProto)
	}
}
