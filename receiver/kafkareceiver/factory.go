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

	"github.com/open-telemetry/opentelemetry-collector/component"
	"github.com/open-telemetry/opentelemetry-collector/config/configerror"
	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
	"github.com/open-telemetry/opentelemetry-collector/consumer"
)

const (
	typeStr = "kafka"

	defaultBroker   = "localhost:9092"
	defaultTopic    = "otlp_spans"
	defaultGroupID  = "otel-collector"
	defaultClientID = "otel-collector"
	defaultEncoding = encodingOTLPProto
)

// Factory is the factory for the Kafka receiver.
type Factory struct{}

var _ component.ReceiverFactory = (*Factory)(nil)

// Type returns the receiver type.
func (f *Factory) Type() configmodels.Type {
	return typeStr
}

// CustomUnmarshaler returns nil; standard mapstructure unmarshaling is used.
func (f *Factory) CustomUnmarshaler() component.CustomUnmarshaler {
	return nil
}

// CreateDefaultConfig creates the default Kafka receiver configuration.
func (f *Factory) CreateDefaultConfig() configmodels.Receiver {
	return &Config{
		ReceiverSettings: configmodels.ReceiverSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
		Brokers:  []string{defaultBroker},
		Topic:    defaultTopic,
		GroupID:  defaultGroupID,
		ClientID: defaultClientID,
		Encoding: defaultEncoding,
	}
}

// CreateTraceReceiver creates a Kafka trace receiver.
func (f *Factory) CreateTraceReceiver(
	ctx context.Context,
	params component.ReceiverCreateParams,
	cfg configmodels.Receiver,
	nextConsumer consumer.TraceConsumer,
) (component.TraceReceiver, error) {
	rCfg := cfg.(*Config)
	return newKafkaReceiver(rCfg, nextConsumer, params.Logger)
}

// CreateMetricsReceiver is not supported.
func (f *Factory) CreateMetricsReceiver(
	_ context.Context,
	_ component.ReceiverCreateParams,
	_ configmodels.Receiver,
	_ consumer.MetricsConsumer,
) (component.MetricsReceiver, error) {
	return nil, configerror.ErrDataTypeIsNotSupported
}
