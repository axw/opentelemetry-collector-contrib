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
	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
)

// Config defines configuration for the Kafka receiver.
type Config struct {
	configmodels.ReceiverSettings `mapstructure:",squash"`

	// Brokers is the list of Kafka broker addresses (host:port).
	Brokers []string `mapstructure:"brokers"`

	// Topic is the Kafka topic to consume from.
	Topic string `mapstructure:"topic"`

	// GroupID is the consumer group ID. All receivers sharing the same GroupID
	// will jointly consume partitions from the topic.
	GroupID string `mapstructure:"group_id"`

	// ClientID is the Kafka client identifier sent to brokers. Used in logs and
	// is exposed as the messaging.kafka.client_id resource attribute.
	ClientID string `mapstructure:"client_id"`

	// Encoding is the message payload format. Supported values:
	//   "otlp_proto"   – OTLP serialised as protobuf (default)
	//   "jaeger_proto" – Jaeger serialised as protobuf
	//   "jaeger_json"  – Jaeger serialised as JSON
	Encoding string `mapstructure:"encoding"`
}
