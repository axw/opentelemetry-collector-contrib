// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafka // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"
)

// NewSaramaClusterAdminClient returns a new Kafka cluster admin client with the given configuration.
func NewSaramaClusterAdminClient(ctx context.Context, config configkafka.ClientConfig) (sarama.ClusterAdmin, error) {
	saramaConfig, err := NewSaramaClientConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return sarama.NewClusterAdmin(config.Brokers, saramaConfig)
}

// NewSaramaConsumerGroup returns a new Kafka consumer group with the given configuration.
func NewSaramaConsumerGroup(
	ctx context.Context,
	clientConfig configkafka.ClientConfig,
	consumerConfig configkafka.ConsumerConfig,
) (sarama.ConsumerGroup, error) {
	saramaConfig, err := NewSaramaClientConfig(ctx, clientConfig)
	if err != nil {
		return nil, err
	}
	saramaConfig.Consumer.Group.Session.Timeout = consumerConfig.SessionTimeout
	saramaConfig.Consumer.Group.Heartbeat.Interval = consumerConfig.HeartbeatInterval
	saramaConfig.Consumer.Fetch.Min = consumerConfig.MinFetchSize
	saramaConfig.Consumer.Fetch.Default = consumerConfig.DefaultFetchSize
	saramaConfig.Consumer.Fetch.Max = consumerConfig.MaxFetchSize
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = consumerConfig.AutoCommit.Enable
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = consumerConfig.AutoCommit.Interval
	saramaConfig.Consumer.Offsets.Initial, err = toSaramaInitialOffset(consumerConfig.InitialOffset)
	if err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(clientConfig.Brokers, consumerConfig.GroupID, saramaConfig)
}

// NewSaramaClientConfig returns a Sarama client config, based on the given config.
func NewSaramaClientConfig(ctx context.Context, config configkafka.ClientConfig) (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()
	if config.ResolveCanonicalBootstrapServersOnly {
		saramaConfig.Net.ResolveCanonicalBootstrapServers = true
	}
	if config.ProtocolVersion != "" {
		var err error
		if saramaConfig.Version, err = sarama.ParseKafkaVersion(config.ProtocolVersion); err != nil {
			return nil, err
		}
	}
	if err := ConfigureSaramaAuthentication(ctx, config.Authentication, saramaConfig); err != nil {
		return nil, err
	}
	return saramaConfig, nil
}

func toSaramaInitialOffset(initialOffset string) (int64, error) {
	switch initialOffset {
	case configkafka.EarliestOffset:
		return sarama.OffsetOldest, nil
	case configkafka.LatestOffset, "":
		return sarama.OffsetNewest, nil
	default:
		return 0, fmt.Errorf(
			"invalid initial offset %q, expected %q or %q",
			initialOffset,
			configkafka.EarliestOffset,
			configkafka.LatestOffset,
		)
	}
}
