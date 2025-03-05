// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkatopicsobserver // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/kafkatopicsobserver"
import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
)

var (
	_ extension.Extension      = (*kafkaTopicsObserver)(nil)
	_ observer.EndpointsLister = (*kafkaTopicsObserver)(nil)
	_ observer.Observable      = (*kafkaTopicsObserver)(nil)
)

type kafkaTopicsObserver struct {
	*observer.EndpointsWatcher
	logger           *zap.Logger
	config           *Config
	cancelKafkaAdmin func()
	kafkaAdmin       sarama.ClusterAdmin
	topicRegexp      *regexp.Regexp

	mu     sync.Mutex
	topics []string
}

func newObserver(logger *zap.Logger, config *Config) (extension.Extension, error) {
	topicRegexp, err := regexp.Compile(config.TopicRegex)
	if err != nil {
		return nil, fmt.Errorf("failed to compile topic regex: %w", err)
	}

	kCtx, cancel := context.WithCancel(context.Background())
	admin, err := createKafkaClusterAdmin(kCtx, *config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("could not create kafka cluster admin: %w", err)
	}

	d := &kafkaTopicsObserver{
		logger:           logger,
		config:           config,
		cancelKafkaAdmin: cancel,
		kafkaAdmin:       admin,
		topicRegexp:      topicRegexp,
	}
	d.EndpointsWatcher = observer.NewEndpointsWatcher(d, config.TopicsSyncInterval, logger)
	return d, nil
}

func (k *kafkaTopicsObserver) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (k *kafkaTopicsObserver) Shutdown(_ context.Context) error {
	k.StopListAndWatch()
	k.cancelKafkaAdmin()
	err := k.kafkaAdmin.Close()
	if err != nil {
		return fmt.Errorf("failed to close kafka cluster admin client: %w", err)
	}
	k.logger.Info("kafka cluster admin client closed")
	return nil
}

type kafkaTopicsEndpointsLister struct {
	mu     sync.Mutex
	topics []string
}

func (k *kafkaTopicsObserver) ListEndpoints() []observer.Endpoint {
	topics, err := k.listMatchingTopics()
	if err != nil {
		k.logger.Error("failed to list topics, using cached list", zap.Error(err))
		// Use the previously cached list of topics.
		k.mu.Lock()
		topics = k.topics
		k.mu.Unlock()
	} else {
		// Cache the new list of topics.
		k.mu.Lock()
		k.topics = topics
		k.mu.Unlock()
	}
	endpoints := make([]observer.Endpoint, len(topics))
	for i, topic := range topics {
		details := &observer.KafkaTopic{}
		endpoints[i] = observer.Endpoint{
			ID:      observer.EndpointID(topic),
			Target:  topic,
			Details: details,
		}
	}
	return endpoints
}

func (k *kafkaTopicsObserver) listMatchingTopics() ([]string, error) {
	// Collect all available topics
	topics, err := k.kafkaAdmin.ListTopics()
	if err != nil {
		return nil, err
	}

	// Filter topics
	var matchingTopics []string
	for topic := range topics {
		if k.topicRegexp.MatchString(topic) {
			matchingTopics = append(matchingTopics, topic)
		}
	}
	return matchingTopics, nil
}

var createKafkaClusterAdmin = func(ctx context.Context, config Config) (sarama.ClusterAdmin, error) {
	saramaConfig := sarama.NewConfig()
	var err error
	if config.ResolveCanonicalBootstrapServersOnly {
		saramaConfig.Net.ResolveCanonicalBootstrapServers = true
	}
	if config.ProtocolVersion != "" {
		if saramaConfig.Version, err = sarama.ParseKafkaVersion(config.ProtocolVersion); err != nil {
			return nil, err
		}
	}
	if err := kafka.ConfigureSaramaAuthentication(ctx, config.Authentication, saramaConfig); err != nil {
		return nil, err
	}
	return sarama.NewClusterAdmin(config.Brokers, saramaConfig)
}
