// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type topicNamer[T any] interface {
	// TopicName returns the topic name for the given data (plog.Logs, ptrace.Traces, etc.).
	TopicName(ctx context.Context, data T) (string, error)
}

type ottlTopicNamer[T any] struct {
	expr *ottl.ValueExpression[T]
}

func (n *ottlTopicNamer[T]) TopicName(ctx context.Context, data T) (string, error) {
	return "", nil
}
