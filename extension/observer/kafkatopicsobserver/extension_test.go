// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkatopicsobserver

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer/kafkatopicsobserver/internal/metadata"
)

type MockClusterAdmin struct {
	sarama.ClusterAdmin
	mock.Mock
}

func (m *MockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

func (m *MockClusterAdmin) Close() error {
	return m.Called().Error(0)
}

func TestCollectEndpointsDefaultConfig(t *testing.T) {
	factory := NewFactory()
	mockAdmin := &MockClusterAdmin{}
	mockAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{"abc": {}, "def": {}}, nil)
	mockAdmin.On("Close").Return(nil).Once()
	// Override the createKafkaClusterAdmin function to return the mock admin
	originalCreateKafkaClusterAdmin := createKafkaClusterAdmin
	createKafkaClusterAdmin = func(_ context.Context, _ Config) (sarama.ClusterAdmin, error) {
		return mockAdmin, nil
	}
	defer func() {
		createKafkaClusterAdmin = originalCreateKafkaClusterAdmin
	}()

	ext, err := newObserver(zap.NewNop(), factory.CreateDefaultConfig().(*Config))
	require.NoError(t, err)
	require.NotNil(t, ext)
	defer func() {
		err := ext.Shutdown(context.Background())
		assert.NoError(t, err)
	}()

	var notifier testNotifier
	obvs := ext.(*kafkaTopicsObserver)
	obvs.ListAndWatch(&notifier)

	assert.Eventually(t, func() bool {
		return len(notifier.getOps()) == 1
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, []notifyOp{{
		op: "add",
		endpoints: []observer.Endpoint{{
			ID:      "abc",
			Target:  "abc",
			Details: &observer.KafkaTopic{},
		}, {
			ID:      "def",
			Target:  "def",
			Details: &observer.KafkaTopic{},
		}},
	}}, notifier.getOps())
}

func TestCollectEndpointsAllConfigSettings(t *testing.T) {
	mockAdmin := &MockClusterAdmin{}

	// During first check new topics matching the regex are detected
	mockAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{
		"topic1": {},
		"topic2": {},
	}, nil).Once()

	// During the second check only one new topic which doesn't match a regex is detected
	mockAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{
		"topic1": {},
		"topic2": {},
		"topics": {},
	}, nil).Once()

	// During the third check one topic matching a regex is detected
	mockAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{
		"topic1": {},
		"topics": {},
	}, nil)
	mockAdmin.On("Close").Return(nil).Once()

	// Override the createKafkaClusterAdmin function to return the mock admin
	originalCreateKafkaClusterAdmin := createKafkaClusterAdmin
	createKafkaClusterAdmin = func(_ context.Context, _ Config) (sarama.ClusterAdmin, error) {
		return mockAdmin, nil
	}
	defer func() {
		createKafkaClusterAdmin = originalCreateKafkaClusterAdmin
	}()

	extAllSettings := loadConfig(t, component.NewIDWithName(metadata.Type, "all_settings"))
	ext, err := newObserver(zap.NewNop(), extAllSettings)
	require.NoError(t, err)
	require.NotNil(t, ext)

	err = ext.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() {
		err := ext.Shutdown(context.Background())
		assert.NoError(t, err)
	}()

	var notifier testNotifier
	obvs := ext.(*kafkaTopicsObserver)
	obvs.ListAndWatch(&notifier)

	assert.Eventually(t, func() bool {
		return len(notifier.getOps()) == 2
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, []notifyOp{{
		op: "add",
		endpoints: []observer.Endpoint{{
			ID:      "topic1",
			Target:  "topic1",
			Details: &observer.KafkaTopic{},
		}, {
			ID:      "topic2",
			Target:  "topic2",
			Details: &observer.KafkaTopic{},
		}},
	}, {
		op: "remove",
		endpoints: []observer.Endpoint{{
			ID:      "topic2",
			Target:  "topic2",
			Details: &observer.KafkaTopic{},
		}},
	}}, notifier.getOps())
}

type testNotifier struct {
	mu  sync.RWMutex
	ops []notifyOp
}

func (n *testNotifier) ID() observer.NotifyID {
	return observer.NotifyID(fmt.Sprintf("%p", n))
}

func (n *testNotifier) getOps() []notifyOp {
	n.mu.RLock()
	defer n.mu.RUnlock()
	copied := make([]notifyOp, len(n.ops))
	copy(copied, n.ops)
	return copied
}

func (n *testNotifier) OnAdd(added []observer.Endpoint) {
	n.addOp("add", added)
}

func (n *testNotifier) OnRemove(removed []observer.Endpoint) {
	n.addOp("remove", removed)
}

func (n *testNotifier) OnChange(changed []observer.Endpoint) {
	n.addOp("remove", changed)
}

func (n *testNotifier) addOp(op string, endpoints []observer.Endpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ops = append(n.ops, notifyOp{
		op: op,
		endpoints: slices.SortedFunc(
			slices.Values(endpoints),
			func(a, b observer.Endpoint) int {
				return cmp.Compare(a.ID, b.ID)
			},
		),
	})
}

type notifyOp struct {
	op        string // add, remove, change
	endpoints []observer.Endpoint
}
