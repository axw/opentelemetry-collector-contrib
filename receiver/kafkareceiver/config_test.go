// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr error
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			expected: &Config{
				ClientConfig: configkafka.ClientConfig{
					Brokers:                              []string{"foo:123", "bar:456"},
					ResolveCanonicalBootstrapServersOnly: true,
					ClientID:                             "otel-collector",
					Authentication: configkafka.AuthenticationConfig{
						TLS: &configtls.ClientConfig{
							Config: configtls.Config{
								CAFile:   "ca.pem",
								CertFile: "cert.pem",
								KeyFile:  "key.pem",
							},
						},
					},
					Metadata: configkafka.MetadataConfig{
						Full: true,
						Retry: configkafka.MetadataRetryConfig{
							Max:     10,
							Backoff: time.Second * 5,
						},
					},
				},
				ConsumerConfig: configkafka.ConsumerConfig{
					GroupID:           "otel-collector",
					InitialOffset:     "latest",
					SessionTimeout:    10 * time.Second,
					HeartbeatInterval: 3 * time.Second,
					AutoCommit: configkafka.AutoCommitConfig{
						Enable:   true,
						Interval: 1 * time.Second,
					},
					MinFetchSize:     1,
					DefaultFetchSize: 1048576,
					MaxFetchSize:     0,
				},
				Topic:    "spans",
				Encoding: "otlp_proto",
				ErrorBackOff: configretry.BackOffConfig{
					Enabled: false,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "logs"),
			expected: &Config{
				ClientConfig: configkafka.ClientConfig{
					Brokers:  []string{"coffee:123", "foobar:456"},
					ClientID: "otel-collector",
					Authentication: configkafka.AuthenticationConfig{
						TLS: &configtls.ClientConfig{
							Config: configtls.Config{
								CAFile:   "ca.pem",
								CertFile: "cert.pem",
								KeyFile:  "key.pem",
							},
						},
					},
					Metadata: configkafka.MetadataConfig{
						Full: true,
						Retry: configkafka.MetadataRetryConfig{
							Max:     10,
							Backoff: time.Second * 5,
						},
					},
				},
				ConsumerConfig: configkafka.ConsumerConfig{
					GroupID:           "otel-collector",
					InitialOffset:     "earliest",
					SessionTimeout:    45 * time.Second,
					HeartbeatInterval: 15 * time.Second,
					AutoCommit: configkafka.AutoCommitConfig{
						Enable:   true,
						Interval: 1 * time.Second,
					},
					MinFetchSize:     1,
					DefaultFetchSize: 1048576,
					MaxFetchSize:     0,
				},
				Topic:    "logs",
				Encoding: "direct",
				ErrorBackOff: configretry.BackOffConfig{
					Enabled:         true,
					InitialInterval: 1 * time.Second,
					MaxInterval:     10 * time.Second,
					MaxElapsedTime:  1 * time.Minute,
					Multiplier:      1.5,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
