// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		option   func(conf *Config)
		expected component.Config
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			option: func(_ *Config) {
				// intentionally left blank so we use default config
			},
			expected: &Config{
				TimeoutSettings: exporterhelper.TimeoutConfig{
					Timeout: 10 * time.Second,
				},
				BackOffConfig: configretry.BackOffConfig{
					Enabled:             true,
					InitialInterval:     10 * time.Second,
					MaxInterval:         1 * time.Minute,
					MaxElapsedTime:      10 * time.Minute,
					RandomizationFactor: backoff.DefaultRandomizationFactor,
					Multiplier:          backoff.DefaultMultiplier,
				},
				QueueSettings: exporterhelper.QueueConfig{
					Enabled:      true,
					NumConsumers: 2,
					QueueSize:    10,
				},
				ClientConfig: configkafka.ClientConfig{
					Brokers:  []string{"foo:123", "bar:456"},
					ClientID: "test_client_id",
					Authentication: configkafka.AuthenticationConfig{
						PlainText: &configkafka.PlainTextConfig{
							Username: "jdoe",
							Password: "pass",
						},
					},
					Metadata: configkafka.MetadataConfig{
						Full: false,
						Retry: configkafka.MetadataRetryConfig{
							Max:     15,
							Backoff: 250 * time.Millisecond,
						},
					},
				},
				Topic:                                "spans",
				Encoding:                             "otlp_proto",
				PartitionTracesByID:                  true,
				PartitionMetricsByResourceAttributes: true,
				PartitionLogsByResourceAttributes:    true,
				Producer: configkafka.ProducerConfig{
					MaxMessageBytes: 10000000,
					RequiredAcks:    configkafka.WaitForAll,
					Compression:     "none",
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, ""),
			option: func(conf *Config) {
				conf.Authentication = configkafka.AuthenticationConfig{
					SASL: &configkafka.SASLConfig{
						Username:  "jdoe",
						Password:  "pass",
						Mechanism: "PLAIN",
						Version:   0,
					},
				}
			},
			expected: &Config{
				TimeoutSettings: exporterhelper.TimeoutConfig{
					Timeout: 10 * time.Second,
				},
				BackOffConfig: configretry.BackOffConfig{
					Enabled:             true,
					InitialInterval:     10 * time.Second,
					MaxInterval:         1 * time.Minute,
					MaxElapsedTime:      10 * time.Minute,
					RandomizationFactor: backoff.DefaultRandomizationFactor,
					Multiplier:          backoff.DefaultMultiplier,
				},
				QueueSettings: exporterhelper.QueueConfig{
					Enabled:      true,
					NumConsumers: 2,
					QueueSize:    10,
				},
				ClientConfig: configkafka.ClientConfig{
					Brokers:  []string{"foo:123", "bar:456"},
					ClientID: "test_client_id",
					Authentication: configkafka.AuthenticationConfig{
						PlainText: &configkafka.PlainTextConfig{
							Username: "jdoe",
							Password: "pass",
						},
						SASL: &configkafka.SASLConfig{
							Username:  "jdoe",
							Password:  "pass",
							Mechanism: "PLAIN",
							Version:   0,
						},
					},
					Metadata: configkafka.MetadataConfig{
						Full: false,
						Retry: configkafka.MetadataRetryConfig{
							Max:     15,
							Backoff: 250 * time.Millisecond,
						},
					},
				},
				Topic:                                "spans",
				Encoding:                             "otlp_proto",
				PartitionTracesByID:                  true,
				PartitionMetricsByResourceAttributes: true,
				PartitionLogsByResourceAttributes:    true,
				Producer: configkafka.ProducerConfig{
					MaxMessageBytes: 10000000,
					RequiredAcks:    configkafka.WaitForAll,
					Compression:     "none",
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, ""),
			option: func(conf *Config) {
				conf.ResolveCanonicalBootstrapServersOnly = true
			},
			expected: &Config{
				TimeoutSettings: exporterhelper.TimeoutConfig{
					Timeout: 10 * time.Second,
				},
				BackOffConfig: configretry.BackOffConfig{
					Enabled:             true,
					InitialInterval:     10 * time.Second,
					MaxInterval:         1 * time.Minute,
					MaxElapsedTime:      10 * time.Minute,
					RandomizationFactor: backoff.DefaultRandomizationFactor,
					Multiplier:          backoff.DefaultMultiplier,
				},
				QueueSettings: exporterhelper.QueueConfig{
					Enabled:      true,
					NumConsumers: 2,
					QueueSize:    10,
				},
				ClientConfig: configkafka.ClientConfig{
					Brokers:                              []string{"foo:123", "bar:456"},
					ClientID:                             "test_client_id",
					ResolveCanonicalBootstrapServersOnly: true,
					Authentication: configkafka.AuthenticationConfig{
						PlainText: &configkafka.PlainTextConfig{
							Username: "jdoe",
							Password: "pass",
						},
					},
					Metadata: configkafka.MetadataConfig{
						Full: false,
						Retry: configkafka.MetadataRetryConfig{
							Max:     15,
							Backoff: 250 * time.Millisecond,
						},
					},
				},
				Topic:                                "spans",
				Encoding:                             "otlp_proto",
				PartitionTracesByID:                  true,
				PartitionMetricsByResourceAttributes: true,
				PartitionLogsByResourceAttributes:    true,
				Producer: configkafka.ProducerConfig{
					MaxMessageBytes: 10000000,
					RequiredAcks:    configkafka.WaitForAll,
					Compression:     "none",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cfg := applyConfigOption(tt.option)

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
