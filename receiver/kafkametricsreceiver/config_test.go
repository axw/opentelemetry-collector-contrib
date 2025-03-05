// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkametricsreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/configkafka"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	assert.Equal(t, &Config{
		ControllerConfig: scraperhelper.NewDefaultControllerConfig(),
		ClientConfig: configkafka.ClientConfig{
			Brokers:         []string{"10.10.10.10:9092"},
			ProtocolVersion: "2.0.0",
			Authentication: configkafka.AuthenticationConfig{
				TLS: &configtls.ClientConfig{
					Config: configtls.Config{
						CAFile:   "ca.pem",
						CertFile: "cert.pem",
						KeyFile:  "key.pem",
					},
				},
			},
			ClientID: "otel-collector",
			Metadata: configkafka.MetadataConfig{
				Full: true,
				Retry: configkafka.MetadataRetryConfig{
					Max:     3,
					Backoff: 250 * time.Millisecond,
				},
			},
		},
		ClusterAlias:         "kafka-test",
		TopicMatch:           "test_\\w+",
		GroupMatch:           "test_\\w+",
		RefreshFrequency:     1,
		Scrapers:             []string{"brokers", "topics", "consumers"},
		MetricsBuilderConfig: metadata.DefaultMetricsBuilderConfig(),
	}, cfg)
}
