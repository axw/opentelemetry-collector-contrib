package configkafka

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

func TestClientConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "client_config.yaml"))
	require.NoError(t, err)

	tests := map[string]struct {
		expected    ClientConfig
		expectedErr string
	}{
		"": {
			expected: NewDefaultClientConfig(),
		},
		"full": {
			expected: ClientConfig{
				Brokers:                              []string{"foo:123", "bar:456"},
				ResolveCanonicalBootstrapServersOnly: true,
				ClientID:                             "vip",
				ProtocolVersion:                      "1.2.3",
				Authentication: AuthenticationConfig{
					TLS: &configtls.ClientConfig{
						Config: configtls.Config{
							CAFile:   "ca.pem",
							CertFile: "cert.pem",
							KeyFile:  "key.pem",
						},
					},
				},
				Metadata: MetadataConfig{
					Full: false,
					Retry: MetadataRetryConfig{
						Max:     10,
						Backoff: 5 * time.Second,
					},
				},
			},
		},
		"sasl_aws_msk_iam": {
			expected: func() ClientConfig {
				cfg := NewDefaultClientConfig()
				cfg.Authentication.SASL = &SASLConfig{
					Mechanism: "AWS_MSK_IAM",
				}
				return cfg
			}(),
		},
		"sasl_plain": {
			expected: func() ClientConfig {
				cfg := NewDefaultClientConfig()
				cfg.Authentication.SASL = &SASLConfig{
					Mechanism: "PLAIN",
					Username:  "abc",
					Password:  "def",
				}
				return cfg
			}(),
		},

		// Invalid configurations
		"brokers_required": {
			expectedErr: "brokers must be specified",
		},
		"sasl_invalid_mechanism": {
			expectedErr: "auth::sasl: mechanism should be one of 'PLAIN', 'AWS_MSK_IAM', 'AWS_MSK_IAM_OAUTHBEARER', 'SCRAM-SHA-256' or 'SCRAM-SHA-512'. configured value FANCY",
		},
		"sasl_invalid_version": {
			expectedErr: "auth::sasl: version has to be either 0 or 1. configured value -1",
		},
		"sasl_plain_username_required": {
			expectedErr: "auth::sasl: username is required",
		},
		"sasl_plain_password_required": {
			expectedErr: "auth::sasl: password is required",
		},
		// TODO validate protocol version? sarama checks it I think?
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := NewDefaultClientConfig()

			sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("kafka"), name).String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, cfg)
			}
		})
	}
}

func TestConsumerConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "consumer_config.yaml"))
	require.NoError(t, err)

	tests := map[string]struct {
		expected    ConsumerConfig
		expectedErr string
	}{
		"": {
			expected: NewDefaultConsumerConfig(),
		},
		"full": {
			expected: ConsumerConfig{
				SessionTimeout:    5 * time.Second,
				HeartbeatInterval: 2 * time.Second,
				GroupID:           "throng",
				InitialOffset:     "earliest",
				AutoCommit: AutoCommitConfig{
					Enable:   false,
					Interval: 10 * time.Minute,
				},
				MinFetchSize:     10,
				DefaultFetchSize: 1024,
				MaxFetchSize:     4096,
			},
		},

		// Invalid configurations
		"invalid_initial_offset": {
			expectedErr: "initial_offset should be one of 'latest' or 'earliest'. configured value middle",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := NewDefaultConsumerConfig()

			sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("kafka"), name).String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, cfg)
			}
		})
	}
}
