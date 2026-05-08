// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awslogsencodingextension

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/constants"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/metadata"
	vpcflowlog "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/unmarshaler/vpc-flow-log"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr string
	}{
		{
			id:          component.NewIDWithName(metadata.Type, ""),
			expectedErr: fmt.Sprintf("format unspecified, expected one of %q", supportedLogFormats),
		},
		{
			id: component.NewIDWithName(metadata.Type, "cloudwatch"),
			expected: &Config{
				Format: constants.FormatCloudWatchLogsSubscriptionFilter,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "text_vpcflow"),
			expected: &Config{
				Format: constants.FormatVPCFlowLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "text_vpc_flow_log"),
			expected: &Config{
				Format: constants.FormatVPCFlowLogV1,
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "parquet_vpcflow"),
			expected: &Config{
				Format: constants.FormatVPCFlowLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatParquet,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "invalid_vpcflow"),
			expectedErr: fmt.Sprintf(
				`unsupported file format "invalid" for VPC flow log, expected one of %q`,
				supportedVPCFlowLogFileFormat,
			),
		},
		{
			id: component.NewIDWithName(metadata.Type, "invalid_vpc_flow_log"),
			expectedErr: fmt.Sprintf(
				`unsupported file format "invalid" for VPC flow log, expected one of %q`,
				supportedVPCFlowLogFileFormat,
			),
		},
		{
			id: component.NewIDWithName(metadata.Type, "s3access"),
			expected: &Config{
				Format: constants.FormatS3AccessLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "waf"),
			expected: &Config{
				Format: constants.FormatWAFLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "cloudtrail"),
			expected: &Config{
				Format: constants.FormatCloudTrailLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "elbaccess"),
			expected: &Config{
				Format: constants.FormatELBAccessLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "networkfirewall"),
			expected: &Config{
				Format: constants.FormatNetworkFirewallLog,
				VPCFlowLogConfig: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
				VPCFlowLogConfigV1: vpcflowlog.Config{
					FileFormat: constants.FileFormatPlainText,
				},
			},
		},
	}

	for _, tt := range tests {
		name := strings.ReplaceAll(tt.id.String(), "/", "_")
		t.Run(name, func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, cfg)
			}
		})
	}
}

func TestSubEncodingValidation(t *testing.T) {
	t.Parallel()

	jsonID := component.MustNewIDWithName("jsonlogencoding", "in-cw")

	tests := map[string]struct {
		cfg         func() *Config
		expectedErr string
	}{
		"sub_encodings_only_for_cloudwatch_format": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatVPCFlowLog
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{
					LogGroup: "*",
					Encoding: &jsonID,
				}}
				return c
			},
			expectedErr: `cloudwatch.sub_encodings is only valid when format is "cloudwatch"`,
		},
		"missing_encoding": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatCloudWatchLogsSubscriptionFilter
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{LogGroup: "*"}}
				return c
			},
			expectedErr: "cloudwatch.sub_encodings[0]: encoding is required",
		},
		"unsupported_payload": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatCloudWatchLogsSubscriptionFilter
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{
					LogGroup: "*",
					Encoding: &jsonID,
					Payload:  "weird",
				}}
				return c
			},
			expectedErr: `cloudwatch.sub_encodings[0]: unsupported payload "weird"`,
		},
		"valid_message_payload": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatCloudWatchLogsSubscriptionFilter
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{
					LogGroup: "/aws/json/*",
					Encoding: &jsonID,
					Payload:  SubEncodingPayloadMessage,
				}}
				return c
			},
		},
		"valid_envelope_payload": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatCloudWatchLogsSubscriptionFilter
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{
					LogGroup: "/aws/vpc/*",
					Encoding: &jsonID,
					Payload:  SubEncodingPayloadEnvelope,
				}}
				return c
			},
		},
		"empty_payload_defaults_message": {
			cfg: func() *Config {
				c := createDefaultConfig().(*Config)
				c.Format = constants.FormatCloudWatchLogsSubscriptionFilter
				c.CloudWatchConfig.SubEncodings = []SubEncoding{{
					LogGroup: "*",
					Encoding: &jsonID,
				}}
				return c
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := xconfmap.Validate(tt.cfg())
			if tt.expectedErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}
