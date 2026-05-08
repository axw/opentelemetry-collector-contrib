// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awslogsencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension"

import (
	"errors"
	"fmt"
	"path"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/constants"
	vpcflowlog "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/unmarshaler/vpc-flow-log"
)

var _ xconfmap.Validator = (*Config)(nil)

const (
	// SubEncodingPayloadMessage routes a CloudWatch payload by passing each
	// log event's message bytes individually to the routed encoding extension.
	SubEncodingPayloadMessage = "message"
	// SubEncodingPayloadEnvelope routes a CloudWatch payload by passing the
	// full CloudWatch JSON record bytes to the routed encoding extension,
	// which is responsible for iterating its log events.
	SubEncodingPayloadEnvelope = "envelope"
)

var (
	supportedLogFormats = []string{
		// New format values
		constants.FormatCloudWatchLogsSubscriptionFilter,
		constants.FormatVPCFlowLog,
		constants.FormatS3AccessLog,
		constants.FormatWAFLog,
		constants.FormatCloudTrailLog,
		constants.FormatELBAccessLog,
		constants.FormatNetworkFirewallLog,
		// Legacy format values (for backward compatibility)
		constants.FormatCloudWatchLogsSubscriptionFilterV1,
		constants.FormatVPCFlowLogV1,
		constants.FormatS3AccessLogV1,
		constants.FormatWAFLogV1,
		constants.FormatCloudTrailLogV1,
		constants.FormatELBAccessLogV1,
	}
	supportedVPCFlowLogFileFormat = []string{constants.FileFormatPlainText, constants.FileFormatParquet}
	supportedSubEncodingPayloads  = []string{SubEncodingPayloadMessage, SubEncodingPayloadEnvelope}
)

type Config struct {
	// Format defines the AWS logs format.
	//
	// Current valid values are:
	// - cloudwatch
	// - vpcflow
	// - s3access
	// - waf
	// - cloudtrail
	// - elbaccess
	// - networkfirewall
	//
	Format string `mapstructure:"format"`

	// CloudWatchConfig holds settings specific to the cloudwatch format.
	CloudWatchConfig CloudWatchConfig `mapstructure:"cloudwatch"`

	VPCFlowLogConfig vpcflowlog.Config `mapstructure:"vpcflow"`
	// Deprecated: use VPCFlowLogConfig instead. It will be removed in v0.138.0
	VPCFlowLogConfigV1 vpcflowlog.Config `mapstructure:"vpc_flow_log"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// CloudWatchConfig configures decoding of CloudWatch Logs subscription filter
// payloads.
type CloudWatchConfig struct {
	// SubEncodings routes CloudWatch payloads to other encoding extensions
	// based on the payload's log group and/or log stream. Rules are evaluated
	// in order and the first match wins. Payloads with no matching rule are
	// decoded with this extension's default behavior (each event's message
	// becomes the log body).
	SubEncodings []SubEncoding `mapstructure:"sub_encodings"`
}

// SubEncoding describes a single routing rule.
type SubEncoding struct {
	// LogGroup is a glob pattern (path.Match syntax) matched against the
	// CloudWatch payload's logGroup. An empty value matches anything.
	LogGroup string `mapstructure:"log_group"`
	// LogStream is a glob pattern (path.Match syntax) matched against the
	// CloudWatch payload's logStream. An empty value matches anything.
	LogStream string `mapstructure:"log_stream"`
	// Encoding is the component ID of the encoding extension that will
	// decode matching payloads. The referenced extension must implement
	// plog.Unmarshaler.
	Encoding *component.ID `mapstructure:"encoding"`
	// Payload selects what is fed to the routed encoding:
	//   - "message" (default): each event's message bytes are passed
	//     individually; CloudWatch resource attributes are added to the
	//     resulting logs and the CloudWatch event timestamp is preserved.
	//   - "envelope": the original CloudWatch record bytes are passed
	//     once; the routed extension owns the result.
	Payload string `mapstructure:"payload"`
}

func (cfg *Config) Validate() error {
	var errs []error

	switch cfg.Format {
	case "":
		errs = append(errs, fmt.Errorf("format unspecified, expected one of %q", supportedLogFormats))
	case constants.FormatCloudWatchLogsSubscriptionFilter: // valid
	case constants.FormatCloudWatchLogsSubscriptionFilterV1: // valid
	case constants.FormatVPCFlowLogV1: // valid
	case constants.FormatVPCFlowLog: // valid
	case constants.FormatS3AccessLogV1: // valid
	case constants.FormatS3AccessLog: // valid
	case constants.FormatWAFLogV1: // valid
	case constants.FormatWAFLog: // valid
	case constants.FormatCloudTrailLogV1: // valid
	case constants.FormatCloudTrailLog: // valid
	case constants.FormatELBAccessLogV1: // valid
	case constants.FormatELBAccessLog: // valid
	case constants.FormatNetworkFirewallLog: // valid
	default:
		errs = append(errs, fmt.Errorf("unsupported format %q, expected one of %q", cfg.Format, supportedLogFormats))
	}

	switch cfg.VPCFlowLogConfig.FileFormat {
	case constants.FileFormatParquet: // valid
	case constants.FileFormatPlainText: // valid
	default:
		errs = append(errs, fmt.Errorf(
			"unsupported file format %q for VPC flow log, expected one of %q",
			cfg.VPCFlowLogConfig.FileFormat,
			supportedVPCFlowLogFileFormat,
		))
	}

	// to be deprecated in v0.138.0
	switch cfg.VPCFlowLogConfigV1.FileFormat {
	case constants.FileFormatParquet: // valid
	case constants.FileFormatPlainText: // valid
	default:
		errs = append(errs, fmt.Errorf(
			"unsupported file format %q for VPC flow log, expected one of %q",
			cfg.VPCFlowLogConfigV1.FileFormat,
			supportedVPCFlowLogFileFormat,
		))
	}

	if len(cfg.CloudWatchConfig.SubEncodings) > 0 {
		isCloudWatch := cfg.Format == constants.FormatCloudWatchLogsSubscriptionFilter ||
			cfg.Format == constants.FormatCloudWatchLogsSubscriptionFilterV1
		if !isCloudWatch {
			errs = append(errs, fmt.Errorf(
				"cloudwatch.sub_encodings is only valid when format is %q",
				constants.FormatCloudWatchLogsSubscriptionFilter,
			))
		}
		for i, rule := range cfg.CloudWatchConfig.SubEncodings {
			if err := rule.validate(); err != nil {
				errs = append(errs, fmt.Errorf("cloudwatch.sub_encodings[%d]: %w", i, err))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (r SubEncoding) validate() error {
	var errs []error
	if r.Encoding == nil {
		errs = append(errs, errors.New("encoding is required"))
	}
	switch r.Payload {
	case "", SubEncodingPayloadMessage, SubEncodingPayloadEnvelope:
	default:
		errs = append(errs, fmt.Errorf(
			"unsupported payload %q, expected one of %q",
			r.Payload, supportedSubEncodingPayloads,
		))
	}
	if r.LogGroup != "" {
		if _, err := path.Match(r.LogGroup, ""); err != nil {
			errs = append(errs, fmt.Errorf("invalid log_group glob %q: %w", r.LogGroup, err))
		}
	}
	if r.LogStream != "" {
		if _, err := path.Match(r.LogStream, ""); err != nil {
			errs = append(errs, fmt.Errorf("invalid log_stream glob %q: %w", r.LogStream, err))
		}
	}
	return errors.Join(errs...)
}
