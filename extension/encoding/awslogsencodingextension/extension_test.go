// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awslogsencodingextension

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/constants"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/metadata"
	subscriptionfilter "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/unmarshaler/subscription-filter"
)

func TestNew_CloudWatchLogsSubscriptionFilter(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatCloudWatchLogsSubscriptionFilter}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "cloudwatch" format`)
}

func TestNew_CloudWatchLogsSubscriptionFilterV1(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatCloudWatchLogsSubscriptionFilterV1}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "cloudwatch" format`)
}

func TestNew_CloudTrailLog(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatCloudTrailLog}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "cloudtrail" format`)
}

func TestNew_CloudTrailLogV1(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatCloudTrailLogV1}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "cloudtrail" format`)
}

func TestNew_VPCFlowLog(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatVPCFlowLog
	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("some test input"))
	require.ErrorContains(t, err, "failed to read first line of VPC logs from S3")
}

func TestNew_VPCFlowLogV1(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatVPCFlowLogV1
	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)
}

func TestNew_S3AccessLog(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatS3AccessLog}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "s3access" format`)
}

func TestNew_S3AccessLogV1(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatS3AccessLogV1
	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)
}

func TestNew_WAFLog(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatWAFLog}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "waf" format`)
}

func TestNew_WAFLogV1(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatWAFLogV1
	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)
}

func TestNew_ELBAcessLog(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatELBAccessLog}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)

	_, err = e.UnmarshalLogs([]byte("invalid"))
	require.ErrorContains(t, err, `failed to unmarshal logs as "elbaccess" format`)
}

func TestNew_ELBAcessLogV1(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatELBAccessLogV1
	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.NotNil(t, e)
}

func TestNew_Unimplemented(t *testing.T) {
	e, err := newExtension(&Config{Format: "invalid"}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.Error(t, err)
	require.Nil(t, e)
	assert.EqualError(t, err, `unimplemented format "invalid"`)
}

func TestValidateFeatureGates(t *testing.T) {
	registry := featuregate.GlobalRegistry()
	require.NoError(t, registry.Set(metadata.ExtensionEncodingAwslogsencodingDontEmitV0RPCConventionsFeatureGate.ID(), true))
	require.NoError(t, registry.Set(metadata.ExtensionEncodingAwslogsencodingEmitV1RPCConventionsFeatureGate.ID(), false))
	t.Cleanup(func() {
		require.NoError(t, registry.Set(metadata.ExtensionEncodingAwslogsencodingDontEmitV0RPCConventionsFeatureGate.ID(), false))
		require.NoError(t, registry.Set(metadata.ExtensionEncodingAwslogsencodingEmitV1RPCConventionsFeatureGate.ID(), false))
	})

	e, err := newExtension(&Config{Format: constants.FormatCloudTrailLog}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.Nil(t, e)
	require.ErrorContains(t, err, "extension.encoding.awslogsencoding.DontEmitV0RPCConventions requires extension.encoding.awslogsencoding.EmitV1RPCConventions to be enabled")
}

func TestGetReaderFromFormat(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		format string
		buf    []byte
	}{
		"non_gzip_data_waf_log": {
			format: constants.FormatWAFLog,
			buf:    []byte("invalid"),
		},
		"valid_gzip_reader": {
			format: constants.FormatWAFLog,
			buf: func() []byte {
				var buf bytes.Buffer
				gz := gzip.NewWriter(&buf)
				_, err := gz.Write([]byte("valid"))
				require.NoError(t, err)
				_ = gz.Close()
				return buf.Bytes()
			}(),
		},
		"valid_bytes_reader": {
			format: constants.FormatS3AccessLog,
			buf:    []byte("valid"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			e := &encodingExtension{
				format: test.format,
				cfg:    createDefaultConfig().(*Config),
			}
			_, reader, err := e.getReaderFromFormat(test.buf)
			require.NoError(t, err)
			require.NotNil(t, reader)
		})
	}
}

type fakeHost struct {
	exts map[component.ID]component.Component
}

func (h *fakeHost) GetExtensions() map[component.ID]component.Component {
	return h.exts
}

type stubLogsUnmarshaler struct{}

func (stubLogsUnmarshaler) UnmarshalLogs([]byte) (plog.Logs, error) {
	return plog.NewLogs(), nil
}

type stubExtension struct {
	stubLogsUnmarshaler
}

func (stubExtension) Start(context.Context, component.Host) error { return nil }
func (stubExtension) Shutdown(context.Context) error              { return nil }

func newStubExt() component.Component { return stubExtension{} }

func TestStart_CloudWatchRouting_ResolvesEncoding(t *testing.T) {
	jsonID := component.MustNewIDWithName("jsonlogencoding", "in-cw")
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatCloudWatchLogsSubscriptionFilter
	cfg.CloudWatchConfig.SubEncodings = []SubEncoding{{
		LogGroup: "/aws/json/*",
		Encoding: &jsonID,
	}}

	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)

	host := &fakeHost{exts: map[component.ID]component.Component{jsonID: newStubExt()}}
	require.NoError(t, e.Start(t.Context(), host))
	require.Equal(t, []component.ID{jsonID}, e.Dependencies())
}

func TestStart_CloudWatchRouting_UnknownEncodingErrors(t *testing.T) {
	missingID := component.MustNewIDWithName("jsonlogencoding", "missing")
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatCloudWatchLogsSubscriptionFilter
	cfg.CloudWatchConfig.SubEncodings = []SubEncoding{{
		LogGroup: "*",
		Encoding: &missingID,
	}}

	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)

	host := &fakeHost{exts: map[component.ID]component.Component{}}
	err = e.Start(t.Context(), host)
	require.ErrorContains(t, err, "not found")
}

func TestStart_CloudWatchRouting_WrongTypeErrors(t *testing.T) {
	id := component.MustNewIDWithName("notunmarshaler", "x")
	cfg := createDefaultConfig().(*Config)
	cfg.Format = constants.FormatCloudWatchLogsSubscriptionFilter
	cfg.CloudWatchConfig.SubEncodings = []SubEncoding{{
		LogGroup: "*",
		Encoding: &id,
	}}

	e, err := newExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)

	// A component that is not a plog.Unmarshaler.
	host := &fakeHost{exts: map[component.ID]component.Component{
		id: nopComponent{},
	}}
	err = e.Start(t.Context(), host)
	require.ErrorContains(t, err, "does not implement plog.Unmarshaler")
}

type nopComponent struct{}

func (nopComponent) Start(context.Context, component.Host) error { return nil }
func (nopComponent) Shutdown(context.Context) error              { return nil }

func TestDependencies_NoRoutes(t *testing.T) {
	e, err := newExtension(&Config{Format: constants.FormatCloudWatchLogsSubscriptionFilter}, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	require.Nil(t, e.Dependencies())
}

// readAndCompressLogFile reads the data inside it, compresses it
// and returns a GZIP reader for it.
func readAndCompressLogFile(t *testing.T, file string) []byte {
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)
	_, err = gzipWriter.Write(data)
	require.NoError(t, err)
	err = gzipWriter.Close()
	require.NoError(t, err)
	return compressedData.Bytes()
}

func TestConcurrentGzipReaderUsage(t *testing.T) {
	// Create an encoding extension for cloudwatch format to test the
	// gzip reader and check that it works as expected for non concurrent
	// and concurrent usage
	ext := &encodingExtension{
		unmarshaler: subscriptionfilter.NewSubscriptionFilterUnmarshaler(component.BuildInfo{}),
		format:      constants.FormatCloudWatchLogsSubscriptionFilter,
		gzipPool:    sync.Pool{},
	}

	cloudwatchData := readAndCompressLogFile(t, "testdata/cloudwatch_log.json")
	testUnmarshall := func() {
		_, err := ext.UnmarshalLogs(cloudwatchData)
		require.NoError(t, err)
	}

	// non concurrent
	testUnmarshall()
	// concurrent usage
	concurrent := 20
	wg := sync.WaitGroup{}
	for range concurrent {
		wg.Go(func() {
			testUnmarshall()
		})
	}
	wg.Wait()
}
