// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package subscriptionfilter // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/unmarshaler/subscription-filter"

import (
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	gojson "github.com/goccy/go-json"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/otel/semconv/v1.40.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/constants"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension/internal/unmarshaler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/xstreamencoding"
)

const (
	ctrlMessageType = "CONTROL_MESSAGE"

	// PayloadMessage and PayloadEnvelope mirror the public payload
	// values exposed in the extension's config; defined here to avoid
	// importing the parent package from this internal package.
	PayloadMessage  = "message"
	PayloadEnvelope = "envelope"
)

var (
	errEmptyOwner     = errors.New("cloudwatch log with message type 'DATA_MESSAGE' has empty owner field")
	errEmptyLogGroup  = errors.New("cloudwatch log with message type 'DATA_MESSAGE' has empty log group field")
	errEmptyLogStream = errors.New("cloudwatch log with message type 'DATA_MESSAGE' has empty log stream field")
)

var _ unmarshaler.StreamingLogsUnmarshaler = (*SubscriptionFilterUnmarshaler)(nil)

// Route is a single resolved routing rule. It is constructed by the
// extension after looking up the referenced encoding extension from the
// component host.
type Route struct {
	// LogGroup is the glob pattern (path.Match) matched against the
	// CloudWatch payload's logGroup. An empty value matches anything.
	LogGroup string
	// LogStream is the glob pattern (path.Match) matched against the
	// CloudWatch payload's logStream. An empty value matches anything.
	LogStream string
	// Encoding is the resolved encoding extension that decodes matching
	// payloads.
	Encoding plog.Unmarshaler
	// Payload selects what is fed to Encoding: PayloadMessage (default)
	// passes each event's message bytes one at a time; PayloadEnvelope
	// passes the full CloudWatch record bytes once.
	Payload string
}

type SubscriptionFilterUnmarshaler struct {
	buildInfo component.BuildInfo
	routes    []Route
}

func NewSubscriptionFilterUnmarshaler(buildInfo component.BuildInfo) *SubscriptionFilterUnmarshaler {
	return &SubscriptionFilterUnmarshaler{
		buildInfo: buildInfo,
	}
}

// SetRoutes configures the routing table used to dispatch CloudWatch
// payloads to other encoding extensions. Callers should invoke this once
// during extension Start, after resolving encoding component IDs against
// the host.
func (f *SubscriptionFilterUnmarshaler) SetRoutes(routes []Route) {
	f.routes = routes
}

// UnmarshalAWSLogs deserializes the given reader as CloudWatch Logs events
// into a plog.Logs, grouping logs by owner (account ID), log group, and
// log stream. When extracted fields are present (from centralized logging),
// logs are further grouped by their extracted account ID and region.
// Logs are assumed to be gzip-compressed as specified at
// https://docs.aws.amazon.com/firehose/latest/dev/writing-with-cloudwatch-logs.html.
func (f *SubscriptionFilterUnmarshaler) UnmarshalAWSLogs(reader io.Reader) (plog.Logs, error) {
	// Decode as a stream but flush all at once using flush options
	streamUnmarshaler, err := f.NewLogsDecoder(reader, encoding.WithFlushItems(0), encoding.WithFlushBytes(0))
	if err != nil {
		return plog.Logs{}, err
	}
	logs, err := streamUnmarshaler.DecodeLogs()
	if err != nil {
		// we must check for EOF with direct comparison and avoid wrapped EOF that can come from stream itself
		//nolint:errorlint
		if err == io.EOF {
			// EOF indicates no logs were found, return any logs that's available
			return logs, nil
		}

		return plog.Logs{}, err
	}

	return logs, nil
}

// NewLogsDecoder returns a LogsDecoder that processes CloudWatch Logs subscription filter events.
// Supported sub formats:
//   - DATA_MESSAGE: Returns logs grouped by owner, log group, and stream; offset is the number of records processed
//   - CONTROL_MESSAGE: Returns empty log; offset is the number of records processed
func (f *SubscriptionFilterUnmarshaler) NewLogsDecoder(reader io.Reader, options ...encoding.DecoderOption) (encoding.LogsDecoder, error) {
	batchHelper := xstreamencoding.NewBatchHelper(options...)
	decoder := gojson.NewDecoder(reader)

	var offset int64

	if batchHelper.Options().Offset > 0 {
		for offset < batchHelper.Options().Offset {
			if !decoder.More() {
				return nil, fmt.Errorf("EOF reached before offset %d records were discarded", batchHelper.Options().Offset)
			}

			var raw gojson.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return nil, err
			}
			offset++
		}
	}

	return xstreamencoding.NewLogsDecoderAdapter(
		func() (plog.Logs, error) {
			logs := plog.NewLogs()
			resourceLogsByKey := make(map[resourceGroupKey]plog.LogRecordSlice)

			for decoder.More() {
				if err := f.decodeOne(decoder, logs, resourceLogsByKey); err != nil {
					return plog.Logs{}, err
				}

				offset++
				batchHelper.IncrementItems(1)

				if batchHelper.ShouldFlush() {
					batchHelper.Reset()
					return logs, nil
				}
			}

			if logs.ResourceLogs().Len() == 0 {
				return plog.NewLogs(), io.EOF
			}

			return logs, nil
		}, func() int64 {
			return offset
		},
	), nil
}

// decodeOne reads a single CloudWatch record from decoder and merges it
// into logs. When no routes are configured the record is decoded directly
// into cloudwatchLogsData (single pass). When routes exist, the record is
// captured as raw bytes and a header-only decode determines the routing
// outcome; only the cases that need event data trigger a full decode.
func (f *SubscriptionFilterUnmarshaler) decodeOne(
	decoder *gojson.Decoder,
	logs plog.Logs,
	resourceLogsByKey map[resourceGroupKey]plog.LogRecordSlice,
) error {
	if len(f.routes) == 0 {
		var cwLog cloudwatchLogsData
		if err := decoder.Decode(&cwLog); err != nil {
			return fmt.Errorf("failed to decode decompressed reader: %w", err)
		}
		if cwLog.MessageType == ctrlMessageType {
			return nil
		}
		if err := validateLog(cwLog); err != nil {
			return fmt.Errorf("invalid cloudwatch log: %w", err)
		}
		f.appendLogs(logs, resourceLogsByKey, cwLog)
		return nil
	}

	// Capture the raw record bytes so envelope routes can replay them
	// without re-marshaling.
	var raw gojson.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("failed to decode decompressed reader: %w", err)
	}

	// Header-only decode for routing inspection. This skips the
	// logEvents array, which is the bulk of the payload.
	var hdr cloudwatchLogsHeader
	if err := gojson.Unmarshal(raw, &hdr); err != nil {
		return fmt.Errorf("failed to decode decompressed reader: %w", err)
	}
	if hdr.MessageType == ctrlMessageType {
		return nil
	}
	if err := validateLogFields(hdr.MessageType, hdr.Owner, hdr.LogGroup, hdr.LogStream); err != nil {
		return fmt.Errorf("invalid cloudwatch log: %w", err)
	}

	route := f.matchRoute(hdr.LogGroup, hdr.LogStream)
	if route != nil && route.Payload == PayloadEnvelope {
		// Envelope mode never needs the events array; dispatch raw bytes.
		return f.routeEnvelope(logs, []byte(raw), hdr.LogGroup, *route)
	}

	// Message-mode dispatch and the fallback path both need the events
	// array; pay for the full decode now.
	var cwLog cloudwatchLogsData
	if err := gojson.Unmarshal(raw, &cwLog); err != nil {
		return fmt.Errorf("failed to decode decompressed reader: %w", err)
	}
	if route != nil {
		return f.routeMessage(logs, cwLog, *route)
	}
	f.appendLogs(logs, resourceLogsByKey, cwLog)
	return nil
}

// matchRoute returns the first route that matches the given log group and
// stream, or nil if none match.
func (f *SubscriptionFilterUnmarshaler) matchRoute(logGroup, logStream string) *Route {
	for i := range f.routes {
		r := &f.routes[i]
		if !globMatch(r.LogGroup, logGroup) {
			continue
		}
		if !globMatch(r.LogStream, logStream) {
			continue
		}
		return r
	}
	return nil
}

// globMatch returns true when pattern is empty or path.Match reports a match.
// A malformed pattern is treated as no match; patterns are sanity-checked at
// config-validation time.
func globMatch(pattern, value string) bool {
	if pattern == "" {
		return true
	}
	ok, err := path.Match(pattern, value)
	return err == nil && ok
}

// routeEnvelope hands the original CloudWatch record bytes to the routed
// encoding once; the routed extension owns iteration and resource
// attribution.
func (*SubscriptionFilterUnmarshaler) routeEnvelope(logs plog.Logs, raw []byte, logGroup string, route Route) error {
	routed, err := route.Encoding.UnmarshalLogs(raw)
	if err != nil {
		return fmt.Errorf("routed encoding for log group %q failed in envelope mode: %w", logGroup, err)
	}
	routed.ResourceLogs().MoveAndAppendTo(logs.ResourceLogs())
	return nil
}

// routeMessage dispatches each event's message bytes to the routed
// encoding, then merges the result under CloudWatch resource attributes
// and back-fills the CloudWatch event timestamp on log records that the
// routed encoding did not already timestamp.
func (*SubscriptionFilterUnmarshaler) routeMessage(logs plog.Logs, cwLog cloudwatchLogsData, route Route) error {
	if route.Payload != "" && route.Payload != PayloadMessage {
		// Caught at config validation; defensive only.
		return fmt.Errorf("unsupported payload %q in route", route.Payload)
	}
	for _, event := range cwLog.LogEvents {
		routed, err := route.Encoding.UnmarshalLogs([]byte(event.Message))
		if err != nil {
			return fmt.Errorf("routed encoding for log group %q failed on event %q: %w", cwLog.LogGroup, event.ID, err)
		}
		eventTS := pcommon.Timestamp(event.Timestamp * int64(time.Millisecond))
		accountID, region := resolveAccountAndRegion(event, cwLog.Owner)
		for i := 0; i < routed.ResourceLogs().Len(); i++ {
			rl := routed.ResourceLogs().At(i)
			addCloudWatchResourceAttrs(rl.Resource().Attributes(), accountID, region, cwLog.LogGroup, cwLog.LogStream)
			backfillTimestamps(rl, eventTS)
		}
		routed.ResourceLogs().MoveAndAppendTo(logs.ResourceLogs())
	}
	return nil
}

// addCloudWatchResourceAttrs sets the standard CloudWatch resource
// attributes on attrs. Existing values are preserved if already set by
// the routed encoding, except for the AWS log group/stream slice attrs
// which are unconditionally written so the CloudWatch envelope is
// authoritative for those.
func addCloudWatchResourceAttrs(attrs pcommon.Map, accountID, region, logGroup, logStream string) {
	if _, ok := attrs.Get(string(conventions.CloudProviderKey)); !ok {
		attrs.PutStr(string(conventions.CloudProviderKey), conventions.CloudProviderAWS.Value.AsString())
	}
	if accountID != "" {
		if _, ok := attrs.Get(string(conventions.CloudAccountIDKey)); !ok {
			attrs.PutStr(string(conventions.CloudAccountIDKey), accountID)
		}
	}
	if region != "" {
		if _, ok := attrs.Get(string(conventions.CloudRegionKey)); !ok {
			attrs.PutStr(string(conventions.CloudRegionKey), region)
		}
	}
	attrs.PutEmptySlice(string(conventions.AWSLogGroupNamesKey)).AppendEmpty().SetStr(logGroup)
	attrs.PutEmptySlice(string(conventions.AWSLogStreamNamesKey)).AppendEmpty().SetStr(logStream)
}

// backfillTimestamps sets ts on every log record in rl whose timestamp
// is zero, leaving timestamps written by the routed encoding intact.
func backfillTimestamps(rl plog.ResourceLogs, ts pcommon.Timestamp) {
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		sl := rl.ScopeLogs().At(i)
		for j := 0; j < sl.LogRecords().Len(); j++ {
			lr := sl.LogRecords().At(j)
			if lr.Timestamp() == 0 {
				lr.SetTimestamp(ts)
			}
		}
	}
}

// resolveAccountAndRegion returns the account ID and region for an event,
// preferring extracted fields when present, falling back to owner.
func resolveAccountAndRegion(event cloudwatchLogsLogEvent, owner string) (string, string) {
	if event.ExtractedFields != nil {
		accountID := event.ExtractedFields.AccountID
		if accountID == "" {
			accountID = owner
		}
		return accountID, event.ExtractedFields.Region
	}
	return owner, ""
}

// appendLogs appends log records from cwLog into the given plog.Logs, reusing
// existing ResourceLogs entries tracked by resourceLogsByKey when possible.
// Events are grouped by their extracted fields (account ID + region) and
// by log group/stream combination.
func (f *SubscriptionFilterUnmarshaler) appendLogs(logs plog.Logs, resourceLogsByKey map[resourceGroupKey]plog.LogRecordSlice, cwLog cloudwatchLogsData) {
	for _, event := range cwLog.LogEvents {
		key := extractResourceKey(event, cwLog.Owner, cwLog.LogGroup, cwLog.LogStream)

		logRecords, exists := resourceLogsByKey[key]
		if !exists {
			rl := logs.ResourceLogs().AppendEmpty()
			resourceAttrs := rl.Resource().Attributes()
			resourceAttrs.PutStr(string(conventions.CloudProviderKey), conventions.CloudProviderAWS.Value.AsString())
			resourceAttrs.PutStr(string(conventions.CloudAccountIDKey), key.accountID)
			if key.region != "" {
				resourceAttrs.PutStr(string(conventions.CloudRegionKey), key.region)
			}
			resourceAttrs.PutEmptySlice(string(conventions.AWSLogGroupNamesKey)).AppendEmpty().SetStr(cwLog.LogGroup)
			resourceAttrs.PutEmptySlice(string(conventions.AWSLogStreamNamesKey)).AppendEmpty().SetStr(cwLog.LogStream)

			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(metadata.ScopeName)
			sl.Scope().SetVersion(f.buildInfo.Version)
			sl.Scope().Attributes().PutStr(constants.FormatIdentificationTag, "aws."+constants.FormatCloudWatchLogsSubscriptionFilter)

			logRecords = sl.LogRecords()
			resourceLogsByKey[key] = logRecords
		}

		logRecord := logRecords.AppendEmpty()
		// pcommon.Timestamp is a time specified as UNIX Epoch time in nanoseconds
		// but timestamp in cloudwatch logs are in milliseconds.
		logRecord.SetTimestamp(pcommon.Timestamp(event.Timestamp * int64(time.Millisecond)))
		logRecord.Body().SetStr(event.Message)
	}
}

// extractResourceKey extracts the resource group key from a log event.
// When extracted fields are present, uses those; otherwise falls back to owner.
func extractResourceKey(event cloudwatchLogsLogEvent, owner, logGroup, logStream string) resourceGroupKey {
	var key resourceGroupKey
	key.logGroup = logGroup
	key.logStream = logStream
	if event.ExtractedFields != nil {
		if event.ExtractedFields.AccountID != "" {
			key.accountID = event.ExtractedFields.AccountID
		} else {
			key.accountID = owner
		}
		key.region = event.ExtractedFields.Region
	} else {
		key.accountID = owner
	}
	return key
}

func validateLog(log cloudwatchLogsData) error {
	return validateLogFields(log.MessageType, log.Owner, log.LogGroup, log.LogStream)
}

func validateLogFields(messageType, owner, logGroup, logStream string) error {
	switch messageType {
	case "DATA_MESSAGE":
		if owner == "" {
			return errEmptyOwner
		}
		if logGroup == "" {
			return errEmptyLogGroup
		}
		if logStream == "" {
			return errEmptyLogStream
		}
	case ctrlMessageType:
	default:
		return fmt.Errorf("cloudwatch log has invalid message type %q", messageType)
	}
	return nil
}
