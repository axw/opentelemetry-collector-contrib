// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/datapoints"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/ecsmapping"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticapmmapping"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/serializer/otelserializer"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

var ErrInvalidTypeForBodyMapMode = errors.New("invalid log record body type for 'bodymap' mapping mode")

// documentEncoder is an interface for encoding signals to Elasticsearch documents.
type documentEncoder interface {
	encodeLog(encodingContext, plog.LogRecord, elasticsearch.Index, *bytes.Buffer) error
	encodeSpan(encodingContext, ptrace.Span, elasticsearch.Index, *bytes.Buffer) error
	encodeSpanEvent(encodingContext, ptrace.Span, ptrace.SpanEvent, elasticsearch.Index, *bytes.Buffer) error
	encodeMetrics(_ encodingContext, _ []datapoints.DataPoint, validationErrors *[]error, _ elasticsearch.Index, _ *bytes.Buffer) (map[string]string, error)
	encodeProfile(_ encodingContext, _ pprofile.Profile, _ func(*bytes.Buffer, string, string) error) error
}

type encodingContext struct {
	resource          pcommon.Resource
	resourceSchemaURL string
	scope             pcommon.InstrumentationScope
	scopeSchemaURL    string
}

func newEncoder(mode MappingMode) (documentEncoder, error) {
	switch mode {
	case MappingNone:
		return legacyModeEncoder{
			metricsUnsupportedEncoder:  metricsUnsupportedEncoder{mode: mode},
			profilesUnsupportedEncoder: profilesUnsupportedEncoder{mode: mode},
			nonOTelSpanEncoder: nonOTelSpanEncoder{
				attributesPrefix: "Attributes",
				eventsPrefix:     "Events",
			},
			attributesPrefix: "Attributes",
		}, nil
	case MappingRaw:
		return legacyModeEncoder{
			metricsUnsupportedEncoder:  metricsUnsupportedEncoder{mode: mode},
			profilesUnsupportedEncoder: profilesUnsupportedEncoder{mode: mode},
			nonOTelSpanEncoder: nonOTelSpanEncoder{
				attributesPrefix: "",
				eventsPrefix:     "",
			},
			attributesPrefix: "",
		}, nil
	case MappingECS:
		return ecsModeEncoder{
			profilesUnsupportedEncoder: profilesUnsupportedEncoder{mode: mode},
			nonOTelSpanEncoder: nonOTelSpanEncoder{
				attributesPrefix: "Attributes",
				eventsPrefix:     "Events",
				dedot:            true,
			},
		}, nil
	case MappingElasticAPM:
		return elasticapmModeEncoder{
			profilesUnsupportedEncoder: profilesUnsupportedEncoder{mode: mode},
		}, nil
	case MappingBodyMap:
		return bodymapModeEncoder{
			metricsUnsupportedEncoder:  metricsUnsupportedEncoder{mode: mode},
			profilesUnsupportedEncoder: profilesUnsupportedEncoder{mode: mode},
		}, nil
	case MappingOTel:
		return otelModeEncoder{}, nil
	}
	return nil, fmt.Errorf("unknown mapping mode %q (%d)", mode, int(mode))
}

type legacyModeEncoder struct {
	nonOTelSpanEncoder
	nopSpanEventEncoder
	metricsUnsupportedEncoder
	profilesUnsupportedEncoder
	attributesPrefix string
}

type ecsModeEncoder struct {
	nonOTelSpanEncoder
	nopSpanEventEncoder
	profilesUnsupportedEncoder
}

type bodymapModeEncoder struct {
	metricsUnsupportedEncoder
	profilesUnsupportedEncoder
}

type elasticapmModeEncoder struct {
	profilesUnsupportedEncoder
}

type otelModeEncoder struct{}

const (
	traceIDField   = "traceID"
	spanIDField    = "spanID"
	attributeField = "attribute"
)

func (e legacyModeEncoder) encodeLog(ec encodingContext, record plog.LogRecord, idx elasticsearch.Index, buf *bytes.Buffer) error {
	var document objmodel.Document

	docTimeStamp := record.Timestamp()
	if docTimeStamp.AsTime().UnixNano() == 0 {
		docTimeStamp = record.ObservedTimestamp()
	}
	// We use @timestamp in order to ensure that we can index if the default data stream logs template is used.
	document.AddTimestamp("@timestamp", docTimeStamp)
	document.AddTraceID("TraceId", record.TraceID())
	document.AddSpanID("SpanId", record.SpanID())
	document.AddInt("TraceFlags", int64(record.Flags()))
	document.AddString("SeverityText", record.SeverityText())
	document.AddInt("SeverityNumber", int64(record.SeverityNumber()))
	document.AddAttribute("Body", record.Body())
	document.AddAttributes("Resource", ec.resource.Attributes())
	document.AddAttributes("Scope", scopeToAttributes(ec.scope))
	encodeAttributes(e.attributesPrefix, &document, record.Attributes(), idx)

	return document.Serialize(buf, false)
}

func (e ecsModeEncoder) encodeLog(
	ec encodingContext,
	record plog.LogRecord,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	var document objmodel.Document
	ecsmapping.EncodeResource(
		ec.resource, &document,
		true, // setAgentFields
		true, // setHostOSType
		(*objmodel.Document).AddAttribute,
	)
	ecsmapping.EncodeScope(ec.scope, &document)
	ecsmapping.EncodeLogRecord(
		record, &document,
		(*objmodel.Document).AddAttribute,
	)
	addDataStreamAttributes(&document, "", idx)
	return document.Serialize(buf, true)
}

func (ecsModeEncoder) encodeMetrics(
	ec encodingContext,
	dataPoints []datapoints.DataPoint,
	validationErrors *[]error,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) (map[string]string, error) {
	var document objmodel.Document
	ecsmapping.EncodeResource(
		ec.resource,
		&document,
		false, // setAgentFields
		false, // setHostOSType
		(*objmodel.Document).AddAttribute,
	)
	ecsmapping.EncodeScope(ec.scope, &document)
	ecsmapping.EncodeDataPoints(
		dataPoints, &document,
		(*objmodel.Document).AddAttribute,
		validationErrors,
	)
	addDataStreamAttributes(&document, "", idx)
	err := document.Serialize(buf, true)
	return document.DynamicTemplates(), err
}

func (elasticapmModeEncoder) encodeLog(
	ec encodingContext,
	record plog.LogRecord,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	var document objmodel.Document
	ecsmapping.EncodeResource(
		ec.resource, &document,
		true, // setAgentFields
		true, // setHostOSType
		elasticapmmapping.SetResourceAttribute,
	)
	elasticapmmapping.EncodeScope(ec.scope, &document)
	ecsmapping.EncodeLogRecord(
		record, &document,
		elasticapmmapping.SetLogRecordAttribute,
	)
	addDataStreamAttributes(&document, "", idx)
	return document.Serialize(buf, true)
}

func (elasticapmModeEncoder) encodeMetrics(
	ec encodingContext,
	dataPoints []datapoints.DataPoint,
	validationErrors *[]error,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) (map[string]string, error) {
	var document objmodel.Document
	ecsmapping.EncodeResource(
		ec.resource,
		&document,
		true, // setAgentFields
		true, // setHostOSType
		elasticapmmapping.SetResourceAttribute,
	)
	elasticapmmapping.EncodeScope(ec.scope, &document)
	ecsmapping.EncodeDataPoints(
		dataPoints, &document,
		elasticapmmapping.SetDataPointAttribute,
		validationErrors,
	)
	addDataStreamAttributes(&document, "", idx)
	err := document.Serialize(buf, true)
	return document.DynamicTemplates(), err
}

func (elasticapmModeEncoder) encodeSpan(
	ec encodingContext,
	span ptrace.Span,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	var document objmodel.Document
	ecsmapping.EncodeResource(
		ec.resource,
		&document,
		true, // setAgentFields
		true, // setHostOSType
		elasticapmmapping.SetResourceAttribute,
	)
	elasticapmmapping.EncodeScope(ec.scope, &document)
	elasticapmmapping.EncodeSpan(span, &document)
	addDataStreamAttributes(&document, "", idx)
	return document.Serialize(buf, true)
}

func (elasticapmModeEncoder) encodeSpanEvent(
	ec encodingContext,
	span ptrace.Span,
	spanEvent ptrace.SpanEvent,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	// TODO
	return nil
}

func (e otelModeEncoder) encodeLog(
	ec encodingContext,
	record plog.LogRecord,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	return otelserializer.SerializeLog(
		ec.resource, ec.resourceSchemaURL,
		ec.scope, ec.scopeSchemaURL,
		record, idx, buf,
	)
}

func (otelModeEncoder) encodeSpan(
	ec encodingContext,
	span ptrace.Span,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	return otelserializer.SerializeSpan(
		ec.resource, ec.resourceSchemaURL,
		ec.scope, ec.scopeSchemaURL,
		span, idx, buf,
	)
}

func (otelModeEncoder) encodeSpanEvent(
	ec encodingContext,
	span ptrace.Span,
	spanEvent ptrace.SpanEvent,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	otelserializer.SerializeSpanEvent(
		ec.resource, ec.resourceSchemaURL,
		ec.scope, ec.scopeSchemaURL,
		span, spanEvent, idx, buf,
	)
	return nil
}

func (otelModeEncoder) encodeMetrics(
	ec encodingContext,
	dataPoints []datapoints.DataPoint,
	validationErrors *[]error,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) (map[string]string, error) {
	return otelserializer.SerializeMetrics(
		ec.resource, ec.resourceSchemaURL,
		ec.scope, ec.scopeSchemaURL,
		dataPoints, validationErrors, idx, buf,
	)
}

func (otelModeEncoder) encodeProfile(
	ec encodingContext,
	profile pprofile.Profile,
	pushData func(*bytes.Buffer, string, string) error,
) error {
	return otelserializer.SerializeProfile(ec.resource, ec.scope, profile, pushData)
}

func (e bodymapModeEncoder) encodeLog(
	_ encodingContext,
	record plog.LogRecord,
	_ elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	body := record.Body()
	if body.Type() != pcommon.ValueTypeMap {
		return fmt.Errorf("%w: %q", ErrInvalidTypeForBodyMapMode, body.Type())
	}
	serializer.Map(body.Map(), buf)
	return nil
}

func (bodymapModeEncoder) encodeSpan(encodingContext, ptrace.Span, elasticsearch.Index, *bytes.Buffer) error {
	return fmt.Errorf("bodymap mode does not support encoding spans")
}

func (bodymapModeEncoder) encodeSpanEvent(encodingContext, ptrace.Span, ptrace.SpanEvent, elasticsearch.Index, *bytes.Buffer) error {
	return fmt.Errorf("bodymap mode does not support encoding span events")
}

type metricsUnsupportedEncoder struct {
	mode MappingMode
}

//nolint:unparam // result 0 is expected to always be nil
func (e metricsUnsupportedEncoder) encodeMetrics(
	_ encodingContext,
	_ []datapoints.DataPoint,
	_ *[]error,
	_ elasticsearch.Index,
	_ *bytes.Buffer,
) (map[string]string, error) {
	return nil, fmt.Errorf("mapping mode %q (%d) does not support metrics", e.mode, int(e.mode))
}

type profilesUnsupportedEncoder struct {
	mode MappingMode
}

func (e profilesUnsupportedEncoder) encodeProfile(
	_ encodingContext, _ pprofile.Profile, _ func(*bytes.Buffer, string, string) error,
) error {
	return fmt.Errorf("mapping mode %q (%d) does not support profiles", e.mode, int(e.mode))
}

type nonOTelSpanEncoder struct {
	attributesPrefix string
	eventsPrefix     string
	dedot            bool
}

func (e nonOTelSpanEncoder) encodeSpan(
	ec encodingContext,
	span ptrace.Span,
	idx elasticsearch.Index,
	buf *bytes.Buffer,
) error {
	var document objmodel.Document
	document.AddTimestamp("@timestamp", span.StartTimestamp()) // We use @timestamp in order to ensure that we can index if the default data stream logs template is used.
	document.AddTimestamp("EndTimestamp", span.EndTimestamp())
	document.AddTraceID("TraceId", span.TraceID())
	document.AddSpanID("SpanId", span.SpanID())
	document.AddSpanID("ParentSpanId", span.ParentSpanID())
	document.AddString("Name", span.Name())
	document.AddString("Kind", traceutil.SpanKindStr(span.Kind()))
	document.AddInt("TraceStatus", int64(span.Status().Code()))
	document.AddString("TraceStatusDescription", span.Status().Message())
	document.AddString("Link", spanLinksToString(span.Links()))
	document.AddAttributes("Resource", ec.resource.Attributes())
	document.AddInt("Duration", durationAsMicroseconds(span.StartTimestamp().AsTime(), span.EndTimestamp().AsTime())) // unit is microseconds
	document.AddAttributes("Scope", scopeToAttributes(ec.scope))
	encodeAttributes(e.attributesPrefix, &document, span.Attributes(), idx)
	document.AddEvents(e.eventsPrefix, span.Events())
	return document.Serialize(buf, e.dedot)
}

func addDataStreamAttributes(document *objmodel.Document, key string, idx elasticsearch.Index) {
	if idx.IsDataStream() {
		document.AddString(key+"data_stream.type", idx.Type)
		document.AddString(key+"data_stream.dataset", idx.Dataset)
		document.AddString(key+"data_stream.namespace", idx.Namespace)
	}
}

// nopSpanEventEncoder is embedded in all non-OTel encoders,
// since only OTel mapping mode currently encodes span events
// as separate documents. In all others they are stored within
// the span document.
type nopSpanEventEncoder struct{}

func (nopSpanEventEncoder) encodeSpanEvent(encodingContext, ptrace.Span, ptrace.SpanEvent, elasticsearch.Index, *bytes.Buffer) error {
	return nil
}

func encodeAttributes(prefix string, document *objmodel.Document, attributes pcommon.Map, idx elasticsearch.Index) {
	document.AddAttributes(prefix, attributes)
	addDataStreamAttributes(document, prefix, idx)
}

func spanLinksToString(spanLinkSlice ptrace.SpanLinkSlice) string {
	linkArray := make([]map[string]any, 0, spanLinkSlice.Len())
	for i := 0; i < spanLinkSlice.Len(); i++ {
		spanLink := spanLinkSlice.At(i)
		link := map[string]any{}
		link[spanIDField] = traceutil.SpanIDToHexOrEmptyString(spanLink.SpanID())
		link[traceIDField] = traceutil.TraceIDToHexOrEmptyString(spanLink.TraceID())
		link[attributeField] = spanLink.Attributes().AsRaw()
		linkArray = append(linkArray, link)
	}
	linkArrayBytes, _ := json.Marshal(&linkArray)
	return string(linkArrayBytes)
}

// durationAsMicroseconds calculate span duration through end - start nanoseconds and converts time.Time to microseconds,
// which is the format the Duration field is stored in the Span.
func durationAsMicroseconds(start, end time.Time) int64 {
	return (end.UnixNano() - start.UnixNano()) / 1000
}

func scopeToAttributes(scope pcommon.InstrumentationScope) pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("name", scope.Name())
	attrs.PutStr("version", scope.Version())
	for k, v := range scope.Attributes().AsRaw() {
		attrs.PutStr(k, v.(string))
	}
	return attrs
}
