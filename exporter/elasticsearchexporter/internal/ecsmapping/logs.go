// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ecsmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/ecsmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/collector/semconv/v1.22.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// EncodeLogRecord encodes a log record to doc. Resource, scope,
// and data stream attributes are expected to be added separately.
func EncodeLogRecord(
	record plog.LogRecord,
	doc *objmodel.Document,
	setUnmappedAttribute func(doc *objmodel.Document, k string, v pcommon.Value),
) {
	// Handle top-level fields.
	if record.Timestamp() != 0 {
		doc.AddTimestamp("@timestamp", record.Timestamp())
	} else {
		doc.AddTimestamp("@timestamp", record.ObservedTimestamp())
	}
	doc.AddTraceID("trace.id", record.TraceID())
	doc.AddSpanID("span.id", record.SpanID())
	if n := record.SeverityNumber(); n != plog.SeverityNumberUnspecified {
		doc.AddInt("event.severity", int64(record.SeverityNumber()))
	}
	doc.AddString("log.level", record.SeverityText())
	if record.Body().Type() == pcommon.ValueTypeStr {
		doc.AddAttribute("message", record.Body())
	}

	// Try to map record-level attributes to ECS fields.
	record.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case "event.name":
			doc.AddAttribute("event.action", v)
		case semconv.AttributeExceptionMessage:
			doc.AddAttribute("error.message", v)
		case semconv.AttributeExceptionStacktrace:
			doc.AddAttribute("error.stacktrace", v)
		case semconv.AttributeExceptionType:
			doc.AddAttribute("error.type", v)
		case semconv.AttributeExceptionEscaped:
			doc.AddAttribute("event.error.exception.handled", v)
		default:
			setUnmappedAttribute(doc, k, v)
		}
		return true
	})
}
