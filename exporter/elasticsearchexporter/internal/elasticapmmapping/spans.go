// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// EncodeSpan encodes a span to an Elasticsearch document in the
// legacy Elastic APM schema. The span is expected to have been
// processed by the elasticapm processor.
func EncodeSpan(span ptrace.Span, doc *objmodel.Document) {
	doc.AddTimestamp("@timestamp", span.StartTimestamp())
	doc.AddTraceID("trace.id", span.TraceID())
	doc.AddSpanID("span.id", span.SpanID())
	doc.AddSpanID("parent.id", span.ParentSpanID())

	spanLinks := span.Links()
	links := make([]objmodel.Value, spanLinks.Len())
	for i := range spanLinks.Len() {
		spanLink := spanLinks.At(i)

		var link objmodel.Document
		link.AddSpanID("span.id", spanLink.SpanID())
		link.AddTraceID("trace.id", spanLink.TraceID())
		links[i] = objmodel.ObjectValue(link)
	}

	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		SetLabel(doc, k, v)
		return true
	})
}
