// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
)

var receiverRegex = regexp.MustCompile(`/receiver/(\w*receiver)`)

const (
	maxDataStreamBytes       = 100
	disallowedNamespaceRunes = "\\/*?\"<>| ,#:"
	disallowedDatasetRunes   = "-\\/*?\"<>| ,#:"
)

// Sanitize the datastream fields (dataset, namespace) to apply restrictions
// as outlined in https://www.elastic.co/guide/en/ecs/current/ecs-data_stream.html
// The suffix will be appended after truncation of max bytes.
func sanitizeDataStreamField(field, disallowed, appendSuffix string) string {
	field = strings.Map(func(r rune) rune {
		if strings.ContainsRune(disallowed, r) {
			return '_'
		}
		return unicode.ToLower(r)
	}, field)

	if len(field) > maxDataStreamBytes-len(appendSuffix) {
		field = field[:maxDataStreamBytes-len(appendSuffix)]
	}
	field += appendSuffix

	return field
}

type documentRoutingContext encodingContext

// documentRouter is an interface for routing records to the appropriate
// index or data stream. The router may mutate record attributes.
type documentRouter interface {
	routeLogRecord(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error)
	routeDataPoint(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error)
	routeSpan(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error)
	routeSpanEvent(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error)
}

func newDocumentRouter(mode MappingMode, dynamicIndex bool, defaultIndex string, cfg *Config) documentRouter {
	var router documentRouter
	if dynamicIndex {
		router = dynamicDocumentRouter{
			index: elasticsearch.Index{Index: defaultIndex},
			otel:  mode == MappingOTel,
		}
	} else {
		router = staticDocumentRouter{
			index: elasticsearch.Index{Index: defaultIndex},
		}
	}
	if cfg.LogstashFormat.Enabled {
		router = logstashDocumentRouter{inner: router, logstashFormat: cfg.LogstashFormat}
	}
	return router
}

type staticDocumentRouter struct {
	index elasticsearch.Index
}

func (r staticDocumentRouter) routeLogRecord(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(ec, recordAttrs)
}

func (r staticDocumentRouter) routeDataPoint(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(ec, recordAttrs)
}

func (r staticDocumentRouter) routeSpan(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(ec, recordAttrs)
}

func (r staticDocumentRouter) routeSpanEvent(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(ec, recordAttrs)
}

func (r staticDocumentRouter) route(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.index, nil
}

type dynamicDocumentRouter struct {
	index elasticsearch.Index
	otel  bool
}

func (r dynamicDocumentRouter) routeLogRecord(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return routeRecord(ec.resource, ec.scope, recordAttrs, r.index.Index, r.otel, defaultDataStreamTypeLogs), nil
}

func (r dynamicDocumentRouter) routeDataPoint(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return routeRecord(ec.resource, ec.scope, recordAttrs, r.index.Index, r.otel, defaultDataStreamTypeMetrics), nil
}

func (r dynamicDocumentRouter) routeSpan(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return routeRecord(ec.resource, ec.scope, recordAttrs, r.index.Index, r.otel, defaultDataStreamTypeTraces), nil
}

func (r dynamicDocumentRouter) routeSpanEvent(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return routeRecord(ec.resource, ec.scope, recordAttrs, r.index.Index, r.otel, defaultDataStreamTypeLogs), nil
}

type logstashDocumentRouter struct {
	inner          documentRouter
	logstashFormat LogstashFormatSettings
}

func (r logstashDocumentRouter) routeLogRecord(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(r.inner.routeLogRecord(ec, recordAttrs))
}

func (r logstashDocumentRouter) routeDataPoint(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(r.inner.routeDataPoint(ec, recordAttrs))
}

func (r logstashDocumentRouter) routeSpan(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(r.inner.routeSpan(ec, recordAttrs))
}

func (r logstashDocumentRouter) routeSpanEvent(ec encodingContext, recordAttrs pcommon.Map) (elasticsearch.Index, error) {
	return r.route(r.inner.routeSpanEvent(ec, recordAttrs))
}

func (r logstashDocumentRouter) route(index elasticsearch.Index, err error) (elasticsearch.Index, error) {
	if err != nil {
		return elasticsearch.Index{}, err
	}
	formattedIndex, err := generateIndexWithLogstashFormat(index.Index, &r.logstashFormat, time.Now())
	if err != nil {
		return elasticsearch.Index{}, err
	}
	return elasticsearch.Index{Index: formattedIndex}, nil
}

func routeRecord(
	resource pcommon.Resource,
	scope pcommon.InstrumentationScope,
	recordAttr pcommon.Map,
	fIndex string,
	otel bool,
	defaultDSType string,
) elasticsearch.Index {
	resourceAttr := resource.Attributes()
	scopeAttr := scope.Attributes()

	// Order:
	// 1. read data_stream.* from attributes
	// 2. read elasticsearch.index.* from attributes
	// 3. receiver-based routing
	// 4. use default hardcoded data_stream.*
	dataset, datasetExists := getFromAttributes(dataStreamDataset, defaultDataStreamDataset, recordAttr, scopeAttr, resourceAttr)
	namespace, namespaceExists := getFromAttributes(dataStreamNamespace, defaultDataStreamNamespace, recordAttr, scopeAttr, resourceAttr)
	dataStreamMode := datasetExists || namespaceExists
	if !dataStreamMode {
		prefix, prefixExists := getFromAttributes(indexPrefix, "", resourceAttr, scopeAttr, recordAttr)
		suffix, suffixExists := getFromAttributes(indexSuffix, "", resourceAttr, scopeAttr, recordAttr)
		if prefixExists || suffixExists {
			return elasticsearch.Index{Index: fmt.Sprintf("%s%s%s", prefix, fIndex, suffix)}
		}
	}

	// Receiver-based routing
	// For example, hostmetricsreceiver (or hostmetricsreceiver.otel in the OTel output mode)
	// for the scope name
	// github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper
	if submatch := receiverRegex.FindStringSubmatch(scope.Name()); len(submatch) > 0 {
		receiverName := submatch[1]
		dataset = receiverName
	}

	// For dataset, the naming convention for datastream is expected to be "logs-[dataset].otel-[namespace]".
	// This is in order to match the built-in logs-*.otel-* index template.
	var datasetSuffix string
	if otel {
		datasetSuffix += ".otel"
	}

	dataset = sanitizeDataStreamField(dataset, disallowedDatasetRunes, datasetSuffix)
	namespace = sanitizeDataStreamField(namespace, disallowedNamespaceRunes, "")
	return elasticsearch.NewDataStreamIndex(defaultDSType, dataset, namespace)
}
