package ecsmapping

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/datapoints"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func EncodeDataPoints(
	dps []datapoints.DataPoint,
	doc *objmodel.Document,
	setAttribute func(doc *objmodel.Document, k string, v pcommon.Value),
	validationErrors *[]error,
) {
	// dps is grouped by various properties, including
	// timestamp and attributes (dimensions). Therefore we can
	// refer just to the first datapoint for common fields.
	dp0 := dps[0]
	doc.AddTimestamp("@timestamp", dp0.Timestamp())
	dp0.Attributes().Range(func(k string, v pcommon.Value) bool {
		setAttribute(doc, k, v)
		return true
	})
	for _, dp := range dps {
		value, err := dp.Value()
		if err != nil {
			*validationErrors = append(*validationErrors, err)
			continue
		}
		doc.AddAttribute(dp.Metric().Name(), value)
	}
}
