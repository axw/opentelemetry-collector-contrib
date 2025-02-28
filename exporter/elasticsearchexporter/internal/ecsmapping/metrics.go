package ecsmapping

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/datapoints"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

func EncodeDataPoints(
	dps []datapoints.DataPoint,
	doc *objmodel.Document,
	validationErrors *[]error,
) {
	// dps is grouped by various properties, including
	// timestamp and attributes (dimensions). Therefore we can
	// refer just to the first datapoint for common fields.
	dp0 := dps[0]
	doc.AddTimestamp("@timestamp", dp0.Timestamp())
	doc.AddAttributes("", dp0.Attributes())
	for _, dp := range dps {
		value, err := dp.Value()
		if err != nil {
			*validationErrors = append(*validationErrors, err)
			continue
		}
		doc.AddAttribute(dp.Metric().Name(), value)
	}
}
