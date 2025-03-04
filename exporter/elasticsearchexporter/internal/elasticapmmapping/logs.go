// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticapmmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// SetLogRecordAttribute sets fields or labels based on log record attributes.
//
// This is passed to ecsmapping.EncodeLogRecord for handling fields that are
// not do not require translation for ECS, to avoid mapping some well-known
// attributes to labels.
func SetLogRecordAttribute(doc *objmodel.Document, k string, v pcommon.Value) {
	switch k {
	// TODO handle well-known Elastic APM specific translations
	default:
		SetLabel(doc, k, v)
	}
}
