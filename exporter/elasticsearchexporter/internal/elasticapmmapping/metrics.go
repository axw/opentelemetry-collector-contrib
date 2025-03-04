// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticapmmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// SetDataPointAttribute sets fields or labels based on data point attributes.
//
// This is passed to ecsmapping.EncodeDataPoints for handling fields that are
// not do not require translation for ECS, to avoid mapping some well-known
// attributes to labels.
func SetDataPointAttribute(doc *objmodel.Document, k string, v pcommon.Value) {
	switch k {
	case "system.process.cpu.start_time",
		"system.process.cmdline",
		"system.process.state",
		"system.filesystem.mount_point",
		"event.dataset",
		"event.module",
		"user.name":
		doc.AddAttribute(k, v)
	default:
		SetLabel(doc, k, v)
	}
}
