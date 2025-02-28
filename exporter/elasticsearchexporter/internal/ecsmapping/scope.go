// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ecsmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/ecsmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// EncodeScope adds fields to doc based on scope attributes.
func EncodeScope(scope pcommon.InstrumentationScope, doc *objmodel.Document) {
	scope.Attributes().Range(func(k string, v pcommon.Value) bool {
		// TODO add attribute with original name for ECS,
		// different name for Elastic APM
		doc.AddAttribute(k, v)
		return true
	})
}
