// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticapmmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// EncodeScope adds fields to doc based on scope attributes.
func EncodeScope(scope pcommon.InstrumentationScope, doc *objmodel.Document) {
	// Only scope name and version are handled, attributes are ignored.
	name := scope.Name()
	if name == "" {
		return
	}
	doc.AddString("service.framework.name", name)
	doc.AddString("service.framework.version", scope.Version())
}
