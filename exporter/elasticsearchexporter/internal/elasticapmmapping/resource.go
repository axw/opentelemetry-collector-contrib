// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticapmmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticapmmapping"

import (
	"regexp"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.25.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

var (
	serviceNameInvalidRegexp = regexp.MustCompile("[^a-zA-Z0-9 _-]")
)

// SetResourceAttribute sets fields or labels based on resource attributes.
//
// This is passed to ecsmapping.EncodeResource for handling fields that are
// not do not require translation for ECS, to avoid mapping some well-known
// attributes to labels.
func SetResourceAttribute(doc *objmodel.Document, k string, v pcommon.Value) {
	switch k {
	// service.*
	case semconv.AttributeServiceName:
		doc.AddString(k, cleanServiceName(v.Str()))
	case semconv.AttributeServiceVersion:
		doc.AddAttribute(k, v)

	// cloud.*
	case semconv.AttributeCloudProvider,
		semconv.AttributeCloudAccountID,
		semconv.AttributeCloudRegion,
		semconv.AttributeCloudAvailabilityZone:
		doc.AddAttribute(k, v)

	// container.*
	case semconv.AttributeContainerName,
		semconv.AttributeContainerID,
		semconv.AttributeContainerImageName,
		semconv.AttributeContainerRuntime,
		"container.image.tag":
		doc.AddAttribute(k, v)

	// host.*
	case semconv.AttributeHostID,
		semconv.AttributeHostType,
		semconv.AttributeHostIP:
		doc.AddAttribute(k, v)

	// process.*
	case semconv.AttributeProcessPID,
		semconv.AttributeProcessCommandLine,
		semconv.AttributeProcessExecutablePath:
		doc.AddAttribute(k, v)
	case semconv.AttributeProcessOwner:
		doc.AddAttribute("user.name", v)

	// device.*
	case semconv.AttributeDeviceID,
		semconv.AttributeDeviceModelIdentifier,
		semconv.AttributeDeviceModelName,
		semconv.AttributeDeviceManufacturer:
		doc.AddAttribute(k, v)

	default:
		SetLabel(doc, k, v)
	}
}

func cleanServiceName(name string) string {
	return serviceNameInvalidRegexp.ReplaceAllString(truncate(name), "_")
}
