// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ecsmapping // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/ecsmapping"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.22.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
)

// resourceAttrsConversionMap contains conversions for resource-level attributes
// from their Semantic Conventions (SemConv) names to equivalent Elastic Common
// Schema (ECS) names.
var resourceAttrsConversionMap = map[string]string{
	semconv.AttributeServiceInstanceID:     "service.node.name",
	semconv.AttributeDeploymentEnvironment: "service.environment",
	semconv.AttributeCloudPlatform:         "cloud.service.name",
	semconv.AttributeContainerImageTags:    "container.image.tag",
	semconv.AttributeHostName:              "host.hostname",
	semconv.AttributeHostArch:              "host.architecture",
	semconv.AttributeProcessExecutablePath: "process.executable",
	semconv.AttributeProcessRuntimeName:    "service.runtime.name",
	semconv.AttributeProcessRuntimeVersion: "service.runtime.version",
	semconv.AttributeOSName:                "host.os.name",
	semconv.AttributeOSType:                "host.os.platform",
	semconv.AttributeOSDescription:         "host.os.full",
	semconv.AttributeOSVersion:             "host.os.version",
	semconv.AttributeK8SDeploymentName:     "kubernetes.deployment.name",
	semconv.AttributeK8SNamespaceName:      "kubernetes.namespace",
	semconv.AttributeK8SNodeName:           "kubernetes.node.name",
	semconv.AttributeK8SPodName:            "kubernetes.pod.name",
	semconv.AttributeK8SPodUID:             "kubernetes.pod.uid",
	semconv.AttributeK8SJobName:            "kubernetes.job.name",
	semconv.AttributeK8SCronJobName:        "kubernetes.cronjob.name",
	semconv.AttributeK8SStatefulSetName:    "kubernetes.statefulset.name",
	semconv.AttributeK8SReplicaSetName:     "kubernetes.replicaset.name",
	semconv.AttributeK8SDaemonSetName:      "kubernetes.daemonset.name",
	semconv.AttributeK8SContainerName:      "kubernetes.container.name",
	semconv.AttributeK8SClusterName:        "orchestrator.cluster.name",
}

// EncodeResource adds fields to doc based on resource attributes.
//
// TODO make handling of unknown fields injectable.
func EncodeResource(
	resource pcommon.Resource, doc *objmodel.Document,
	setAgentFields, setHostOSType bool,
	setUnmappedAttribute func(doc *objmodel.Document, k string, v pcommon.Value),
) {
	var sdkName, sdkLanguage, sdkVersion pcommon.Value
	var distroName, distroVersion pcommon.Value
	var osType, osName pcommon.Value

	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case semconv.AttributeTelemetrySDKName:
			sdkName = v
			return true
		case semconv.AttributeTelemetrySDKLanguage:
			sdkLanguage = v
			return true
		case semconv.AttributeTelemetrySDKVersion:
			sdkVersion = v
			return true
		case semconv.AttributeTelemetryDistroName:
			distroName = v
			return true
		case semconv.AttributeTelemetryDistroVersion:
			distroVersion = v
			return true
		case semconv.AttributeOSType:
			osType = v
		case semconv.AttributeOSName:
			osName = v
		case semconv.AttributeHostName:
			// host.name is mapped as both host.name and
			// host.hostname per resourceAttrsConversionMap
			doc.AddAttribute(k, v)
		}
		if ecsName, ok := resourceAttrsConversionMap[k]; ok {
			doc.AddAttribute(ecsName, v)
		} else {
			// TODO add attribute with original name for ECS,
			// different name for Elastic APM
			setUnmappedAttribute(doc, k, v)
		}
		return true
	})

	// Handle special cases.
	if setAgentFields {
		encodeAgentName(doc, sdkName, sdkLanguage, distroName)
		encodeAgentVersion(doc, distroVersion, sdkVersion)
	}
	if setHostOSType {
		encodeHostOSType(doc, osType, osName)
	}
}

func encodeAgentName(
	doc *objmodel.Document,
	sdkName, sdkLanguage, distroName pcommon.Value,
) {
	var language string
	if sdkLanguage != (pcommon.Value{}) {
		language = sdkLanguage.Str()
	}

	// Construct agent name from telemetry SDK name, language, and distro name.
	agentName := "otlp"
	if sdkName != (pcommon.Value{}) {
		agentName = sdkName.Str()
	}
	if distroName != (pcommon.Value{}) {
		if language == "" {
			language = "unknown"
		}
		agentName += "/" + language + "/" + distroName.Str()
	} else if language != "" {
		agentName += "/" + language
	}

	doc.AddString("agent.name", agentName)
}

func encodeAgentVersion(doc *objmodel.Document, distroVersion, sdkVersion pcommon.Value) {
	if distroVersion != (pcommon.Value{}) {
		doc.AddString("agent.version", distroVersion.Str())
	} else if sdkVersion != (pcommon.Value{}) {
		doc.AddString("agent.version", sdkVersion.Str())
	}
}

func encodeHostOSType(doc *objmodel.Document, osType, osName pcommon.Value) {
	// https://www.elastic.co/guide/en/ecs/current/ecs-os.html#field-os-type:
	//
	// "One of these following values should be used (lowercase): linux, macos, unix, windows.
	// If the OS you’re dealing with is not in the list, the field should not be populated."

	var ecsHostOsType string
	if osType != (pcommon.Value{}) {
		switch str := osType.Str(); str {
		case "windows", "linux":
			ecsHostOsType = str
		case "darwin":
			ecsHostOsType = "macos"
		case "aix", "hpux", "solaris":
			ecsHostOsType = "unix"
		}
	}

	if osName != (pcommon.Value{}) {
		switch str := osName.Str(); str {
		case "Android":
			ecsHostOsType = "android"
		case "iOS":
			ecsHostOsType = "ios"
		}
	}

	doc.AddString("host.os.type", ecsHostOsType)
}
