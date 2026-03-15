// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkareceiver

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	jaegermodel "github.com/jaegertracing/jaeger/model"
	"github.com/open-telemetry/opentelemetry-collector/consumer/pdata"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector/translator/trace/jaeger"
	collectortrace "github.com/open-telemetry/opentelemetry-proto/gen/go/collector/trace/v1"
)

// jaegerProtoUnmarshaler decodes a Jaeger protobuf-encoded Batch.
//
// The message value must be a serialised jaeger.model.Batch proto message.
// This encoding is produced by the Jaeger all-in-one binary and Jaeger
// agents/collectors configured to emit to Kafka.
type jaegerProtoUnmarshaler struct{}

var _ TracesUnmarshaler = (*jaegerProtoUnmarshaler)(nil)

func (j *jaegerProtoUnmarshaler) Encoding() string { return encodingJaegerProto }

func (j *jaegerProtoUnmarshaler) Unmarshal(data []byte) (pdata.Traces, error) {
	var batch jaegermodel.Batch
	if err := proto.Unmarshal(data, &batch); err != nil {
		return pdata.NewTraces(), fmt.Errorf("jaeger_proto: failed to unmarshal proto: %w", err)
	}
	traces := jaegertranslator.ProtoBatchesToInternalTraces([]*jaegermodel.Batch{&batch})
	return traces, nil
}

// otlpProtoUnmarshaler decodes an OTLP protobuf-encoded ExportTraceServiceRequest.
//
// The message value must be a serialised
// opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest proto
// message. This is the canonical format used by the OpenTelemetry SDKs and
// the OTLP exporter.
type otlpProtoUnmarshaler struct{}

var _ TracesUnmarshaler = (*otlpProtoUnmarshaler)(nil)

func (o *otlpProtoUnmarshaler) Encoding() string { return encodingOTLPProto }

func (o *otlpProtoUnmarshaler) Unmarshal(data []byte) (pdata.Traces, error) {
	var req collectortrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return pdata.NewTraces(), fmt.Errorf("otlp_proto: failed to unmarshal proto: %w", err)
	}
	// pdata.TracesFromOtlp wraps the OTLP ResourceSpans slice into the
	// internal pdata.Traces representation without copying the data.
	return pdata.TracesFromOtlp(req.ResourceSpans), nil
}
