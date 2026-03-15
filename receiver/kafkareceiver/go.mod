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

module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver

go 1.14

require (
	github.com/Shopify/sarama v1.26.4
	github.com/golang/protobuf v1.3.5
	github.com/jaegertracing/jaeger v1.17.0
	github.com/open-telemetry/opentelemetry-collector v0.3.1-0.20200503151053-5d1aacc0e168
	github.com/open-telemetry/opentelemetry-proto v0.3.0
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.13.0
)
