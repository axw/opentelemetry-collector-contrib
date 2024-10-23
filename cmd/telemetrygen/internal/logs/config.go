// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"fmt"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel/trace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/common"
)

// Config describes the test scenario.
type Config struct {
	common.Config
	NumLogs        int
	Body           string
	SeverityText   string
	SeverityNumber int32
	TraceID        trace.TraceID
	SpanID         trace.SpanID
}

type traceIDValue trace.TraceID

func (v *traceIDValue) String() string {
	return (*trace.TraceID)(v).String()
}

func (v *traceIDValue) Set(h string) error {
	traceID, err := trace.TraceIDFromHex(h)
	if err != nil {
		return err
	}
	*v = traceIDValue(traceID)
	return nil
}

func (v *traceIDValue) Type() string {
	return "trace-id"
}

type spanIDValue trace.SpanID

func (v *spanIDValue) String() string {
	return (*trace.SpanID)(v).String()
}

func (v *spanIDValue) Set(h string) error {
	spanID, err := trace.SpanIDFromHex(h)
	if err != nil {
		return err
	}
	*v = spanIDValue(spanID)
	return nil
}

func (v *spanIDValue) Type() string {
	return "span-id"
}

// Flags registers config flags.
func (c *Config) Flags(fs *pflag.FlagSet) {
	c.CommonFlags(fs)

	//fs.StringVar(&c.HTTPPath, "otlp-http-url-path", "/v1/logs", "Which URL path to write to")

	fs.IntVar(&c.NumLogs, "logs", 1, "Number of logs to generate in each worker (ignored if duration is provided)")
	fs.StringVar(&c.Body, "body", "the message", "Body of the log")
	fs.StringVar(&c.SeverityText, "severity-text", "Info", "Severity text of the log")
	fs.Int32Var(&c.SeverityNumber, "severity-number", 9, "Severity number of the log, range from 1 to 24 (inclusive)")
	fs.Var((*traceIDValue)(&c.TraceID), "trace-id", "TraceID of the log")
	fs.Var((*spanIDValue)(&c.SpanID), "span-id", "SpanID of the log")
}

// Validate validates the test scenario parameters.
func (c *Config) Validate() error {
	if c.TotalDuration <= 0 && c.NumLogs <= 0 {
		return fmt.Errorf("either `logs` or `duration` must be greater than 0")
	}
	return nil
}
