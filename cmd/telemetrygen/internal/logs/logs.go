// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Run executes the test scenario.
func Run(ctx context.Context, c *Config, lp log.LoggerProvider, logger *zap.Logger) error {
	if err := c.Validate(); err != nil {
		return err
	}

	if c.TotalDuration > 0 {
		c.NumLogs = 0
	}

	limit := rate.Limit(c.Rate)
	if c.Rate == 0 {
		limit = rate.Inf
		logger.Info("generation of logs isn't being throttled")
	} else {
		logger.Info("generation of logs is limited", zap.Float64("per-second", float64(limit)))
	}

	severityText, severityNumber, err := parseSeverity(c.SeverityText, c.SeverityNumber)
	if err != nil {
		return err
	}

	if c.SpanID.IsValid() || c.TraceID.IsValid() {
		spanContext := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: c.TraceID,
			SpanID:  c.SpanID,
			//TraceFlags TraceFlags
			//TraceState TraceState
		})
		ctx = trace.ContextWithSpanContext(ctx, spanContext)
	}

	l := lp.Logger("telemetrygen")
	var g errgroup.Group
	for i := 0; i < c.WorkerCount; i++ {
		w := worker{
			numLogs:        c.NumLogs,
			limitPerSecond: limit,
			body:           c.Body,
			severityText:   severityText,
			severityNumber: severityNumber,
			logger:         logger.With(zap.Int("worker", i)),
		}
		g.Go(func() error {
			err := w.simulateLogs(ctx, l, c.GetTelemetryAttributes())
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		})
	}
	return g.Wait()
}

func parseSeverity(severityText string, severityNumber int32) (string, log.Severity, error) {
	sn := log.Severity(severityNumber)
	if sn < log.SeverityTrace1 || sn > log.SeverityFatal4 {
		return "", log.SeverityUndefined, fmt.Errorf("severity-number is out of range, the valid range is [1,24]")
	}

	// severity number should match well-known severityText
	switch severityText {
	case plog.SeverityNumberTrace.String():
		if !(severityNumber >= 1 && severityNumber <= 4) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [1,4]", severityText, severityNumber)
		}
	case plog.SeverityNumberDebug.String():
		if !(severityNumber >= 5 && severityNumber <= 8) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [5,8]", severityText, severityNumber)
		}
	case plog.SeverityNumberInfo.String():
		if !(severityNumber >= 9 && severityNumber <= 12) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [9,12]", severityText, severityNumber)
		}
	case plog.SeverityNumberWarn.String():
		if !(severityNumber >= 13 && severityNumber <= 16) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [13,16]", severityText, severityNumber)
		}
	case plog.SeverityNumberError.String():
		if !(severityNumber >= 17 && severityNumber <= 20) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [17,20]", severityText, severityNumber)
		}
	case plog.SeverityNumberFatal.String():
		if !(severityNumber >= 21 && severityNumber <= 24) {
			return "", 0, fmt.Errorf("severity text %q does not match severity number %d, the valid range is [21,24]", severityText, severityNumber)
		}
	}

	return severityText, sn, nil
}
