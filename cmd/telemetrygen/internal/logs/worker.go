// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type worker struct {
	numLogs        int          // how many logs the worker has to generate (only when duration==0)
	body           string       // the body of the log
	severityNumber log.Severity // the severityNumber of the log
	severityText   string       // the severityText of the log
	limitPerSecond rate.Limit   // how many logs per second to generate
	logger         *zap.Logger  // logger
}

// simulateLogs generates logs until ctx is done.
func (w worker) simulateLogs(ctx context.Context, logger log.Logger, telemetryAttributes []attribute.KeyValue) error {
	limiter := rate.NewLimiter(w.limitPerSecond, 1)
	var logsEmitted int
	for ; w.numLogs == 0 || logsEmitted < w.numLogs; logsEmitted++ {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}

		var record log.Record
		record.SetTimestamp(time.Now())
		record.SetSeverity(w.severityNumber)
		record.SetSeverityText(w.severityText)
		record.SetBody(log.StringValue(w.body))
		record.AddAttributes(log.String("app", "server"))
		for _, attr := range telemetryAttributes {
			// TODO maintain attribute types
			record.AddAttributes(log.String(string(attr.Key), attr.Value.AsString()))
		}
		logger.Emit(ctx, log.Record{})
	}
	w.logger.Info("logs generated", zap.Int("logs", logsEmitted))
	return nil
}
