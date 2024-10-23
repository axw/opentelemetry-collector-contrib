// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Run executes the test scenario.
func Run(ctx context.Context, cfg *Config, mp metric.MeterProvider, logger *zap.Logger) error {
	logger.Info("starting the metrics generator with configuration", zap.Any("config", cfg))

	if err := cfg.Validate(); err != nil {
		return err
	}

	if cfg.TotalDuration > 0 {
		cfg.NumMetrics = 0
	}

	limit := rate.Limit(cfg.Rate)
	if cfg.Rate == 0 {
		limit = rate.Inf
		logger.Info("generation of metrics isn't being throttled")
	} else {
		logger.Info("generation of metrics is limited", zap.Float64("per-second", float64(limit)))
	}

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cfg.WorkerCount; i++ {
		w := worker{
			numMetrics: cfg.NumMetrics,
			metricName: cfg.MetricName,
			metricType: cfg.MetricType,
			exemplars:  exemplarsFromConfig(cfg),
			//limitPerSecond: limit,
			logger: logger.With(zap.Int("worker", i)),
		}
		g.Go(func() error {
			err := w.simulateMetrics(ctx, mp, cfg.GetTelemetryAttributes())
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		})
	}
	return g.Wait()
}

func exemplarsFromConfig(c *Config) []metricdata.Exemplar[int64] {
	if c.TraceID != "" || c.SpanID != "" {
		var exemplars []metricdata.Exemplar[int64]

		exemplar := metricdata.Exemplar[int64]{
			Value: 1,
			Time:  time.Now(),
		}

		if c.TraceID != "" {
			// we validated this already during the Validate() function for config
			// nolint: errcheck
			traceID, _ := hex.DecodeString(c.TraceID)
			exemplar.TraceID = traceID
		}

		if c.SpanID != "" {
			// we validated this already during the Validate() function for config
			// nolint: errcheck
			spanID, _ := hex.DecodeString(c.SpanID)
			exemplar.SpanID = spanID
		}

		return append(exemplars, exemplar)
	}
	return nil
}
