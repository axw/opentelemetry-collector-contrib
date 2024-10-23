// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traces

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Run executes the test scenario.
func Run(ctx context.Context, c *Config, tp trace.TracerProvider, logger *zap.Logger) error {
	if err := c.Validate(); err != nil {
		return err
	}

	if c.TotalDuration > 0 {
		c.NumTraces = 0
	}

	limit := rate.Limit(c.Rate)
	if c.Rate == 0 {
		limit = rate.Inf
		logger.Info("generation of traces isn't being throttled")
	} else {
		logger.Info("generation of traces is limited", zap.Float64("per-second", float64(limit)))
	}

	var statusCode codes.Code

	switch strings.ToLower(c.StatusCode) {
	case "0", "unset", "":
		statusCode = codes.Unset
	case "1", "error":
		statusCode = codes.Error
	case "2", "ok":
		statusCode = codes.Ok
	default:
		return fmt.Errorf("expected `status-code` to be one of (Unset, Error, Ok) or (0, 1, 2), got %q instead", c.StatusCode)
	}

	telemetryAttributes := c.GetTelemetryAttributes()
	tracer := tp.Tracer("telemetrygen")
	var g errgroup.Group
	for i := 0; i < c.WorkerCount; i++ {
		w := worker{
			numTraces:        c.NumTraces,
			numChildSpans:    int(math.Max(1, float64(c.NumChildSpans))),
			propagateContext: c.PropagateContext,
			statusCode:       statusCode,
			limitPerSecond:   limit,
			logger:           logger.With(zap.Int("worker", i)),
			loadSize:         c.LoadSize,
			spanDuration:     c.SpanDuration,
		}
		g.Go(func() error {
			err := w.simulateTraces(ctx, tracer, telemetryAttributes)
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		})
	}
	return g.Wait()
}
