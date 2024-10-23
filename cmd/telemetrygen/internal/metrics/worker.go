// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

type worker struct {
	metricName string                       // name of metric to generate
	metricType metricType                   // type of metric to generate
	exemplars  []metricdata.Exemplar[int64] // exemplars to attach to the metric
	numMetrics int                          // how many metrics the worker has to generate
	logger     *zap.Logger                  // logger
}

func (w worker) simulateMetrics(
	ctx context.Context,
	mp metric.MeterProvider,
	signalAttrs []attribute.KeyValue,
) error {
	var metricsEmitted int
	meter := mp.Meter("telemetrygen")
	switch w.metricType {
	case metricTypeGauge:
		_, err := meter.Float64ObservableGauge(
			w.metricName,
			metric.WithFloat64Callback(func(ctx context.Context, o metric.Float64Observer) error {
				for i := 0; i < w.numMetrics; i++ {
					metricsEmitted++
					o.Observe(
						1.23,
						metric.WithAttributes(signalAttrs...),
						metric.WithAttributes(attribute.Int("index", i)),
					)
				}
				return nil
			}),
		)
		if err != nil {
			return err
		}
	case metricTypeSum:
		_, err := meter.Int64ObservableCounter(
			w.metricName,
			metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
				for i := 0; i < w.numMetrics; i++ {
					metricsEmitted++
					o.Observe(
						1,
						metric.WithAttributes(signalAttrs...),
						metric.WithAttributes(attribute.Int("index", i)),
					)
				}
				return nil
			}),
		)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unhandled metric type %q", w.metricType)
	}
	<-ctx.Done()
	w.logger.Info("metrics generated", zap.Int("metrics", metricsEmitted))
	return ctx.Err()
}
