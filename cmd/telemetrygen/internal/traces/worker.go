// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package traces // import "github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen/internal/traces"

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type worker struct {
	numTraces        int           // how many traces the worker has to generate (only when duration==0)
	numChildSpans    int           // how many child spans the worker has to generate per trace
	propagateContext bool          // whether the worker needs to propagate the trace context via HTTP headers
	statusCode       codes.Code    // the status code set for the child and parent spans
	limitPerSecond   rate.Limit    // how many spans per second to generate
	loadSize         int           // desired minimum size in MB of string data for each generated trace
	spanDuration     time.Duration // duration of generated spans
	logger           *zap.Logger
}

const (
	fakeIP string = "1.2.3.4"

	// One character takes up one byte of space, so this number comes from the number of bytes in a megabyte
	charactersPerMB = 1024 * 1024
)

func (w *worker) simulateTraces(ctx context.Context, tracer trace.Tracer, telemetryAttributes []attribute.KeyValue) error {
	limiter := rate.NewLimiter(w.limitPerSecond, 1)
	var tracesEmitted int
	for ; w.numTraces == 0 || tracesEmitted < w.numTraces; tracesEmitted++ {
		if err := w.simulateTrace(ctx, limiter, tracer, telemetryAttributes); err != nil {
			return err
		}
	}
	w.logger.Info("traces generated", zap.Int("traces", tracesEmitted))
	return nil
}

func (w *worker) simulateTrace(ctx context.Context, limiter *rate.Limiter, tracer trace.Tracer, telemetryAttributes []attribute.KeyValue) error {
	if err := limiter.Wait(context.Background()); err != nil {
		return err
	}

	spanStart := time.Now()
	spanEnd := spanStart.Add(w.spanDuration)
	ctx, sp := tracer.Start(
		context.Background(),
		"lets-go",
		trace.WithAttributes(
			semconv.NetPeerIPKey.String(fakeIP),
			semconv.PeerServiceKey.String("telemetrygen-server"),
		),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(spanStart),
	)
	sp.SetAttributes(telemetryAttributes...)
	for j := 0; j < w.loadSize; j++ {
		sp.SetAttributes(
			attribute.String(
				fmt.Sprintf("load-%v", j),
				strings.Repeat("*", charactersPerMB),
			),
		)
	}

	childCtx := ctx
	if w.propagateContext {
		header := propagation.HeaderCarrier{}
		// simulates going remote
		otel.GetTextMapPropagator().Inject(childCtx, header)

		// simulates getting a request from a client
		childCtx = otel.GetTextMapPropagator().Extract(childCtx, header)
	}

	for j := 0; j < w.numChildSpans; j++ {
		if err := limiter.Wait(context.Background()); err != nil {
			return err
		}

		_, child := tracer.Start(childCtx, "okey-dokey-"+strconv.Itoa(j), trace.WithAttributes(
			semconv.NetPeerIPKey.String(fakeIP),
			semconv.PeerServiceKey.String("telemetrygen-client"),
		),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithTimestamp(spanStart),
		)
		child.SetAttributes(telemetryAttributes...)

		child.SetStatus(w.statusCode, "")
		child.End(trace.WithTimestamp(spanEnd))

		// Reset the start and end for next span
		spanStart = spanEnd
		spanEnd = spanStart.Add(w.spanDuration)
	}

	sp.SetStatus(w.statusCode, "")
	sp.End(trace.WithTimestamp(spanEnd))
	return nil
}
