// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"context"
	"fmt"
	"iter"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

type routingKey struct {
	// Topic is the topic name to which the data should be sent.
	Topic string

	// Key is the optional message key to be used for the data.
	Key string
}

type dataRouter[T any] interface {
	// MessageRouter groups the data (plog.Logs etc.) by topic and key,
	// returning a map of routing keys to data. All data with the same
	// routing key will be grouped together.
	routeData(ctx context.Context, data T) (map[routingKey]T, error)
}

type dataRouterFunc[T any] func(ctx context.Context, data T) (map[routingKey]T, error)

func (f dataRouterFunc[T]) routeData(ctx context.Context, data T) (map[routingKey]T, error) {
	return f(ctx, data)
}

// newOTTLLogsRouter creates a new dataRouter that routes logs based
// on the given OTTL expressions. The router will mutate the data.
func newOTTLLogsRouter(topicExpr, keyExpr string, set component.TelemetrySettings) (dataRouter[plog.Logs], error) {
	moveAndAppendTo := func(to, from plog.Logs) {
		from.ResourceLogs().MoveAndAppendTo(to.ResourceLogs())
	}

	splitResources := func(logs plog.Logs) iter.Seq2[plog.Logs, ottlresource.TransformContext] {
		return func(yield func(plog.Logs, ottlresource.TransformContext) bool) {
			for _, resourceLogs := range logs.ResourceLogs().All() {
				newLogs := plog.NewLogs()
				newResourceLogs := newLogs.ResourceLogs().AppendEmpty()
				resourceLogs.MoveTo(newResourceLogs)
				if !yield(newLogs, ottlresource.NewTransformContext(
					newResourceLogs.Resource(),
					newResourceLogs,
				)) {
					return
				}
			}
		}
	}
	splitScopes := func(logs plog.Logs) iter.Seq2[plog.Logs, ottlscope.TransformContext] {
		return func(yield func(plog.Logs, ottlscope.TransformContext) bool) {
			for _, resourceLogs := range logs.ResourceLogs().All() {
				for _, scopeLogs := range resourceLogs.ScopeLogs().All() {
					newLogs := plog.NewLogs()
					newResourceLogs := newLogs.ResourceLogs().AppendEmpty()
					newResourceLogs.SetSchemaUrl(resourceLogs.SchemaUrl())
					resourceLogs.Resource().CopyTo(newResourceLogs.Resource())
					newScopeLogs := newResourceLogs.ScopeLogs().AppendEmpty()
					scopeLogs.MoveTo(newScopeLogs)

					if !yield(newLogs, ottlscope.NewTransformContext(
						newScopeLogs.Scope(),
						newResourceLogs.Resource(),
						newScopeLogs,
					)) {
						return
					}
				}
			}
		}
	}
	splitLogRecords := func(logs plog.Logs) iter.Seq2[plog.Logs, ottllog.TransformContext] {
		return func(yield func(plog.Logs, ottllog.TransformContext) bool) {
			for _, resourceLogs := range logs.ResourceLogs().All() {
				for _, scopeLogs := range resourceLogs.ScopeLogs().All() {
					for _, logRecord := range scopeLogs.LogRecords().All() {
						newLogs := plog.NewLogs()

						newResourceLogs := newLogs.ResourceLogs().AppendEmpty()
						newResourceLogs.SetSchemaUrl(resourceLogs.SchemaUrl())
						resourceLogs.Resource().CopyTo(newResourceLogs.Resource())

						newScopeLogs := newResourceLogs.ScopeLogs().AppendEmpty()
						newScopeLogs.SetSchemaUrl(scopeLogs.SchemaUrl())
						scopeLogs.Scope().CopyTo(newScopeLogs.Scope())
						newLogRecord := newScopeLogs.LogRecords().AppendEmpty()
						logRecord.MoveTo(newLogRecord)

						if !yield(newLogs, ottllog.NewTransformContext(
							newLogRecord,
							newScopeLogs.Scope(),
							newResourceLogs.Resource(),
							newScopeLogs, newResourceLogs,
						)) {
							return
						}
					}
				}
			}
		}
	}

	return newOTTLRouter[plog.Logs](
		topicExpr, keyExpr, set, moveAndAppendTo,
		splitResources, splitScopes, splitLogRecords,
	)
}

func newOTTLRouter[T any](
	topicExpr, keyExpr string, set component.TelemetrySettings, moveAndAppend func(to, from T),
	getResourceContexts func(T) iter.Seq2[T, ottlresource.TransformContext],
	getScopeContexts func(T) iter.Seq2[T, ottlscope.TransformContext],
	getLogRecordContexts func(T) iter.Seq2[T, ottllog.TransformContext],
) (dataRouter[T], error) {
	pc, err := ottl.NewParserCollection(set,
		withOTTLResourceParser[T](set, moveAndAppend, getResourceContexts),
		withOTTLScopeParser[T](set, moveAndAppend, getScopeContexts),
		withOTTLLogRecordParser[T](set, moveAndAppend, getLogRecordContexts),
	)
	if err != nil {
		return nil, err
	}
	router, err := pc.ParseValueExpressions(ottl.NewValueExpressionsGetter([]string{topicExpr, keyExpr}))
	if err != nil {
		return nil, err
	}
	return router, nil
}

func withOTTLResourceParser[T any](
	set component.TelemetrySettings, moveAndAppend func(to, from T),
	getResourceContexts func(T) iter.Seq2[T, ottlresource.TransformContext],
) ottl.ParserCollectionOption[dataRouter[T]] {
	return func(pc *ottl.ParserCollection[dataRouter[T]]) error {
		parser, err := ottlresource.NewParser(
			ottlfuncs.StandardConverters[ottlresource.TransformContext](),
			set, ottlresource.EnablePathContextNames(),
		)
		if err != nil {
			return err
		}
		return ottl.WithParserCollectionContext[ottlresource.TransformContext, dataRouter[T]](
			ottlresource.ContextName, &parser,
			ottl.WithValueExpressionConverter[ottlresource.TransformContext, dataRouter[T]](
				newValueExpressionsConverter(getResourceContexts, moveAndAppend),
			),
		)(pc)
	}
}

func withOTTLScopeParser[T any](
	set component.TelemetrySettings, moveAndAppend func(to, from T),
	getScopeContexts func(T) iter.Seq2[T, ottlscope.TransformContext],
) ottl.ParserCollectionOption[dataRouter[T]] {
	return func(pc *ottl.ParserCollection[dataRouter[T]]) error {
		parser, err := ottlscope.NewParser(
			ottlfuncs.StandardConverters[ottlscope.TransformContext](),
			set, ottlscope.EnablePathContextNames(),
		)
		if err != nil {
			return err
		}
		return ottl.WithParserCollectionContext[ottlscope.TransformContext, dataRouter[T]](
			ottlscope.ContextName, &parser,
			ottl.WithValueExpressionConverter[ottlscope.TransformContext, dataRouter[T]](
				newValueExpressionsConverter(getScopeContexts, moveAndAppend),
			),
		)(pc)
	}
}

func withOTTLLogRecordParser[T any](
	set component.TelemetrySettings, moveAndAppend func(to, from T),
	getLogRecordContexts func(T) iter.Seq2[T, ottllog.TransformContext],
) ottl.ParserCollectionOption[dataRouter[T]] {
	return func(pc *ottl.ParserCollection[dataRouter[T]]) error {
		parser, err := ottllog.NewParser(
			ottlfuncs.StandardConverters[ottllog.TransformContext](),
			set, ottllog.EnablePathContextNames(),
		)
		if err != nil {
			return err
		}
		return ottl.WithParserCollectionContext[ottllog.TransformContext, dataRouter[T]](
			ottllog.ContextName, &parser,
			ottl.WithValueExpressionConverter[ottllog.TransformContext, dataRouter[T]](
				newValueExpressionsConverter(getLogRecordContexts, moveAndAppend),
			),
		)(pc)
	}
}

func newValueExpressionsConverter[K, T any](
	getContexts func(data T) iter.Seq2[T, K],
	moveAndAppend func(to, from T),
) ottl.ParsedValueExpressionsConverter[K, dataRouter[T]] {
	return func(
		pc *ottl.ParserCollection[dataRouter[T]],
		expressions ottl.ValueExpressionsGetter,
		parsedValueExpressions []*ottl.ValueExpression[K],
	) (dataRouter[T], error) {
		return dataRouterFunc[T](func(ctx context.Context, data T) (map[routingKey]T, error) {
			result := make(map[routingKey]T)
			for data, dataContext := range getContexts(data) {
				topic, err := parsedValueExpressions[0].Eval(ctx, dataContext)
				if err != nil {
					return nil, err
				}
				messageKey, err := parsedValueExpressions[1].Eval(ctx, dataContext)
				if err != nil {
					return nil, err
				}
				var rk routingKey
				if topic != nil {
					rk.Topic = fmt.Sprint(topic)
				}
				if messageKey != nil {
					rk.Key = fmt.Sprint(messageKey)
				}

				existing, ok := result[rk]
				if !ok {
					result[rk] = data
				} else {
					moveAndAppend(existing, data)
				}
			}
			return result, nil
		}), nil
	}
}
