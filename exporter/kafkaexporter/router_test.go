package kafkaexporter

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/testdata"
)

func TestOTTLLogsRouter(t *testing.T) {
	router, err := newOTTLLogsRouter(
		`resource.attributes["resource-attr"]`,
		`log.trace_id`,
		componenttest.NewNopTelemetrySettings(),
	)
	require.NoError(t, err)
	require.NotNil(t, router)

	logs := testdata.GenerateLogs(4)

	routedLogs, err := router.routeData(context.Background(), logs)
	require.NoError(t, err)
	require.Equal(t, 2, len(routedLogs))

	assert.ElementsMatch(t,
		[]routingKey{
			{Topic: "resource-attr-val-1", Key: ""},
			{Topic: "resource-attr-val-1", Key: "08040201000000000000000000000000"},
		},
		slices.Collect(maps.Keys(routedLogs)),
	)
	for _, logs := range routedLogs {
		assert.Equal(t, 2, logs.LogRecordCount())

	}
}
