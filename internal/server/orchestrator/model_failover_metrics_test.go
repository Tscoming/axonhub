package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metricSdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/looplj/axonhub/internal/server/biz"
)

func TestModelFailoverMetrics_NilMeterIsNoop(t *testing.T) {
	m, err := NewModelFailoverMetrics(nil, biz.NewModelCircuitBreaker())
	require.NoError(t, err)

	m.RecordStateTransition(t.Context(), 1, "model", biz.StateClosed, biz.StateOpen)
	m.RecordProbe(t.Context(), 1, "model", "success")
	m.RecordPriorityReorder(t.Context())
}

func TestModelFailoverMetrics_EmitsCircuitProbeAndReorderMetrics(t *testing.T) {
	reader := metricSdk.NewManualReader()
	provider := metricSdk.NewMeterProvider(metricSdk.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	breaker := biz.NewModelCircuitBreaker()
	m, err := NewModelFailoverMetrics(provider.Meter("axonhub-test"), breaker)
	require.NoError(t, err)
	breaker.SetObserver(m)

	policy := biz.DefaultModelCircuitBreakerPolicy()
	for range policy.OpenThreshold {
		breaker.RecordError(t.Context(), 42, "provider-model", false)
	}
	breaker.RecordError(t.Context(), 42, "provider-model", true)
	m.RecordPriorityReorder(t.Context())

	rm := metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Equal(t, int64(2), sumCounter(findCounter(t, rm, "axonhub_model_failover_state_transitions_total")))
	require.Equal(t, int64(1), sumCounter(findCounter(t, rm, "axonhub_model_failover_probe_total")))
	require.Equal(t, int64(1), sumCounter(findCounter(t, rm, "axonhub_model_failover_priority_reorders_total")))

	active := findGauge(t, rm, "axonhub_model_failover_active")
	require.Len(t, active.DataPoints, 2)
	require.Equal(t, int64(1), sumGauge(active))

	breaker.RecordSuccess(t.Context(), 42, "provider-model", true)
	rm = metricdata.ResourceMetrics{}
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Equal(t, int64(3), sumCounter(findCounter(t, rm, "axonhub_model_failover_state_transitions_total")))
	require.Equal(t, int64(2), sumCounter(findCounter(t, rm, "axonhub_model_failover_probe_total")))
	require.Zero(t, sumGauge(findGauge(t, rm, "axonhub_model_failover_active")))
}

func sumCounter(counter metricdata.Sum[int64]) int64 {
	var total int64
	for _, point := range counter.DataPoints {
		total += point.Value
	}

	return total
}

func sumGauge(gauge metricdata.Gauge[int64]) int64 {
	var total int64
	for _, point := range gauge.DataPoints {
		total += point.Value
	}

	return total
}
