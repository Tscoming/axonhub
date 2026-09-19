package orchestrator

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/looplj/axonhub/internal/server/biz"
)

// ModelFailoverMetrics exposes model circuit-breaker and priority failover events.
type ModelFailoverMetrics struct {
	stateTransitions metric.Int64Counter
	probes           metric.Int64Counter
	priorityReorders metric.Int64Counter
}

func NewModelFailoverMetrics(meter metric.Meter, breaker *biz.ModelCircuitBreaker) (*ModelFailoverMetrics, error) {
	if meter == nil {
		return &ModelFailoverMetrics{}, nil
	}

	stateTransitions, err := meter.Int64Counter(
		"axonhub_model_failover_state_transitions_total",
		metric.WithDescription("Model provider circuit-breaker state transitions"),
		metric.WithUnit("transitions"),
	)
	if err != nil {
		return nil, fmt.Errorf("create model failover state transition counter: %w", err)
	}

	probes, err := meter.Int64Counter(
		"axonhub_model_failover_probe_total",
		metric.WithDescription("Model provider recovery probe events by outcome"),
		metric.WithUnit("events"),
	)
	if err != nil {
		return nil, fmt.Errorf("create model failover probe counter: %w", err)
	}

	priorityReorders, err := meter.Int64Counter(
		"axonhub_model_failover_priority_reorders_total",
		metric.WithDescription("Requests whose model priority groups were reordered by health"),
		metric.WithUnit("requests"),
	)
	if err != nil {
		return nil, fmt.Errorf("create model failover priority reorder counter: %w", err)
	}

	active, err := meter.Int64ObservableGauge(
		"axonhub_model_failover_active",
		metric.WithDescription("Current number of degraded or open provider models by state"),
		metric.WithUnit("models"),
	)
	if err != nil {
		return nil, fmt.Errorf("create model failover active gauge: %w", err)
	}

	if breaker != nil {
		_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
			counts := map[biz.CircuitBreakerState]int64{
				biz.StateHalfOpen: 0,
				biz.StateOpen:     0,
			}
			for _, stats := range breaker.GetAllNonClosedModels(ctx) {
				counts[stats.State]++
			}
			for state, count := range counts {
				observer.ObserveInt64(active, count, metric.WithAttributes(attribute.String("state", string(state))))
			}

			return nil
		}, active)
		if err != nil {
			return nil, fmt.Errorf("register model failover active gauge callback: %w", err)
		}
	}

	return &ModelFailoverMetrics{
		stateTransitions: stateTransitions,
		probes:           probes,
		priorityReorders: priorityReorders,
	}, nil
}

func (m *ModelFailoverMetrics) RecordStateTransition(
	ctx context.Context,
	channelID int,
	modelID string,
	from, to biz.CircuitBreakerState,
) {
	if m == nil || m.stateTransitions == nil {
		return
	}

	m.stateTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("channel_id", channelID),
		attribute.String("model_id", modelID),
		attribute.String("from_state", string(from)),
		attribute.String("to_state", string(to)),
	))
}

func (m *ModelFailoverMetrics) RecordProbe(ctx context.Context, channelID int, modelID, outcome string) {
	if m == nil || m.probes == nil {
		return
	}

	m.probes.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("channel_id", channelID),
		attribute.String("model_id", modelID),
		attribute.String("outcome", outcome),
	))
}

func (m *ModelFailoverMetrics) RecordPriorityReorder(ctx context.Context) {
	if m == nil || m.priorityReorders == nil {
		return
	}

	m.priorityReorders.Add(ctx, 1)
}
