package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/streams"
)

type stubResponseStream struct {
	events []*llm.Response
	index  int
}

func (s *stubResponseStream) Next() bool {
	if s.index >= len(s.events) {
		return false
	}
	s.index++
	return true
}

func (s *stubResponseStream) Current() *llm.Response {
	if s.index == 0 || s.index > len(s.events) {
		return nil
	}
	return s.events[s.index-1]
}

func (s *stubResponseStream) Err() error { return nil }

func (s *stubResponseStream) Close() error { return nil }

func TestModelCircuitBreakerTracker_StreamSuccessUsesCurrentChannel(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "provider-gpt-4", false)
	}
	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 42, "provider-gpt-4").State)

	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
				Models: []biz.ChannelModelEntry{{ActualModel: "provider-gpt-4"}},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	stream, err := tracker.OnOutboundLlmStream(ctx, &stubResponseStream{
		events: []*llm.Response{{
			Usage: &llm.Usage{CompletionTokens: 8},
		}},
	})
	require.NoError(t, err)

	require.True(t, stream.Next())
	require.NotNil(t, stream.Current())
	require.NoError(t, stream.Close())

	stats := cb.GetModelCircuitBreakerStats(ctx, 42, "provider-gpt-4")
	require.Equal(t, biz.StateClosed, stats.State)
	require.Zero(t, stats.ConsecutiveFailures)
}

func TestModelCircuitBreakerTracker_SkipCandidateDoesNotRecordError(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
				Models: []biz.ChannelModelEntry{{ActualModel: "gpt-4"}},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	// Force open so the next request is skipped instead of probed.
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "gpt-4", false)
	}
	before := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, biz.StateOpen, before.State)
	failuresBefore := before.ConsecutiveFailures
	lastFailureBefore := before.LastFailureAt

	_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
	require.ErrorIs(t, err, errSkipCandidateByCircuitBreaker)
	tracker.OnOutboundRawError(ctx, err)

	after := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, failuresBefore, after.ConsecutiveFailures)
	require.Equal(t, lastFailureBefore, after.LastFailureAt)
}

func TestModelCircuitBreakerTracker_AllCandidatesSkippedReturnsTypedError(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	candidate := &ChannelModelsCandidate{
		Channel: &biz.Channel{
			Channel: &ent.Channel{ID: 42, Name: "primary"},
		},
		Models: []biz.ChannelModelEntry{{ActualModel: "provider-gpt-4"}},
	}
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel:           "gpt-4",
			ChannelModelsCandidates: []*ChannelModelsCandidate{candidate},
			CurrentCandidate:        candidate,
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyCircuitBreaker,
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "provider-gpt-4", false)
	}

	_, err := tracker.OnOutboundRawRequest(ctx, &httpclient.Request{})
	var unavailableErr *NoAvailableChannelError
	require.ErrorAs(t, err, &unavailableErr)
	require.Equal(t, "gpt-4", unavailableErr.ModelName)
}

func TestModelCircuitBreakerTracker_NonCircuitBreakerSkipsStreamTracking(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	policy := biz.DefaultModelCircuitBreakerPolicy()
	for i := 0; i < policy.OpenThreshold; i++ {
		cb.RecordError(ctx, 42, "gpt-4", false)
	}
	require.Equal(t, biz.StateOpen, cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4").State)

	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "gpt-4",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyAdaptive,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{
					Channel: &ent.Channel{ID: 42, Name: "primary"},
				},
				Models: []biz.ChannelModelEntry{{ActualModel: "gpt-4"}},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	raw := &stubResponseStream{
		events: []*llm.Response{{
			Usage: &llm.Usage{CompletionTokens: 8},
		}},
	}
	stream, err := tracker.OnOutboundLlmStream(ctx, raw)
	require.NoError(t, err)
	require.Equal(t, streams.Stream[*llm.Response](raw), stream)

	require.True(t, stream.Next())
	require.NotNil(t, stream.Current())

	stats := cb.GetModelCircuitBreakerStats(ctx, 42, "gpt-4")
	require.Equal(t, biz.StateOpen, stats.State)
	require.Equal(t, policy.OpenThreshold, stats.ConsecutiveFailures)
}

func TestModelCircuitBreakerTracker_RoundRobinTracksActualModel(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			OriginalModel: "logical-model",
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyRoundRobin,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{Channel: &ent.Channel{ID: 42, Name: "primary"}},
				Models:  []biz.ChannelModelEntry{{ActualModel: "actual-model"}},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	tracker.OnOutboundRawError(ctx, &httpclient.Error{StatusCode: 429})

	require.Equal(t, 1, cb.GetModelCircuitBreakerStats(ctx, 42, "actual-model").ConsecutiveFailures)
	require.Zero(t, cb.GetModelCircuitBreakerStats(ctx, 42, "logical-model").ConsecutiveFailures)
}

func TestModelCircuitBreakerTracker_RoundRobinIgnoresNonRetryableError(t *testing.T) {
	cb := biz.NewModelCircuitBreaker()
	ctx := context.Background()
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			RoutingPolicy: EffectiveRoutingPolicy{
				LoadBalancerStrategy: biz.LoadBalancerStrategyRoundRobin,
			},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{Channel: &ent.Channel{ID: 42, Name: "primary"}},
				Models:  []biz.ChannelModelEntry{{ActualModel: "actual-model"}},
			},
		},
	}
	tracker := withModelCircuitBreaker(outbound, cb).(*modelCircuitBreakerTracker)

	tracker.OnOutboundRawError(ctx, &httpclient.Error{StatusCode: 400})

	require.Zero(t, cb.GetModelCircuitBreakerStats(ctx, 42, "actual-model").ConsecutiveFailures)
}
