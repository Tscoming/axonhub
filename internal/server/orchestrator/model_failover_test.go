package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

type modelFailoverRetryPolicyProvider struct {
	policy *biz.RetryPolicy
}

func (p *modelFailoverRetryPolicyProvider) RetryPolicyOrDefault(context.Context) *biz.RetryPolicy {
	return p.policy
}

func modelFailoverCandidate(channelID, priority int, models ...string) *ChannelModelsCandidate {
	entries := make([]biz.ChannelModelEntry, 0, len(models))
	for _, model := range models {
		entries = append(entries, biz.ChannelModelEntry{ActualModel: model})
	}

	return &ChannelModelsCandidate{
		Channel:  &biz.Channel{Channel: &ent.Channel{ID: channelID}},
		Priority: priority,
		Models:   entries,
	}
}

func openProviderModel(ctx context.Context, cb *biz.ModelCircuitBreaker, channelID int, modelID string) {
	for range biz.DefaultModelCircuitBreakerPolicy().OpenThreshold {
		cb.RecordError(ctx, channelID, modelID, false)
	}
}

func TestLoadBalancedSelector_ModelFailoverDemotesPriorityWhenAllProvidersOpen(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	openProviderModel(ctx, cb, 1, "primary-model")
	openProviderModel(ctx, cb, 2, "primary-model")

	policy := &modelFailoverRetryPolicyProvider{policy: &biz.RetryPolicy{
		Enabled:           true,
		MaxChannelRetries: 2,
		ModelFailover: &biz.ModelFailoverPolicy{
			Enabled:                   true,
			ReserveFallbackPriorities: 1,
		},
	}}
	lb := NewLoadBalancer(policy, nil).WithModelFailoverHealth(cb)
	selector := &LoadBalancedSelector{policy: policy}
	candidates := []*ChannelModelsCandidate{
		modelFailoverCandidate(1, 0, "primary-model"),
		modelFailoverCandidate(2, 0, "primary-model"),
		modelFailoverCandidate(3, 1, "fallback-model"),
	}

	result := selector.sortCandidates(ctx, lb, candidates, &llm.Request{Model: "logical-model"}, 3, false)

	require.Len(t, result, 3)
	require.Equal(t, "fallback-model", result[0].Models[0].ActualModel)
	require.Equal(t, 1, result[0].Priority)
}

func TestLoadBalancedSelector_ModelFailoverKeepsPriorityWhenAnyProviderHealthy(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	openProviderModel(ctx, cb, 1, "primary-model")

	policy := &modelFailoverRetryPolicyProvider{policy: &biz.RetryPolicy{Enabled: true, MaxChannelRetries: 2}}
	lb := NewLoadBalancer(policy, nil).WithModelFailoverHealth(cb)
	selector := &LoadBalancedSelector{policy: policy}
	candidates := []*ChannelModelsCandidate{
		modelFailoverCandidate(1, 0, "primary-model"),
		modelFailoverCandidate(2, 0, "primary-model"),
		modelFailoverCandidate(3, 1, "fallback-model"),
	}

	result := selector.sortCandidates(ctx, lb, candidates, &llm.Request{Model: "logical-model"}, 3, false)

	require.Len(t, result, 3)
	require.Equal(t, 0, result[0].Priority)
	require.Equal(t, 2, result[0].Channel.ID)
}

func TestLoadBalancedSelector_ReservesRetryCandidateForLaterPriority(t *testing.T) {
	ctx := context.Background()
	policy := &modelFailoverRetryPolicyProvider{policy: &biz.RetryPolicy{
		Enabled:           true,
		MaxChannelRetries: 2,
		ModelFailover: &biz.ModelFailoverPolicy{
			Enabled:                   true,
			ReserveFallbackPriorities: 1,
		},
	}}
	lb := NewLoadBalancer(policy, nil).WithModelFailoverHealth(biz.NewModelCircuitBreaker())
	selector := &LoadBalancedSelector{policy: policy}
	candidates := []*ChannelModelsCandidate{
		modelFailoverCandidate(1, 0, "primary-model"),
		modelFailoverCandidate(2, 0, "primary-model"),
		modelFailoverCandidate(3, 0, "primary-model"),
		modelFailoverCandidate(4, 1, "fallback-model"),
	}

	result := selector.sortCandidates(ctx, lb, candidates, &llm.Request{Model: "logical-model"}, 3, false)

	require.Len(t, result, 3)
	require.Equal(t, []int{0, 0, 1}, []int{result[0].Priority, result[1].Priority, result[2].Priority})
}

func TestLoadBalancedSelector_ModelFailoverReordersModelsWithinCandidate(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	openProviderModel(ctx, cb, 1, "primary-model")
	candidate := modelFailoverCandidate(1, 0, "primary-model", "fallback-model")
	candidate.modelAPIFormats = []string{"primary-format", "fallback-format"}
	lb := NewLoadBalancer(&modelFailoverRetryPolicyProvider{policy: &biz.RetryPolicy{}}, nil).
		WithModelFailoverHealth(cb)

	lb.prioritizeCandidatesByModelHealth(ctx, []*ChannelModelsCandidate{candidate})

	require.Equal(t, "fallback-model", candidate.Models[0].ActualModel)
	require.Equal(t, []string{"fallback-format", "primary-format"}, candidate.modelAPIFormats)
}
