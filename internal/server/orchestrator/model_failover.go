package orchestrator

import (
	"context"
	"slices"
	"time"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
)

// ModelFailoverHealthProvider exposes per-provider actual-model health.
type ModelFailoverHealthProvider interface {
	Enabled(ctx context.Context) bool
	GetModelCircuitBreakerStats(ctx context.Context, channelID int, modelID string) *biz.ModelCircuitBreakerStats
}

const (
	modelHealthProbeReady = -1
	modelHealthAvailable  = 0
	modelHealthOpen       = 1
)

func candidateActualModel(candidate *ChannelModelsCandidate) string {
	if candidate == nil || len(candidate.Models) == 0 {
		return ""
	}

	return candidate.Models[0].ActualModel
}

func (lb *LoadBalancer) modelHealthRank(ctx context.Context, candidate *ChannelModelsCandidate) int {
	if lb == nil || lb.modelFailoverHealth == nil || !lb.modelFailoverHealth.Enabled(ctx) || candidate == nil || candidate.Channel == nil {
		return modelHealthAvailable
	}

	rank := modelHealthOpen
	for _, entry := range candidate.Models {
		entryRank := lb.modelEntryHealthRank(ctx, candidate.Channel.ID, entry.ActualModel)
		if entryRank < rank {
			rank = entryRank
		}
	}

	if len(candidate.Models) == 0 {
		return modelHealthAvailable
	}

	return rank
}

func (lb *LoadBalancer) modelFailoverEnabled(ctx context.Context) bool {
	return lb != nil && lb.modelFailoverHealth != nil && lb.modelFailoverHealth.Enabled(ctx)
}

func (lb *LoadBalancer) reserveFallbackPriorities(ctx context.Context) int {
	if !lb.modelFailoverEnabled(ctx) || lb.systemService == nil {
		return 0
	}

	policy := lb.systemService.RetryPolicyOrDefault(ctx)
	if policy == nil || policy.ModelFailover == nil {
		return 0
	}

	return policy.ModelFailover.ReserveFallbackPriorities
}

func (lb *LoadBalancer) modelEntryHealthRank(ctx context.Context, channelID int, modelID string) int {
	if modelID == "" {
		return modelHealthAvailable
	}

	stats := lb.modelFailoverHealth.GetModelCircuitBreakerStats(ctx, channelID, modelID)
	if stats == nil || stats.State != biz.StateOpen {
		return modelHealthAvailable
	}
	if !stats.NextProbeAt.IsZero() && !time.Now().Before(stats.NextProbeAt) {
		return modelHealthProbeReady
	}

	return modelHealthOpen
}

func (lb *LoadBalancer) priorityHealthRank(ctx context.Context, candidates []*ChannelModelsCandidate) int {
	probeReady := false
	for _, candidate := range candidates {
		rank := lb.modelHealthRank(ctx, candidate)
		if rank == modelHealthAvailable {
			return modelHealthAvailable
		}
		if rank == modelHealthProbeReady {
			probeReady = true
		}
	}

	if probeReady {
		// A due probe temporarily restores the configured priority. The outbound
		// tracker still admits only one request through the open circuit.
		return modelHealthAvailable
	}

	return modelHealthOpen
}

func (lb *LoadBalancer) orderPrioritiesByModelHealth(
	ctx context.Context,
	priorities []int,
	groups map[int][]*ChannelModelsCandidate,
) {
	configuredPriorities := slices.Clone(priorities)
	slices.SortStableFunc(priorities, func(a, b int) int {
		aRank := lb.priorityHealthRank(ctx, groups[a])
		bRank := lb.priorityHealthRank(ctx, groups[b])
		if aRank != bRank {
			return aRank - bRank
		}
		return a - b
	})
	if !slices.Equal(configuredPriorities, priorities) {
		lb.modelFailoverMetrics.RecordPriorityReorder(ctx)
		log.Debug(ctx, "reordered model priority groups due to provider model health",
			log.Any("configured_priorities", configuredPriorities),
			log.Any("effective_priorities", priorities),
		)
	}
}

func (lb *LoadBalancer) prioritizeCandidatesByModelHealth(
	ctx context.Context,
	candidates []*ChannelModelsCandidate,
) []*ChannelModelsCandidate {
	for _, candidate := range candidates {
		lb.prioritizeCandidateModelsByHealth(ctx, candidate)
	}
	slices.SortStableFunc(candidates, func(a, b *ChannelModelsCandidate) int {
		return lb.modelHealthRank(ctx, a) - lb.modelHealthRank(ctx, b)
	})

	return candidates
}

func (lb *LoadBalancer) prioritizeCandidateModelsByHealth(ctx context.Context, candidate *ChannelModelsCandidate) {
	if candidate == nil || candidate.Channel == nil || len(candidate.Models) <= 1 {
		return
	}

	indices := make([]int, len(candidate.Models))
	for i := range indices {
		indices[i] = i
	}
	slices.SortStableFunc(indices, func(a, b int) int {
		return lb.modelEntryHealthRank(ctx, candidate.Channel.ID, candidate.Models[a].ActualModel) -
			lb.modelEntryHealthRank(ctx, candidate.Channel.ID, candidate.Models[b].ActualModel)
	})

	models := make([]biz.ChannelModelEntry, len(candidate.Models))
	formats := make([]string, len(candidate.modelAPIFormats))
	for target, source := range indices {
		models[target] = candidate.Models[source]
		if source < len(candidate.modelAPIFormats) {
			formats[target] = candidate.modelAPIFormats[source]
		}
	}
	candidate.Models = models
	if len(candidate.modelAPIFormats) > 0 {
		candidate.modelAPIFormats = formats
	}
}
