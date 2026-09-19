package orchestrator

import (
	"context"
	"errors"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

func withModelCircuitBreaker(outbound *PersistentOutboundTransformer, modelCircuitBreaker *biz.ModelCircuitBreaker) pipeline.Middleware {
	return &modelCircuitBreakerTracker{
		outbound:            outbound,
		modelCircuitBreaker: modelCircuitBreaker,
	}
}

type modelCircuitBreakerTracker struct {
	pipeline.DummyMiddleware

	outbound            *PersistentOutboundTransformer
	modelCircuitBreaker *biz.ModelCircuitBreaker

	probeActive    bool
	probeChannelID int
	probeModelID   string
	skippedCount   int
}

func (m *modelCircuitBreakerTracker) Name() string {
	return "model-circuit-breaker-tracker"
}

func (m *modelCircuitBreakerTracker) OnOutboundRawRequest(ctx context.Context, request *httpclient.Request) (*httpclient.Request, error) {
	if !m.shouldEnforce(ctx) {
		return request, nil
	}

	channel := m.outbound.GetCurrentChannel()
	modelID := m.outbound.GetCurrentModelID()
	if channel == nil || modelID == "" {
		return request, nil
	}

	stats := m.modelCircuitBreaker.GetModelCircuitBreakerStats(ctx, channel.ID, modelID)
	if stats == nil || stats.State != biz.StateOpen {
		return request, nil
	}

	if !m.modelCircuitBreaker.TryBeginProbe(ctx, channel.ID, modelID) {
		m.skippedCount++
		log.Debug(ctx, "skipping candidate by circuit breaker: probe conditions not met or another probe in progress",
			log.Int("channel_id", channel.ID),
			log.String("model_id", modelID),
		)

		if m.skippedCount == len(m.outbound.state.ChannelModelsCandidates) {
			return nil, NewNoAvailableChannelError(m.outbound.state.OriginalModel)
		}

		return nil, errSkipCandidateByCircuitBreaker
	}

	m.probeActive = true
	m.probeChannelID = channel.ID
	m.probeModelID = modelID

	return request, nil
}

func (m *modelCircuitBreakerTracker) OnOutboundLlmResponse(ctx context.Context, response *llm.Response) (*llm.Response, error) {
	if !m.shouldTrack(ctx) {
		return response, nil
	}

	wasProbe := m.probeActive
	m.releaseProbeLease()

	channel := m.outbound.GetCurrentChannel()
	modelID := m.outbound.GetCurrentModelID()
	if channel == nil || modelID == "" {
		return response, nil
	}
	m.modelCircuitBreaker.RecordSuccess(ctx, channel.ID, modelID, wasProbe)

	return response, nil
}

func (m *modelCircuitBreakerTracker) OnOutboundRawError(ctx context.Context, err error) {
	if !m.shouldTrack(ctx) {
		return
	}

	// Capture whether this attempt was an active probe BEFORE releasing the lease,
	// so RecordError can decide whether to apply exponential backoff.
	wasProbe := m.probeActive
	m.releaseProbeLease()

	if errors.Is(err, context.Canceled) {
		return
	}

	// Local skips never reached upstream — must not count as model errors.
	if errors.Is(err, errSkipCandidateByCircuitBreaker) || isChannelQueueError(err) {
		return
	}
	if !isRetryableError(err) {
		return
	}

	channel := m.outbound.GetCurrentChannel()
	modelID := m.outbound.GetCurrentModelID()
	if channel == nil || modelID == "" {
		return
	}
	m.modelCircuitBreaker.RecordError(ctx, channel.ID, modelID, wasProbe)
}

func (m *modelCircuitBreakerTracker) OnOutboundLlmStream(ctx context.Context, stream streams.Stream[*llm.Response]) (streams.Stream[*llm.Response], error) {
	if !m.shouldTrack(ctx) {
		return stream, nil
	}

	channelID := 0
	modelID := m.outbound.GetCurrentModelID()
	if channel := m.outbound.GetCurrentChannel(); channel != nil {
		channelID = channel.ID
	}

	return &probeReleasingStream{
		ctx:       ctx,
		stream:    stream,
		state:     m.outbound.state,
		channelID: channelID,
		modelID:   modelID,
		wasProbe:  m.probeActive,
		release: func() {
			if m.outbound != nil {
				m.releaseProbeLease()
			}
		},
		released:            false,
		recorded:            false,
		modelCircuitBreaker: m.modelCircuitBreaker,
	}, nil
}

func (m *modelCircuitBreakerTracker) shouldTrack(ctx context.Context) bool {
	if m.outbound == nil || m.outbound.state == nil || m.modelCircuitBreaker == nil {
		return false
	}

	strategy := m.outbound.state.RoutingPolicy.LoadBalancerStrategy
	if strategy == biz.LoadBalancerStrategyCircuitBreaker {
		return true
	}

	return strategy == biz.LoadBalancerStrategyRoundRobin && m.modelCircuitBreaker.Enabled(ctx)
}

func (m *modelCircuitBreakerTracker) shouldEnforce(ctx context.Context) bool {
	if !m.shouldTrack(ctx) {
		return false
	}

	strategy := m.outbound.state.RoutingPolicy.LoadBalancerStrategy
	return strategy == biz.LoadBalancerStrategyCircuitBreaker ||
		strategy == biz.LoadBalancerStrategyRoundRobin
}

func (m *modelCircuitBreakerTracker) releaseProbeLease() {
	if m.outbound == nil || m.outbound.state == nil || m.modelCircuitBreaker == nil {
		return
	}

	if !m.probeActive {
		return
	}

	m.modelCircuitBreaker.EndProbe(m.probeChannelID, m.probeModelID)
	m.probeActive = false
}

//nolint:containedctx // Checked.
type probeReleasingStream struct {
	ctx      context.Context
	stream   streams.Stream[*llm.Response]
	state    *PersistenceState
	release  func()
	released bool
	recorded bool

	modelCircuitBreaker *biz.ModelCircuitBreaker
	channelID           int
	modelID             string
	wasProbe            bool
}

func (s *probeReleasingStream) Next() bool {
	return s.stream.Next()
}

func (s *probeReleasingStream) Current() *llm.Response {
	event := s.stream.Current()
	if event == nil {
		return nil
	}

	if s.modelCircuitBreaker == nil {
		return event
	}

	if !s.recorded {
		if tokenCount := event.Usage.GetCompletionTokens(); tokenCount != nil && *tokenCount > 0 {
			if s.channelID != 0 && s.modelID != "" {
				s.modelCircuitBreaker.RecordSuccess(s.ctx, s.channelID, s.modelID, s.wasProbe)
			}
			s.recorded = true
		}
	}

	return event
}

func (s *probeReleasingStream) Err() error {
	return s.stream.Err()
}

func (s *probeReleasingStream) Close() error {
	if !s.released && s.release != nil {
		s.released = true
		s.release()
	}

	return s.stream.Close()
}
