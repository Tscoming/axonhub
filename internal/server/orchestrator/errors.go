package orchestrator

import "fmt"

// QuotaExhaustedError is returned when all channels are quota exhausted for a model.
type QuotaExhaustedError struct {
	ModelName string
}

func (e *QuotaExhaustedError) Error() string {
	return fmt.Sprintf("all channels quota exhausted for model %s", e.ModelName)
}

func NewQuotaExhaustedError(modelName string) error {
	return &QuotaExhaustedError{ModelName: modelName}
}

// NoAvailableChannelError is returned when no selected channel can serve a model.
type NoAvailableChannelError struct {
	ModelName string
}

func (e *NoAvailableChannelError) Error() string {
	return fmt.Sprintf("no available channels for model %s: all candidates skipped by circuit breaker", e.ModelName)
}

func (e *NoAvailableChannelError) Unwrap() error {
	return errSkipCandidateByCircuitBreaker
}

func NewNoAvailableChannelError(modelName string) error {
	return &NoAvailableChannelError{ModelName: modelName}
}
