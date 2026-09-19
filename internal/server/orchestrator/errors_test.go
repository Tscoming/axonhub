package orchestrator

import (
	"errors"
	"testing"
)

func TestQuotaExhaustedError(t *testing.T) {
	err := NewQuotaExhaustedError("gpt-4")
	msg := err.Error()
	expected := "all channels quota exhausted for model gpt-4"
	if msg != expected {
		t.Fatalf("unexpected message: got %q, want %q", msg, expected)
	}
	t.Log("PASS: error message correct:", msg)
}

func TestNoAvailableChannelError(t *testing.T) {
	err := NewNoAvailableChannelError("gpt-4")
	expected := "no available channels for model gpt-4: all candidates skipped by circuit breaker"
	if err.Error() != expected {
		t.Fatalf("unexpected message: got %q, want %q", err.Error(), expected)
	}
	if !errors.Is(err, errSkipCandidateByCircuitBreaker) {
		t.Fatal("expected no-available-channel error to retain circuit-breaker skip identity")
	}
}
