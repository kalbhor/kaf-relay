package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

// newTestSourcePool returns a pool with the given servers registered for
// metrics/weight tracking. Tests set curCandidate and targetOffsets directly.
func newTestSourcePool(t *testing.T, servers []Server) *SourcePool {
	t.Helper()

	return &SourcePool{
		log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		metr:         newSourceMetrics(metrics.NewSet(), servers),
		servers:      servers,
		forceCheckCh: make(chan struct{}, 1),
	}
}

func TestSourcePool_getCurCandidate(t *testing.T) {
	tests := []struct {
		name          string
		candidate     Server
		targetOffsets TopicOffsets
		expectedErr   error
		expectedID    int
	}{
		{
			name:        "unhealthy candidate returns no healthy error",
			candidate:   Server{ID: 0, Weight: unhealthyWeight, Healthy: false},
			expectedErr: ErrorNoHealthy,
		},
		{
			name:        "initial placeholder candidate returns no healthy error",
			candidate:   Server{Weight: 1000, Healthy: false},
			expectedErr: ErrorNoHealthy,
		},
		{
			// Regression numbers from a production incident: the trailing
			// node's high watermark was a few dozen records behind the
			// resume offset recorded from the leading node's tip.
			name:          "healthy candidate behind resume offset is refused",
			candidate:     Server{ID: 1, Weight: 22_996_209, Healthy: true},
			targetOffsets: TopicOffsets{0: 22_996_278},
			expectedErr:   ErrCandidateBehind,
		},
		{
			name:          "healthy candidate past resume offset is handed out",
			candidate:     Server{ID: 1, Weight: 22_996_311, Healthy: true},
			targetOffsets: TopicOffsets{0: 22_996_278},
			expectedID:    1,
		},
		{
			// Fetching at the high watermark is valid tailing; only offsets
			// past it are out of range.
			name:          "weight equal to resume offset is handed out",
			candidate:     Server{ID: 0, Weight: 100, Healthy: true},
			targetOffsets: TopicOffsets{0: 100},
			expectedID:    0,
		},
		{
			name:       "no stored offsets always passes",
			candidate:  Server{ID: 0, Weight: 0, Healthy: true},
			expectedID: 0,
		},
		{
			name:          "resume offset is summed across partitions",
			candidate:     Server{ID: 0, Weight: 150, Healthy: true},
			targetOffsets: TopicOffsets{0: 100, 1: 100},
			expectedErr:   ErrCandidateBehind,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := newTestSourcePool(t, []Server{{ID: 0}, {ID: 1}})
			sp.targetOffsets = tt.targetOffsets
			sp.curCandidate = tt.candidate

			s, err := sp.getCurCandidate()
			if tt.expectedErr != nil {
				if !errors.Is(err, tt.expectedErr) {
					t.Fatalf("getCurCandidate() error = %v, want %v", err, tt.expectedErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("getCurCandidate() unexpected error: %v", err)
			}
			if s.ID != tt.expectedID {
				t.Errorf("getCurCandidate() ID = %d, want %d", s.ID, tt.expectedID)
			}
			if sp.lastSentID != tt.expectedID {
				t.Errorf("lastSentID = %d, want %d", sp.lastSentID, tt.expectedID)
			}
		})
	}
}

// TestSourcePool_getCurCandidateCatchUp replays a trailing-node failover:
// the candidate is refused while its high watermark is behind the resume
// offset and handed out once a healthcheck reports it has caught up.
func TestSourcePool_getCurCandidateCatchUp(t *testing.T) {
	sp := newTestSourcePool(t, []Server{{ID: 0}, {ID: 1}})
	sp.targetOffsets = TopicOffsets{0: 1000}
	sp.curCandidate = Server{ID: 1, Weight: 950, Healthy: true}

	if _, err := sp.getCurCandidate(); !errors.Is(err, ErrCandidateBehind) {
		t.Fatalf("getCurCandidate() error = %v, want %v", err, ErrCandidateBehind)
	}

	// Healthcheck tick: the node's tail crosses the resume offset.
	sp.setWeight(1, 1050)

	s, err := sp.getCurCandidate()
	if err != nil {
		t.Fatalf("getCurCandidate() after catch-up: %v", err)
	}
	if s.ID != 1 || s.Weight != 1050 {
		t.Errorf("getCurCandidate() = {ID: %d, Weight: %d}, want {ID: 1, Weight: 1050}", s.ID, s.Weight)
	}
}

// TestSourcePool_GetForcesHealthcheck verifies that Get nudges the healthcheck
// loop when the candidate is refused for lagging behind the resume offset,
// instead of only waiting for the next healthcheck tick.
func TestSourcePool_GetForcesHealthcheck(t *testing.T) {
	sp := newTestSourcePool(t, []Server{{ID: 0}, {ID: 1}})
	sp.cfg = SourcePoolCfg{MaxRetries: 2}
	sp.backoffFn = func(int) time.Duration { return time.Millisecond }
	sp.targetOffsets = TopicOffsets{0: 1000}
	sp.curCandidate = Server{ID: 1, Weight: 950, Healthy: true}

	if _, err := sp.Get(context.Background()); err == nil {
		t.Fatal("Get() error = nil, want retries-exhausted error")
	}

	if len(sp.forceCheckCh) != 1 {
		t.Errorf("forceCheckCh length = %d, want 1 (pending healthcheck nudge)", len(sp.forceCheckCh))
	}
}
