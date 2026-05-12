package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type liveMaintenanceStats struct {
	mu                         sync.RWMutex
	archiveRuns                atomic.Int64
	archiveErrors              atomic.Int64
	pruneRuns                  atomic.Int64
	pruneErrors                atomic.Int64
	blocksArchived             atomic.Int64
	blocksPruned               atomic.Int64
	lastArchiveAttemptUnixNano atomic.Int64
	lastPruneAttemptUnixNano   atomic.Int64
	lastArchiveUnixNano        atomic.Int64
	lastPruneUnixNano          atomic.Int64
	lastArchiveError           string
	lastPruneError             string
}

type liveStatusSnapshot func() map[string]any

const (
	liveMetricP2PPeers       = "p2p_peers"
	liveMetricPeerBestHeight = "peer_best_height"
	liveMetricNextHeight     = "next_height"
	liveMetricArchiveErrors  = "archive_errors"
	liveMetricPruneErrors    = "prune_errors"
	liveMetricInflight       = "blocksync_inflight_requests"
	liveMetricBuffered       = "blocksync_buffered_responses"
	liveMetricColdResponses  = "blocksync_cold_responses"
	liveMetricColdErrors     = "blocksync_cold_errors"
	liveMetricColdQueue      = "blocksync_cold_queue"
	liveMetricColdQueueFull  = "blocksync_cold_queue_full"
	liveMetricColdActive     = "blocksync_cold_active"
	liveMetricArchiveError   = "last_archive_error"
	liveMetricPruneError     = "last_prune_error"
)

func newLiveMaintenanceStats() *liveMaintenanceStats {
	return &liveMaintenanceStats{}
}

func (s *liveMaintenanceStats) recordArchive(blocks int, err error) {
	if s == nil {
		return
	}
	now := time.Now().UTC().UnixNano()
	s.archiveRuns.Add(1)
	s.lastArchiveAttemptUnixNano.Store(now)
	if err != nil {
		s.archiveErrors.Add(1)
		s.setLastArchiveError(err.Error())
		return
	}
	s.setLastArchiveError("")
	s.blocksArchived.Add(int64(blocks))
	s.lastArchiveUnixNano.Store(now)
}

func (s *liveMaintenanceStats) recordPrune(blocks uint64, err error) {
	if s == nil {
		return
	}
	now := time.Now().UTC().UnixNano()
	s.pruneRuns.Add(1)
	s.lastPruneAttemptUnixNano.Store(now)
	if err != nil {
		s.pruneErrors.Add(1)
		s.setLastPruneError(err.Error())
		return
	}
	s.setLastPruneError("")
	s.blocksPruned.Add(int64(blocks))
	s.lastPruneUnixNano.Store(now)
}

func (s *liveMaintenanceStats) snapshot() map[string]any {
	s.mu.RLock()
	lastArchiveError := s.lastArchiveError
	lastPruneError := s.lastPruneError
	s.mu.RUnlock()
	return map[string]any{
		"archive_runs":                   s.archiveRuns.Load(),
		liveMetricArchiveErrors:          s.archiveErrors.Load(),
		"prune_runs":                     s.pruneRuns.Load(),
		liveMetricPruneErrors:            s.pruneErrors.Load(),
		"blocks_archived":                s.blocksArchived.Load(),
		"blocks_pruned":                  s.blocksPruned.Load(),
		"last_archive_attempt_unix_nano": s.lastArchiveAttemptUnixNano.Load(),
		"last_prune_attempt_unix_nano":   s.lastPruneAttemptUnixNano.Load(),
		"last_archive_success_unix_nano": s.lastArchiveUnixNano.Load(),
		"last_prune_success_unix_nano":   s.lastPruneUnixNano.Load(),
		"last_archive_unix_nano":         s.lastArchiveUnixNano.Load(),
		"last_prune_unix_nano":           s.lastPruneUnixNano.Load(),
		liveMetricArchiveError:           lastArchiveError,
		liveMetricPruneError:             lastPruneError,
	}
}

func (s *liveMaintenanceStats) setLastArchiveError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastArchiveError = message
}

func (s *liveMaintenanceStats) setLastPruneError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPruneError = message
}

func startMetricsServer(ctx context.Context, listenAddress string, stats *liveMaintenanceStats, statusSnapshots ...liveStatusSnapshot) (string, error) {
	if listenAddress == "" {
		return "", nil
	}
	if stats == nil {
		stats = newLiveMaintenanceStats()
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return "", err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := w.Write([]byte("ok\n")); err != nil {
			return
		}
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		snapshot := mergedStatusSnapshot(stats, statusSnapshots)
		if !livePeerReady(snapshot) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := w.Write([]byte("ok\n")); err != nil {
			return
		}
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(mergedStatusSnapshot(stats, statusSnapshots)); err != nil {
			http.Error(w, "encode metrics", http.StatusInternalServerError)
			return
		}
	})
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			if closeErr := listener.Close(); closeErr != nil {
				return
			}
		}
	}()
	go func() {
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			if closeErr := listener.Close(); closeErr != nil {
				return
			}
		}
	}()
	return listener.Addr().String(), nil
}

func mergedStatusSnapshot(stats *liveMaintenanceStats, statusSnapshots []liveStatusSnapshot) map[string]any {
	snapshot := stats.snapshot()
	for _, statusSnapshot := range statusSnapshots {
		if statusSnapshot == nil {
			continue
		}
		for key, value := range statusSnapshot() {
			snapshot[key] = value
		}
	}
	return snapshot
}

func livePeerReady(snapshot map[string]any) bool {
	peers, hasPeers := numericMetric(snapshot[liveMetricP2PPeers])
	bestHeight, hasBestHeight := numericMetric(snapshot[liveMetricPeerBestHeight])
	if !hasPeers || peers <= 0 {
		return false
	}
	if !hasBestHeight || bestHeight <= 0 {
		return false
	}
	return true
}

func numericMetric(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}
