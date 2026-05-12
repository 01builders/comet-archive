package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	soakStatusPassed = "passed"
	soakStatusFailed = "failed"
)

type soakOptions struct {
	MetricsURL            string
	Duration              time.Duration
	Interval              time.Duration
	MinPeers              int64
	RequireHeadAdvance    bool
	MaxArchiveErrorsDelta int64
	MaxPruneErrorsDelta   int64
	MinColdResponsesDelta int64
	MaxColdErrorsDelta    int64
	MaxColdQueueFullDelta int64
	MaxColdQueue          int64
	MaxBufferedResponses  int64
	MaxHeadLag            int64
	HTTPClient            *http.Client
}

type soakResult struct {
	Status               string `json:"status"`
	MetricsURL           string `json:"metrics_url"`
	Samples              int    `json:"samples"`
	ReadySamples         int    `json:"ready_samples"`
	FirstPeerBestHeight  int64  `json:"first_peer_best_height"`
	LastPeerBestHeight   int64  `json:"last_peer_best_height"`
	LastNextHeight       int64  `json:"last_next_height,omitempty"`
	MaxHeadLag           int64  `json:"max_head_lag,omitempty"`
	MaxPeers             int64  `json:"max_peers"`
	ArchiveErrorsDelta   int64  `json:"archive_errors_delta"`
	PruneErrorsDelta     int64  `json:"prune_errors_delta"`
	ColdResponsesDelta   int64  `json:"cold_responses_delta"`
	ColdErrorsDelta      int64  `json:"cold_errors_delta"`
	ColdQueueFullDelta   int64  `json:"cold_queue_full_delta"`
	MaxColdQueue         int64  `json:"max_cold_queue"`
	MaxBufferedResponses int64  `json:"max_buffered_responses"`
	LastArchiveError     string `json:"last_archive_error"`
	LastPruneError       string `json:"last_prune_error"`
}

type soakSample struct {
	Ready          bool
	Peers          int64
	PeerBestHeight int64
	NextHeight     int64
	HasNextHeight  bool
	ArchiveErrors  int64
	PruneErrors    int64
	ColdResponses  int64
	ColdErrors     int64
	ColdQueueFull  int64
	ColdQueue      int64
	Buffered       int64
	ArchiveError   string
	PruneError     string
}

func runSoakCheck(ctx context.Context, opts soakOptions) (soakResult, error) {
	if opts.MetricsURL == "" {
		return soakResult{}, errors.New("metrics-url is required")
	}
	baseURL, err := normalizeMetricsURL(opts.MetricsURL)
	if err != nil {
		return soakResult{}, err
	}
	if opts.Duration <= 0 {
		return soakResult{}, errors.New("duration must be positive")
	}
	if opts.Interval <= 0 {
		return soakResult{}, errors.New("interval must be positive")
	}
	if err := validateSoakThresholds(opts); err != nil {
		return soakResult{}, err
	}
	if opts.MinPeers <= 0 {
		opts.MinPeers = 1
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}

	deadline := time.Now().Add(opts.Duration)
	result := soakResult{Status: soakStatusFailed, MetricsURL: baseURL}
	var first soakSample
	var last soakSample
	for {
		sample, sampleErr := readSoakSample(ctx, opts.HTTPClient, baseURL)
		if sampleErr != nil {
			return result, sampleErr
		}
		if result.Samples == 0 {
			first = sample
		}
		last = sample
		result.Samples++
		if sample.Ready {
			result.ReadySamples++
		}
		if sample.Peers > result.MaxPeers {
			result.MaxPeers = sample.Peers
		}
		if sample.ColdQueue > result.MaxColdQueue {
			result.MaxColdQueue = sample.ColdQueue
		}
		if sample.Buffered > result.MaxBufferedResponses {
			result.MaxBufferedResponses = sample.Buffered
		}
		if sample.HasNextHeight {
			headLag := sample.PeerBestHeight - sample.NextHeight + 1
			if headLag < 0 {
				headLag = 0
			}
			if headLag > result.MaxHeadLag {
				result.MaxHeadLag = headLag
			}
		}
		if !time.Now().Before(deadline) {
			break
		}
		sleep := opts.Interval
		if remaining := time.Until(deadline); remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}

	result.Status = soakStatusPassed
	result.MetricsURL = baseURL
	result.FirstPeerBestHeight = first.PeerBestHeight
	result.LastPeerBestHeight = last.PeerBestHeight
	if last.HasNextHeight {
		result.LastNextHeight = last.NextHeight
	}
	result.ArchiveErrorsDelta = last.ArchiveErrors - first.ArchiveErrors
	result.PruneErrorsDelta = last.PruneErrors - first.PruneErrors
	result.ColdResponsesDelta = last.ColdResponses - first.ColdResponses
	result.ColdErrorsDelta = last.ColdErrors - first.ColdErrors
	result.ColdQueueFullDelta = last.ColdQueueFull - first.ColdQueueFull
	result.LastArchiveError = last.ArchiveError
	result.LastPruneError = last.PruneError

	if result.ReadySamples != result.Samples {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("ready samples %d, expected %d", result.ReadySamples, result.Samples)
	}
	if result.MaxPeers < opts.MinPeers {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("max peers %d below required %d", result.MaxPeers, opts.MinPeers)
	}
	if opts.RequireHeadAdvance && result.LastPeerBestHeight <= result.FirstPeerBestHeight {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("peer best height did not advance: first=%d last=%d", result.FirstPeerBestHeight, result.LastPeerBestHeight)
	}
	if result.ArchiveErrorsDelta > opts.MaxArchiveErrorsDelta {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("archive errors increased by %d, allowed %d", result.ArchiveErrorsDelta, opts.MaxArchiveErrorsDelta)
	}
	if result.PruneErrorsDelta > opts.MaxPruneErrorsDelta {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("prune errors increased by %d, allowed %d", result.PruneErrorsDelta, opts.MaxPruneErrorsDelta)
	}
	if result.ColdResponsesDelta < opts.MinColdResponsesDelta {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("cold responses increased by %d, required at least %d", result.ColdResponsesDelta, opts.MinColdResponsesDelta)
	}
	if result.ColdErrorsDelta > opts.MaxColdErrorsDelta {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("cold serving errors increased by %d, allowed %d", result.ColdErrorsDelta, opts.MaxColdErrorsDelta)
	}
	if result.ColdQueueFullDelta > opts.MaxColdQueueFullDelta {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("cold queue full events increased by %d, allowed %d", result.ColdQueueFullDelta, opts.MaxColdQueueFullDelta)
	}
	if opts.MaxColdQueue >= 0 && result.MaxColdQueue > opts.MaxColdQueue {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("max cold queue %d exceeded allowed %d", result.MaxColdQueue, opts.MaxColdQueue)
	}
	if opts.MaxBufferedResponses >= 0 && result.MaxBufferedResponses > opts.MaxBufferedResponses {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("max buffered responses %d exceeded allowed %d", result.MaxBufferedResponses, opts.MaxBufferedResponses)
	}
	if opts.MaxHeadLag >= 0 {
		if !last.HasNextHeight {
			result.Status = soakStatusFailed
			return result, fmt.Errorf("max-head-lag requires numeric metric %q", liveMetricNextHeight)
		}
		if result.MaxHeadLag > opts.MaxHeadLag {
			result.Status = soakStatusFailed
			return result, fmt.Errorf("max head lag %d exceeded allowed %d", result.MaxHeadLag, opts.MaxHeadLag)
		}
	}
	if result.LastArchiveError != "" {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("last archive error is not clear: %s", result.LastArchiveError)
	}
	if result.LastPruneError != "" {
		result.Status = soakStatusFailed
		return result, fmt.Errorf("last prune error is not clear: %s", result.LastPruneError)
	}
	return result, nil
}

func validateSoakThresholds(opts soakOptions) error {
	if opts.MinPeers < 0 {
		return errors.New("min peers cannot be negative")
	}
	if opts.MaxArchiveErrorsDelta < 0 {
		return errors.New("max archive errors delta cannot be negative")
	}
	if opts.MaxPruneErrorsDelta < 0 {
		return errors.New("max prune errors delta cannot be negative")
	}
	if opts.MinColdResponsesDelta < 0 {
		return errors.New("min cold responses delta cannot be negative")
	}
	if opts.MaxColdErrorsDelta < 0 {
		return errors.New("max cold errors delta cannot be negative")
	}
	if opts.MaxColdQueueFullDelta < 0 {
		return errors.New("max cold queue full delta cannot be negative")
	}
	if opts.MaxColdQueue < -1 {
		return errors.New("max cold queue must be -1 or greater")
	}
	if opts.MaxBufferedResponses < -1 {
		return errors.New("max buffered responses must be -1 or greater")
	}
	if opts.MaxHeadLag < -1 {
		return errors.New("max head lag must be -1 or greater")
	}
	return nil
}

func normalizeMetricsURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("metrics-url must use http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", errors.New("metrics-url must include a host")
	}
	return strings.TrimRight(rawURL, "/"), nil
}

func readSoakSample(ctx context.Context, client *http.Client, baseURL string) (soakSample, error) {
	readyResp, err := getSoakURL(ctx, client, baseURL+"/readyz")
	if err != nil {
		return soakSample{}, err
	}
	ready := readyResp.StatusCode == http.StatusOK
	_ = readyResp.Body.Close()

	metricsResp, err := getSoakURL(ctx, client, baseURL+"/metrics")
	if err != nil {
		return soakSample{}, err
	}
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusOK {
		return soakSample{}, fmt.Errorf("metrics status %d", metricsResp.StatusCode)
	}
	var metrics map[string]any
	if decodeErr := json.NewDecoder(metricsResp.Body).Decode(&metrics); decodeErr != nil {
		return soakSample{}, decodeErr
	}
	peers, err := requiredIntMetric(metrics, liveMetricP2PPeers)
	if err != nil {
		return soakSample{}, err
	}
	peerBestHeight, err := requiredIntMetric(metrics, liveMetricPeerBestHeight)
	if err != nil {
		return soakSample{}, err
	}
	archiveErrors, err := requiredIntMetric(metrics, liveMetricArchiveErrors)
	if err != nil {
		return soakSample{}, err
	}
	pruneErrors, err := requiredIntMetric(metrics, liveMetricPruneErrors)
	if err != nil {
		return soakSample{}, err
	}
	coldResponses, err := requiredIntMetric(metrics, liveMetricColdResponses)
	if err != nil {
		return soakSample{}, err
	}
	coldErrors, err := requiredIntMetric(metrics, liveMetricColdErrors)
	if err != nil {
		return soakSample{}, err
	}
	coldQueue, err := requiredIntMetric(metrics, liveMetricColdQueue)
	if err != nil {
		return soakSample{}, err
	}
	coldQueueFull, err := requiredIntMetric(metrics, liveMetricColdQueueFull)
	if err != nil {
		return soakSample{}, err
	}
	buffered, err := requiredIntMetric(metrics, liveMetricBuffered)
	if err != nil {
		return soakSample{}, err
	}
	nextHeight, hasNextHeight := optionalIntMetric(metrics, liveMetricNextHeight)
	return soakSample{
		Ready:          ready,
		Peers:          peers,
		PeerBestHeight: peerBestHeight,
		NextHeight:     nextHeight,
		HasNextHeight:  hasNextHeight,
		ArchiveErrors:  archiveErrors,
		PruneErrors:    pruneErrors,
		ColdResponses:  coldResponses,
		ColdErrors:     coldErrors,
		ColdQueueFull:  coldQueueFull,
		ColdQueue:      coldQueue,
		Buffered:       buffered,
		ArchiveError:   optionalStringMetric(metrics, liveMetricArchiveError),
		PruneError:     optionalStringMetric(metrics, liveMetricPruneError),
	}, nil
}

func getSoakURL(ctx context.Context, client *http.Client, endpoint string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func requiredIntMetric(metrics map[string]any, name string) (int64, error) {
	value, ok := numericMetric(metrics[name])
	if !ok {
		return 0, fmt.Errorf("missing numeric metric %q", name)
	}
	return int64(value), nil
}

func optionalIntMetric(metrics map[string]any, name string) (int64, bool) {
	value, ok := numericMetric(metrics[name])
	if !ok {
		return 0, false
	}
	return int64(value), true
}

func optionalStringMetric(metrics map[string]any, name string) string {
	value, ok := metrics[name].(string)
	if !ok {
		return ""
	}
	return value
}
