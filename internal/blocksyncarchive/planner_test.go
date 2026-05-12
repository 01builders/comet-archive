package blocksyncarchive

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRequestPlannerPlansContiguousMissingHeights(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 8}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	requests, err := planner.Plan(5, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []Request{
		{PeerID: "peer-a", Height: 5},
		{PeerID: "peer-a", Height: 6},
		{PeerID: "peer-b", Height: 7},
	}
	if len(requests) != len(want) {
		t.Fatalf("got %d requests, want %d: %+v", len(requests), len(want), requests)
	}
	for i := range want {
		if requests[i] != want[i] {
			t.Fatalf("request %d got %+v want %+v", i, requests[i], want[i])
		}
	}
}

func TestRequestPlannerTracksInflightAndRetriesFailures(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("got %d first requests, want 2", len(first))
	}
	if peerID, ok := planner.InflightPeer(1); !ok || peerID != "peer" {
		t.Fatalf("inflight peer for height 1 = %q/%v, want peer/true", peerID, ok)
	}
	second, err := planner.Plan(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || second[0].Height != 3 || second[1].Height != 4 {
		t.Fatalf("unexpected second requests: %+v", second)
	}
	planner.MarkFailed(1)
	retry, err := planner.Plan(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("unexpected retry requests: %+v", retry)
	}
}

func TestRequestPlannerFailsOverAfterNoBlockResponse(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer-a", 2)
	second, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].PeerID != "peer-b" || second[0].Height != 2 {
		t.Fatalf("unexpected failover request: %+v", second)
	}
}

func TestRequestPlannerIgnoresNoBlockFromWrongPeer(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer-b", 2)
	if peerID, ok := planner.InflightPeer(2); !ok || peerID != "peer-a" {
		t.Fatalf("inflight peer after wrong no-block = %q/%v, want peer-a/true", peerID, ok)
	}
	next, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 0 {
		t.Fatalf("unexpected next request after wrong no-block: %+v", next)
	}
	planner.RemovePeer("peer-a")
	failover, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(failover) != 1 || failover[0].PeerID != "peer-b" || failover[0].Height != 2 {
		t.Fatalf("unexpected failover after wrong no-block: %+v", failover)
	}
}

func TestRequestPlannerSkipsReceivedBufferedHeightsWithinWindow(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || first[0].Height != 1 || first[1].Height != 2 || first[2].Height != 3 {
		t.Fatalf("unexpected first requests: %+v", first)
	}
	planner.MarkReceived(2)
	second, err := planner.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("unexpected duplicate requests while window full: %+v", second)
	}
	planner.MarkDone(1)
	third, err := planner.Plan(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 1 || third[0].Height != 4 {
		t.Fatalf("unexpected request after advancing window: %+v", third)
	}
}

func TestRequestPlannerExpiresInflightAndPrefersAlternatePeer(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(2, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(time.Second), 2*time.Second); expired != 0 {
		t.Fatalf("expired %d early requests, want 0", expired)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	second, err := planner.PlanAt(2, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].PeerID != "peer-b" || second[0].Height != 2 {
		t.Fatalf("unexpected timeout failover request: %+v", second)
	}
}

func TestRequestPlannerRetriesTimedOutPeerWhenItIsTheOnlySource(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(2, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer-a" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	retry, err := planner.PlanAt(2, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer-a" || retry[0].Height != 2 {
		t.Fatalf("unexpected timeout retry request: %+v", retry)
	}
}

func TestRequestPlannerPrunesStaleNoBlockAndTimeoutMarkers(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 6}); err != nil {
		t.Fatal(err)
	}
	planner.MarkNoBlock("peer-a", 2)
	now := time.Unix(100, 0)
	first, err := planner.PlanAt(3, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Height != 3 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	if expired := planner.ExpireInflight(now.Add(3*time.Second), 2*time.Second); expired != 1 {
		t.Fatalf("expired %d requests, want 1", expired)
	}
	if _, ok := planner.noBlocks["peer-a"][2]; ok {
		t.Fatal("no-block marker below next height was not pruned")
	}
	if _, ok := planner.timeouts["peer-a"][3]; !ok {
		t.Fatal("timeout marker for pending height was not recorded")
	}
	retry, err := planner.PlanAt(4, 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].Height != 4 {
		t.Fatalf("unexpected retry request: %+v", retry)
	}
	if _, ok := planner.timeouts["peer-a"][3]; ok {
		t.Fatal("timeout marker below next height was not pruned")
	}
}

func TestRequestPlannerClearsNoBlockMarkersWhenPeerRangeChanges(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	first, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].PeerID != "peer" || first[0].Height != 2 {
		t.Fatalf("unexpected first request: %+v", first)
	}
	planner.MarkNoBlock("peer", 2)
	if upsertErr := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 5}); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	retry, err := planner.Plan(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer" || retry[0].Height != 2 {
		t.Fatalf("unexpected retry after range update: %+v", retry)
	}
}

func TestRequestPlannerClearsInflightWhenPeerRangeContracts(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	if err := planner.UpsertPeer("peer-b", PeerRange{Base: 1, Height: 4}); err != nil {
		t.Fatal(err)
	}
	requests, err := planner.Plan(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].PeerID != "peer-a" || requests[0].Height != 3 {
		t.Fatalf("unexpected first request: %+v", requests)
	}
	if upsertErr := planner.UpsertPeer("peer-a", PeerRange{Base: 1, Height: 2}); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	retry, err := planner.Plan(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(retry) != 1 || retry[0].PeerID != "peer-b" || retry[0].Height != 3 {
		t.Fatalf("unexpected retry after peer range contraction: %+v", retry)
	}
}

func TestRequestPlannerReportsUnfillableGap(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 3, Height: 5}); err != nil {
		t.Fatal(err)
	}
	_, err := planner.Plan(1, 1)
	if !errors.Is(err, ErrNoPeerForHeight) {
		t.Fatalf("got err %v, want ErrNoPeerForHeight", err)
	}
}

func TestRequestPlannerRejectsExcessiveLimit(t *testing.T) {
	planner := NewRequestPlanner()
	if err := planner.UpsertPeer("peer", PeerRange{Base: 1, Height: 5}); err != nil {
		t.Fatal(err)
	}
	_, err := planner.Plan(1, MaxRequestLimit+1)
	if err == nil || !strings.Contains(err.Error(), "request limit") {
		t.Fatalf("Plan err=%v, want request limit error", err)
	}
}
