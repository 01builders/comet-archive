package blocksyncarchive

import (
	"errors"
	"fmt"
	"testing"
	"time"

	cmtblocksync "github.com/cometbft/cometbft/blocksync"
	"github.com/cometbft/cometbft/p2p"
	bcproto "github.com/cometbft/cometbft/proto/tendermint/blocksync"
	"github.com/cometbft/cometbft/types"
)

func peerStopped(p *fakePeer) bool {
	select {
	case <-p.quit:
		return true
	default:
		return false
	}
}

func receiveBlock(t *testing.T, r *Reactor, src p2p.Peer, block *types.Block) {
	t.Helper()
	pb, err := block.ToProto()
	if err != nil {
		t.Fatal(err)
	}
	r.Receive(p2p.Envelope{Src: src, ChannelID: cmtblocksync.BlocksyncChannel, Message: &bcproto.BlockResponse{Block: pb}})
}

func receiveStatus(r *Reactor, src p2p.Peer, base, height int64) {
	r.Receive(p2p.Envelope{Src: src, ChannelID: cmtblocksync.BlocksyncChannel, Message: &bcproto.StatusResponse{Base: base, Height: height}})
}

func forgedIngestBlock(t *testing.T, height int64) *types.Block {
	t.Helper()
	block := makeIngestBlock(t, height)
	block.ProposerAddress = bytesOf(0x99, 20) // different hash, still self-consistent
	return block
}

// A forged pending block must not turn honest successors into disconnects: the
// conflict is ambiguous, so the pending block is dropped and refetched elsewhere.
func TestReactorPendingConflictRefetchesWithoutDisconnecting(t *testing.T) {
	blockStore := newTestBlockStore(t)
	ingestor, err := NewHotIngestor(blockStore, IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	// Sorted peer IDs make the attacker the planner's first choice for height 1.
	attacker := newFakePeer("a-attacker")
	honest := newFakePeer("b-honest")
	reactor.AddPeer(attacker)
	receiveStatus(reactor, attacker, 1, 1)
	receiveBlock(t, reactor, attacker, forgedIngestBlock(t, 1))
	if ingestor.PendingHeight() != 1 {
		t.Fatalf("pending height = %d, want 1", ingestor.PendingHeight())
	}

	reactor.AddPeer(honest)
	receiveStatus(reactor, honest, 1, 2)
	receiveBlock(t, reactor, honest, makeIngestBlock(t, 2))
	if peerStopped(honest) || peerStopped(attacker) {
		t.Fatalf("ambiguous conflict disconnected a peer: honest=%v attacker=%v", peerStopped(honest), peerStopped(attacker))
	}
	if ingestor.PendingHeight() != 0 || ingestor.NextHeight() != 1 {
		t.Fatalf("conflicting pending block kept: pending=%d next=%d", ingestor.PendingHeight(), ingestor.NextHeight())
	}
	peer, ok := reactor.planner.InflightPeer(1)
	if !ok || peer != "b-honest" {
		t.Fatalf("height 1 refetch peer = %q (%v), want b-honest over suspected attacker", peer, ok)
	}
	receiveBlock(t, reactor, honest, makeIngestBlock(t, 1))
	if peer, ok := reactor.planner.InflightPeer(2); ok && peer != "b-honest" {
		t.Fatalf("height 2 refetch peer = %q", peer)
	}
	receiveBlock(t, reactor, honest, makeIngestBlock(t, 2))
	if blockStore.Height() != 1 || ingestor.PendingHeight() != 2 {
		t.Fatalf("did not recover: store=%d pending=%d", blockStore.Height(), ingestor.PendingHeight())
	}
}

// A pending block pinned by a checkpoint is authentic, so a successor that
// does not commit to it is provably invalid and its sender is disconnected.
func TestReactorDisconnectsSuccessorConflictingWithCheckpoint(t *testing.T) {
	first := makeIngestBlock(t, 1)
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{
		ChainID:     ingestTestChainID,
		StartHeight: 1,
		Validation:  ValidationCheckpoint,
		Checkpoints: map[int64]string{1: fmt.Sprintf("%x", first.Hash())},
	})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	good := newFakePeer("a-good")
	liar := newFakePeer("b-liar")
	reactor.AddPeer(good)
	receiveStatus(reactor, good, 1, 1)
	receiveBlock(t, reactor, good, first)
	reactor.AddPeer(liar)
	receiveStatus(reactor, liar, 2, 2)
	// Commits to a forged height-1 block instead of the pinned one.
	forged := forgedIngestBlock(t, 1)
	parts, err := forged.MakePartSet(types.BlockPartSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	successor := makeIngestBlock(t, 2)
	successor.LastBlockID = types.BlockID{Hash: forged.Hash(), PartSetHeader: parts.Header()}
	successor.LastCommit.BlockID = successor.LastBlockID
	successor.LastCommitHash = successor.LastCommit.Hash()
	receiveBlock(t, reactor, liar, successor)
	if !peerStopped(liar) || peerStopped(good) {
		t.Fatalf("liar stopped=%v good stopped=%v", peerStopped(liar), peerStopped(good))
	}
	if ingestor.PendingHeight() != 1 {
		t.Fatalf("pinned pending block dropped: pending=%d", ingestor.PendingHeight())
	}
}

type failingValidatorSetSource struct{}

func (failingValidatorSetSource) ValidatorSet(int64) (*types.ValidatorSet, error) {
	return nil, errors.New("rpc: connection refused")
}

// A local validator-set lookup failure is not the sender's fault.
func TestReactorKeepsPeerOnLocalValidatorSourceError(t *testing.T) {
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{
		ChainID:            ingestTestChainID,
		StartHeight:        1,
		Validation:         ValidationValidatorSet,
		ValidatorSetSource: failingValidatorSetSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{RequestLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	peer := newFakePeer("honest")
	reactor.AddPeer(peer)
	receiveStatus(reactor, peer, 1, 1)
	peer.resetSent()
	receiveBlock(t, reactor, peer, makeIngestBlock(t, 1))
	if peerStopped(peer) {
		t.Fatal("peer disconnected for a local validator-set failure")
	}
	if retry := collectRequests(peer.sentEnvelopes()); len(retry) != 1 || retry[0].Height != 1 {
		t.Fatalf("expected retry of height 1, got %+v", retry)
	}
}

// Ingest errors attributable to the block itself must be classified as such.
func TestSubmitClassifiesBlockFaults(t *testing.T) {
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	wrongChain := makeIngestBlock(t, 1)
	wrongChain.ChainID = "other"
	if _, err := ingestor.Submit(wrongChain); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("wrong chain: %v", err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor.Submit(makeIngestBlock(t, 3)); err == nil || errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("out-of-order height must be unattributed: %v", err)
	}
	var conflict *PendingConflictError
	if _, err := ingestor.Submit(forgedSuccessor(t)); !errors.As(err, &conflict) || conflict.PendingHeight != 1 {
		t.Fatalf("link mismatch: %v", err)
	}
}

func forgedSuccessor(t *testing.T) *types.Block {
	t.Helper()
	forged := forgedIngestBlock(t, 1)
	parts, err := forged.MakePartSet(types.BlockPartSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	block := makeIngestBlock(t, 2)
	block.LastBlockID = types.BlockID{Hash: forged.Hash(), PartSetHeader: parts.Header()}
	block.LastCommit.BlockID = block.LastBlockID
	block.LastCommitHash = block.LastCommit.Hash()
	return block
}

// Budgets recreated by requests racing RemovePeer must not lock out new peers
// once they have refilled.
func TestReactorColdBudgetsReclaimRefilledEntries(t *testing.T) {
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID, StartHeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err := NewReactor(ingestor, nil, ReactorOptions{
		ColdBlockSource: &scriptedColdBlockSource{advertised: PeerRange{Base: 1, Height: 2}, block: makeIngestBlock(t, 1)},
		RequestLimit:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &bcproto.BlockRequest{Height: 1}
	for i := range maxColdPeerBudgets {
		gone := newFakePeer(p2p.ID(fmt.Sprintf("gone-%d", i)))
		reactor.AddPeer(gone)
		reactor.RemovePeer(gone, nil)
		reactor.Receive(p2p.Envelope{Src: gone, ChannelID: cmtblocksync.BlocksyncChannel, Message: request})
	}
	if len(reactor.coldBudgets) != maxColdPeerBudgets {
		t.Fatalf("budgets = %d, want full table", len(reactor.coldBudgets))
	}
	fresh := newFakePeer("fresh")
	before := reactor.ColdRateLimited()
	reactor.Receive(p2p.Envelope{Src: fresh, ChannelID: cmtblocksync.BlocksyncChannel, Message: request})
	if reactor.ColdRateLimited() != before+1 {
		t.Fatal("full table of recently active budgets must still bound new peers")
	}

	reactor.mu.Lock()
	for id, budget := range reactor.coldBudgets {
		budget.updated = budget.updated.Add(-time.Minute)
		reactor.coldBudgets[id] = budget
	}
	reactor.mu.Unlock()
	before = reactor.ColdRateLimited()
	reactor.Receive(p2p.Envelope{Src: fresh, ChannelID: cmtblocksync.BlocksyncChannel, Message: request})
	if reactor.ColdRateLimited() != before {
		t.Fatal("fresh peer rate limited after stale budgets refilled")
	}
	if len(reactor.coldBudgets) != 1 {
		t.Fatalf("budgets after sweep = %d, want 1", len(reactor.coldBudgets))
	}
}
