package blocksyncarchive

import (
	"encoding/hex"
	"testing"

	"github.com/01builders/cometbft-archive/internal/archive"
)

func TestCheckpointArchiveStopsAtAuthenticatedHeightAcrossRestart(t *testing.T) {
	hot := newTestBlockStore(t)
	opts := IngestOptions{ChainID: ingestTestChainID, StartHeight: 1, Validation: ValidationCheckpoint, Checkpoints: map[int64]string{3: hex.EncodeToString(makeIngestBlock(t, 3).Hash())}}
	ingestor, err := NewHotIngestor(hot, opts)
	if err != nil {
		t.Fatal(err)
	}
	for h := int64(1); h <= 2; h++ {
		if _, checkErr := ingestor.Submit(makeIngestBlock(t, h)); checkErr != nil {
			t.Fatal(checkErr)
		}
	}
	objects, err := archive.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	archiveOpts := archive.LiveArchiveOptions{ChainID: ingestTestChainID, SegmentBlocks: 1}
	result, err := archive.ArchiveReadyFromHead(t.Context(), ingestor.BlockReader(), objects, archiveOpts, 0)
	if err != nil || result.BlocksArchived != 0 {
		t.Fatalf("archived before checkpoint: %+v %v", result, err)
	}
	for h := int64(3); h <= 5; h++ {
		ingested, submitErr := ingestor.Submit(makeIngestBlock(t, h))
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		if ingested.ReadyToArchiveThrough > 3 {
			t.Fatal("unverified height reported ready")
		}
	}
	result, err = archive.ArchiveReadyFromHead(t.Context(), ingestor.BlockReader(), objects, archiveOpts, 0)
	if err != nil || result.Manifest.LastHeight != 3 {
		t.Fatalf("archive passed checkpoint: %+v %v", result, err)
	}
	restarted, err := NewHotIngestor(hot, opts)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.BlockReader().ArchiveHeightLimit() != 3 {
		t.Fatal("restart promoted unauthenticated tail")
	}
	if _, checkErr := restarted.Submit(makeIngestBlock(t, 5)); checkErr != nil {
		t.Fatal(checkErr)
	}
	// A mismatching stored checkpoint must fail startup.
	opts.Checkpoints[3] = hex.EncodeToString(makeIngestBlock(t, 2).Hash())
	if _, checkErr := NewHotIngestor(hot, opts); checkErr == nil {
		t.Fatal("accepted mismatching persisted checkpoint")
	}
}

func TestCheckpointRejectsCommitDetachedFromHeader(t *testing.T) {
	first := makeIngestBlock(t, 1)
	second := makeIngestBlock(t, 2)
	ingestor, err := NewHotIngestor(newTestBlockStore(t), IngestOptions{ChainID: ingestTestChainID, Validation: ValidationCheckpoint, Checkpoints: map[int64]string{2: hex.EncodeToString(second.Hash())}})
	if err != nil {
		t.Fatal(err)
	}
	if _, checkErr := ingestor.Submit(first); checkErr != nil {
		t.Fatal(checkErr)
	}
	second.LastCommit.BlockID.Hash = make([]byte, 32)
	if _, checkErr := ingestor.Submit(second); checkErr == nil {
		t.Fatal("accepted commit not bound to trusted header")
	}
	if ingestor.BlockReader().ArchiveHeightLimit() != 0 {
		t.Fatal("rejected checkpoint advanced authentication")
	}
}
