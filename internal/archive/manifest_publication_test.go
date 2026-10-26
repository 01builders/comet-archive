package archive

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestManifestPublicationRejectsStaleWriters(t *testing.T) {
	root := t.TempDir()
	first, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest(testChainID, nil)
	if err != nil {
		t.Fatal(err)
	}
	const key = "manifest.json"
	if checkErr := SaveManifest(t.Context(), first, key, &manifest); checkErr != nil {
		t.Fatal(checkErr)
	}
	left, err := LoadManifest(t.Context(), first, key)
	if err != nil {
		t.Fatal(err)
	}
	right, err := LoadManifest(t.Context(), second, key)
	if err != nil {
		t.Fatal(err)
	}
	left.UpdatedAt = left.UpdatedAt.Add(1)
	right.UpdatedAt = right.UpdatedAt.Add(2)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i, candidate := range []*Manifest{&left, &right} {
		writer := []ObjectStore{first, second}[i]
		wg.Go(func() { <-start; results <- SaveManifest(t.Context(), writer, key, candidate) })
	}
	close(start)
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrObjectConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	fresh, err := NewManifest(testChainID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if checkErr := SaveManifest(t.Context(), first, key, &fresh); !errors.Is(checkErr, ErrObjectConflict) {
		t.Fatalf("create existing: %v", checkErr)
	}
	if checkErr := SaveManifest(t.Context(), first, "other.json", &left); checkErr == nil {
		t.Fatal("allowed revision reuse at another key")
	}
}

func TestLocalConditionalWriteHonorsCancellation(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, checkErr := store.CompareAndSwap(ctx, "key", nil, ""); !errors.Is(checkErr, context.Canceled) {
		t.Fatalf("checkErr=%v", checkErr)
	}
	if exists, checkErr := store.Exists(t.Context(), "key"); checkErr != nil || exists {
		t.Fatalf("canceled write exists=%v checkErr=%v", exists, checkErr)
	}
}

func TestMigrationStateRejectsStaleProgress(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := MigrationState{ChainID: testChainID, StartHeight: 1, EndHeight: 2, NextHeight: 1}
	if checkErr := saveMigrationState(t.Context(), store, "state.json", &state); checkErr != nil {
		t.Fatal(checkErr)
	}
	left, err := loadMigrationState(t.Context(), store, "state.json")
	if err != nil {
		t.Fatal(err)
	}
	right, err := loadMigrationState(t.Context(), store, "state.json")
	if err != nil {
		t.Fatal(err)
	}
	left.LastUpdatedAt = left.LastUpdatedAt.Add(1)
	right.LastUpdatedAt = right.LastUpdatedAt.Add(2)
	if checkErr := saveMigrationState(t.Context(), store, "state.json", &left); checkErr != nil {
		t.Fatal(checkErr)
	}
	if checkErr := saveMigrationState(t.Context(), store, "state.json", &right); !errors.Is(checkErr, ErrObjectConflict) {
		t.Fatalf("stale state write: %v", checkErr)
	}
}

func TestPublicationPreservesV1StorageLayout(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, segment, err := EncodeSegment([]BlockRecord{makeTestRecord(t, 1)}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = "segments/1.cba"
	manifest, err := NewManifest(testChainID, []SegmentManifest{segment})
	if err != nil {
		t.Fatal(err)
	}
	if checkErr := SaveManifest(t.Context(), store, "manifest.json", &manifest); checkErr != nil {
		t.Fatal(checkErr)
	}
	loaded, err := LoadManifest(t.Context(), store, "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if checkErr := SaveManifest(t.Context(), store, "manifest.json", &loaded); checkErr != nil {
		t.Fatal(checkErr)
	}
	state := MigrationState{ChainID: testChainID, StartHeight: 1, EndHeight: 2, NextHeight: 2, Segments: []SegmentManifest{segment}}
	if checkErr := saveMigrationState(t.Context(), store, "state.json", &state); checkErr != nil {
		t.Fatal(checkErr)
	}
	for _, key := range []string{"manifest.json", "state.json"} {
		data, readErr := store.Get(t.Context(), key)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var document struct {
			Version  int               `json:"version"`
			Segments []SegmentManifest `json:"segments"`
		}
		if decodeErr := json.Unmarshal(data, &document); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if key == "manifest.json" && document.Version != 1 {
			t.Fatalf("manifest version=%d", document.Version)
		}
		if key == "state.json" && document.Version != 0 {
			t.Fatal("resume state gained a format version")
		}
		if len(document.Segments) != 1 || len(document.Segments[0].Blocks) != 1 || document.Segments[0].Blocks[0].Height != 1 {
			t.Fatalf("%s lost inline block index", key)
		}
	}
	objects, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("publication created auxiliary metadata objects: %+v", objects)
	}
}
