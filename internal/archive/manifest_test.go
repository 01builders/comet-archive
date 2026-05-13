package archive

import (
	"strings"
	"testing"
	"time"
)

func TestManifestValidationCatchesGap(t *testing.T) {
	recordsA := []BlockRecord{makeTestRecord(t, 1)}
	_, segA, err := EncodeSegment(recordsA, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segA.Key = "a"
	recordsB := []BlockRecord{makeTestRecord(t, 3)}
	_, segB, err := EncodeSegment(recordsB, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segB.Key = "b"
	manifest := Manifest{
		Version:     ManifestVersion,
		ChainID:     testChainID,
		FirstHeight: 1,
		LastHeight:  3,
		Segments:    []SegmentManifest{segA, segB},
	}
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected manifest gap validation error")
	}
}

func TestManifestValidationRejectsInconsistentTimestamps(t *testing.T) {
	manifest, err := NewManifest(testChainID, nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest.CreatedAt = time.Now().UTC()
	manifest.UpdatedAt = manifest.CreatedAt.Add(-time.Second)
	err = manifest.Validate()
	if err == nil || !strings.Contains(err.Error(), "updated_at") {
		t.Fatalf("manifest validation err=%v, want updated_at error", err)
	}
}

func TestManifestAppendSegmentInPlace(t *testing.T) {
	recordsA := []BlockRecord{makeTestRecord(t, 1)}
	_, segA, err := EncodeSegment(recordsA, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segA.Key = "chains/" + testChainID + "/segments/a.cba"
	recordsB := []BlockRecord{makeTestRecord(t, 2)}
	_, segB, err := EncodeSegment(recordsB, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segB.Key = "chains/" + testChainID + "/segments/b.cba"

	manifest, err := NewManifest(testChainID, nil)
	if err != nil {
		t.Fatal(err)
	}
	created := manifest.CreatedAt
	now := time.Now().UTC()
	err = manifest.AppendSegmentInPlace(segA, now)
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	if manifest.FirstHeight != 1 || manifest.LastHeight != 1 || len(manifest.Segments) != 1 {
		t.Fatalf("manifest after first append: %+v", manifest)
	}
	if !manifest.CreatedAt.Equal(created) {
		t.Fatalf("created_at changed: %v != %v", manifest.CreatedAt, created)
	}
	err = manifest.AppendSegmentInPlace(segB, now.Add(time.Second))
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if manifest.LastHeight != 2 || len(manifest.Segments) != 2 {
		t.Fatalf("manifest after second append: %+v", manifest)
	}
	err = manifest.Validate()
	if err != nil {
		t.Fatalf("manifest invalid after appends: %v", err)
	}

	// Non-contiguous segment must be rejected.
	recordsD := []BlockRecord{makeTestRecord(t, 5)}
	_, segD, err := EncodeSegment(recordsD, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segD.Key = "chains/" + testChainID + "/segments/d.cba"
	if err := manifest.AppendSegmentInPlace(segD, now); err == nil || !strings.Contains(err.Error(), "expected 3") {
		t.Fatalf("expected contiguity error, got %v", err)
	}
}

func TestManifestValidationRejectsUnsafeChainID(t *testing.T) {
	for _, chainID := range []string{
		"../escape",
		"/absolute",
		`bad\chain`,
		"bad/chain",
		"bad//chain",
		" bad ",
	} {
		t.Run(chainID, func(t *testing.T) {
			manifest := Manifest{
				Version: ManifestVersion,
				ChainID: chainID,
			}
			err := manifest.Validate()
			if err == nil || !strings.Contains(err.Error(), "unsafe chain ID") {
				t.Fatalf("manifest validation err=%v, want unsafe chain ID error", err)
			}
		})
	}
}
