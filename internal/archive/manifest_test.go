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
