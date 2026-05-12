package archive

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestSegmentRoundTrip(t *testing.T) {
	records := []BlockRecord{
		makeTestRecord(t, 1),
		makeTestRecord(t, 2),
		makeTestRecord(t, 3),
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	got, err := DecodeSegment(data, segment)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(records) {
		t.Fatalf("got %d records, want %d", len(got), len(records))
	}
	for i := range got {
		if got[i].Height != records[i].Height || got[i].Hash != records[i].Hash {
			t.Fatalf("record %d mismatch: got %+v want %+v", i, got[i], records[i])
		}
	}
}

func TestSegmentDetectsCorruption(t *testing.T) {
	records := []BlockRecord{makeTestRecord(t, 1)}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(data)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := DecodeSegment(corrupt, segment); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestDecodeSegmentBlockDecodesOnlyRequestedBlock(t *testing.T) {
	records := []BlockRecord{
		makeTestRecord(t, 1),
		{Height: 2, Hash: strings.Repeat("a", blockHashHexLen), Bytes: []byte("not a comet block")},
		makeTestRecord(t, 3),
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	if _, decodeErr := DecodeSegment(data, segment); decodeErr == nil {
		t.Fatal("expected full segment decode to reject invalid block")
	}
	block, err := DecodeSegmentBlock(data, segment, 3)
	if err != nil {
		t.Fatal(err)
	}
	if block.Height != 3 {
		t.Fatalf("decoded height %d, want 3", block.Height)
	}
	if _, err := DecodeSegmentBlock(data, segment, 2); err == nil {
		t.Fatal("expected requested invalid block to fail")
	}
}

func TestDecodeSegmentBlockValidatesRequestedBlockHash(t *testing.T) {
	badHashRecord := makeTestRecord(t, 1)
	badHashRecord.Hash = strings.Repeat("b", blockHashHexLen)
	data, segment, err := EncodeSegment([]BlockRecord{badHashRecord}, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	_, err = DecodeSegmentBlock(data, segment, 1)
	if err == nil || !strings.Contains(err.Error(), "record hash mismatch") {
		t.Fatalf("DecodeSegmentBlock err=%v, want record hash mismatch", err)
	}
}

func TestSegmentRequiresContiguousRecords(t *testing.T) {
	_, _, err := EncodeSegment([]BlockRecord{
		makeTestRecord(t, 1),
		makeTestRecord(t, 3),
	}, CompressionGzip)
	if err == nil {
		t.Fatal("expected contiguity error")
	}
}

func TestEncodeSegmentRejectsMalformedRecordHash(t *testing.T) {
	for _, record := range []BlockRecord{
		{Height: 1, Hash: "abcd", Bytes: []byte("payload")},
		{Height: 1, Hash: strings.Repeat("z", blockHashHexLen), Bytes: []byte("payload")},
	} {
		_, _, err := EncodeSegment([]BlockRecord{record}, CompressionGzip)
		if err == nil {
			t.Fatalf("expected malformed hash error for %q", record.Hash)
		}
	}
}

func TestEncodeSegmentRejectsTooManyRecords(t *testing.T) {
	records := make([]BlockRecord, MaxSegmentBlocks+1)
	for i := range records {
		height := int64(i + 1)
		records[i] = BlockRecord{
			Height: height,
			Hash:   strings.Repeat("a", blockHashHexLen),
			Bytes:  []byte("payload"),
		}
	}
	_, _, err := EncodeSegment(records, CompressionGzip)
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("EncodeSegment err=%v, want max record count error", err)
	}
}

func TestMaxDecompressedForBlockCountSaturates(t *testing.T) {
	if got := maxDecompressedForBlockCount(0, 10); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
	if got := maxDecompressedForBlockCount(math.MaxInt64, 2); got != math.MaxInt64 {
		t.Fatalf("got %d, want MaxInt64", got)
	}
	if got := maxDecompressedForBlockCount(10, 0); got != math.MaxInt64 {
		t.Fatalf("got %d, want MaxInt64", got)
	}
}

func TestSegmentPayloadLimit(t *testing.T) {
	if segmentPayloadWouldExceedLimit(90, blockHashHexLen, 1, 200) {
		t.Fatal("small record unexpectedly exceeded limit")
	}
	if !segmentPayloadWouldExceedLimit(90, blockHashHexLen, 40, 200) {
		t.Fatal("expected record to exceed limit")
	}
	if got := maxDecompressedFor(SegmentManifest{Blocks: make([]BlockIndex, MaxSegmentBlocks)}); got != MaxSegmentPayloadBytes {
		t.Fatalf("max decompressed = %d, want %d", got, MaxSegmentPayloadBytes)
	}
}

func TestSegmentBlockIndexOffsetsMonotonic(t *testing.T) {
	for _, compression := range []string{CompressionGzip, CompressionNone} {
		t.Run(compression, func(t *testing.T) {
			records := make([]BlockRecord, 5)
			for i := range records {
				records[i] = makeTestRecord(t, int64(i+1))
			}
			_, segment, err := EncodeSegment(records, compression)
			if err != nil {
				t.Fatal(err)
			}
			if segment.Blocks[0].Offset != 0 {
				t.Fatalf("first offset = %d, want 0", segment.Blocks[0].Offset)
			}
			for i := 1; i < len(segment.Blocks); i++ {
				if segment.Blocks[i].Offset <= segment.Blocks[i-1].Offset {
					t.Fatalf("offsets not strictly increasing at %d: %d <= %d", i, segment.Blocks[i].Offset, segment.Blocks[i-1].Offset)
				}
			}
		})
	}
}

func TestDecodeSegmentBlockFastPath(t *testing.T) {
	for _, compression := range []string{CompressionGzip, CompressionNone} {
		t.Run(compression, func(t *testing.T) {
			records := make([]BlockRecord, 12)
			for i := range records {
				records[i] = makeTestRecord(t, int64(i+1))
			}
			data, segment, err := EncodeSegment(records, compression)
			if err != nil {
				t.Fatal(err)
			}
			segment.Key = SegmentKey("archive", testChainID, segment)
			const target = int64(7)
			block, err := DecodeSegmentBlock(data, segment, target)
			if err != nil {
				t.Fatal(err)
			}
			if block.Height != target {
				t.Fatalf("got height %d, want %d", block.Height, target)
			}
			for i := 1; i < len(segment.Blocks); i++ {
				if segment.Blocks[i].Offset == 0 {
					t.Fatalf("non-first offset at index %d is zero", i)
				}
			}
		})
	}
}

func TestDecodeSegmentBlockLegacyFallback(t *testing.T) {
	records := make([]BlockRecord, 12)
	for i := range records {
		records[i] = makeTestRecord(t, int64(i+1))
	}
	data, segment, err := EncodeSegment(records, CompressionGzip)
	if err != nil {
		t.Fatal(err)
	}
	segment.Key = SegmentKey("archive", testChainID, segment)
	for i := range segment.Blocks {
		segment.Blocks[i].Offset = 0
	}
	const target = int64(6)
	block, err := DecodeSegmentBlock(data, segment, target)
	if err != nil {
		t.Fatal(err)
	}
	if block.Height != target {
		t.Fatalf("got height %d, want %d", block.Height, target)
	}
}

func TestValidateCompression(t *testing.T) {
	for _, tc := range []struct {
		name        string
		compression string
		wantErr     bool
	}{
		{name: "default"},
		{name: "none", compression: CompressionNone},
		{name: "gzip", compression: CompressionGzip},
		{name: "unsupported", compression: "zstd", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCompression(tc.compression)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateArchiveKeys(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prefix       string
		chainID      string
		manifestName string
		wantErr      bool
	}{
		{name: "default", chainID: testChainID},
		{name: "custom", prefix: "custom/archive", chainID: testChainID, manifestName: "live.json"},
		{name: "bad-prefix", prefix: "../escape", chainID: testChainID, wantErr: true},
		{name: "bad-chain", prefix: "archive", chainID: "../../escape", wantErr: true},
		{name: "slash-chain", prefix: "archive", chainID: "bad/chain", wantErr: true},
		{name: "bad-manifest-name", prefix: "archive", chainID: testChainID, manifestName: "../../../../escape", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateArchiveKeys(tc.prefix, tc.chainID, tc.manifestName)
			if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
