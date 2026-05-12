package archive

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/cometbft/cometbft/proto/tendermint/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
)

const (
	SegmentMagic           = "CBTASEG1"
	CompressionNone        = "none"
	CompressionGzip        = "gzip"
	DefaultCompression     = CompressionGzip
	DefaultSegmentBlocks   = 100
	MaxSegmentBlocks       = 1000
	MaxSegmentPayloadBytes = 1 << 30
	defaultPrefix          = "archive"
	maxRecordPayloadLen    = 256 << 20
)

var ErrSegmentBlockNotFound = errors.New("segment block not found")

type BlockRecord struct {
	Height int64
	Hash   string
	Bytes  []byte
}

func BlockToRecord(block *ctypes.Block) (BlockRecord, error) {
	if block == nil {
		return BlockRecord{}, errors.New("nil block")
	}
	pb, err := block.ToProto()
	if err != nil {
		return BlockRecord{}, err
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return BlockRecord{}, err
	}
	return BlockRecord{
		Height: block.Height,
		Hash:   hex.EncodeToString(block.Hash()),
		Bytes:  data,
	}, nil
}

func RecordToBlock(record BlockRecord) (*ctypes.Block, error) {
	var pb types.Block
	if err := proto.Unmarshal(record.Bytes, &pb); err != nil {
		return nil, err
	}
	block, err := ctypes.BlockFromProto(&pb)
	if err != nil {
		return nil, err
	}
	if got := hex.EncodeToString(block.Hash()); got != record.Hash {
		return nil, fmt.Errorf("record hash mismatch at height %d: got %s want %s", record.Height, got, record.Hash)
	}
	if block.Height != record.Height {
		return nil, fmt.Errorf("record height mismatch: block has %d, record has %d", block.Height, record.Height)
	}
	return block, nil
}

func EncodeSegment(records []BlockRecord, compression string) ([]byte, SegmentManifest, error) {
	if compression == "" {
		compression = DefaultCompression
	}
	if err := ValidateCompression(compression); err != nil {
		return nil, SegmentManifest{}, err
	}
	if len(records) == 0 {
		return nil, SegmentManifest{}, errors.New("segment requires at least one record")
	}
	if len(records) > MaxSegmentBlocks {
		return nil, SegmentManifest{}, fmt.Errorf("segment has %d records, maximum is %d", len(records), MaxSegmentBlocks)
	}
	var payload bytes.Buffer
	var previousHeight int64
	for i, record := range records {
		if i > 0 && record.Height != previousHeight+1 {
			return nil, SegmentManifest{}, fmt.Errorf("records are not contiguous at height %d", record.Height)
		}
		previousHeight = record.Height
		if record.Height <= 0 {
			return nil, SegmentManifest{}, fmt.Errorf("invalid record height %d", record.Height)
		}
		if record.Hash == "" {
			return nil, SegmentManifest{}, fmt.Errorf("record at height %d has empty hash", record.Height)
		}
		if len(record.Hash) != blockHashHexLen {
			return nil, SegmentManifest{}, fmt.Errorf("record hash for height %d must be %d hex characters", record.Height, blockHashHexLen)
		}
		if _, err := hex.DecodeString(record.Hash); err != nil {
			return nil, SegmentManifest{}, fmt.Errorf("record hash for height %d is not valid hex: %w", record.Height, err)
		}
		if len(record.Bytes) == 0 {
			return nil, SegmentManifest{}, fmt.Errorf("record at height %d has empty payload", record.Height)
		}
		if err := binary.Write(&payload, binary.BigEndian, record.Height); err != nil {
			return nil, SegmentManifest{}, err
		}
		hashBytes := []byte(record.Hash)
		if len(hashBytes) > 255 {
			return nil, SegmentManifest{}, fmt.Errorf("record hash for height %d is too long", record.Height)
		}
		if err := payload.WriteByte(byte(len(hashBytes))); err != nil {
			return nil, SegmentManifest{}, err
		}
		if _, err := payload.Write(hashBytes); err != nil {
			return nil, SegmentManifest{}, err
		}
		if len(record.Bytes) > maxRecordPayloadLen {
			return nil, SegmentManifest{}, fmt.Errorf("record at height %d exceeds max payload", record.Height)
		}
		if segmentPayloadWouldExceedLimit(int64(payload.Len()), len(hashBytes), len(record.Bytes), MaxSegmentPayloadBytes) {
			return nil, SegmentManifest{}, fmt.Errorf("segment payload exceeds max size %d", MaxSegmentPayloadBytes)
		}
		if err := binary.Write(&payload, binary.BigEndian, uint32(len(record.Bytes))); err != nil {
			return nil, SegmentManifest{}, err
		}
		if _, err := payload.Write(record.Bytes); err != nil {
			return nil, SegmentManifest{}, err
		}
	}
	compressed, err := compressPayload(payload.Bytes(), compression)
	if err != nil {
		return nil, SegmentManifest{}, err
	}
	data := append([]byte(SegmentMagic), compressed...)
	sum := sha256.Sum256(data)
	manifest := SegmentManifest{
		FirstHeight: records[0].Height,
		LastHeight:  records[len(records)-1].Height,
		Compression: compression,
		SizeBytes:   int64(len(data)),
		SHA256:      hex.EncodeToString(sum[:]),
		Blocks:      make([]BlockIndex, len(records)),
	}
	for i, record := range records {
		manifest.Blocks[i] = BlockIndex{Height: record.Height, Hash: record.Hash}
	}
	return data, manifest, nil
}

func DecodeSegment(data []byte, manifest SegmentManifest) ([]BlockRecord, error) {
	return decodeSegmentRecords(data, manifest, validateDecodedBlockRecord)
}

func DecodeSegmentBlock(data []byte, manifest SegmentManifest, height int64) (*ctypes.Block, error) {
	if height < manifest.FirstHeight || height > manifest.LastHeight {
		return nil, fmt.Errorf("%w: height %d outside segment range %d-%d", ErrSegmentBlockNotFound, height, manifest.FirstHeight, manifest.LastHeight)
	}
	records, err := decodeSegmentRecords(data, manifest, nil)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.Height != height {
			continue
		}
		block, err := RecordToBlock(record)
		if err != nil {
			return nil, err
		}
		return block, nil
	}
	return nil, fmt.Errorf("%w: height %d", ErrSegmentBlockNotFound, height)
}

func decodeSegmentRecords(data []byte, manifest SegmentManifest, validateRecord func(BlockRecord) error) ([]BlockRecord, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if int64(len(data)) != manifest.SizeBytes {
		return nil, fmt.Errorf("segment size mismatch: got %d want %d", len(data), manifest.SizeBytes)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != manifest.SHA256 {
		return nil, fmt.Errorf("segment checksum mismatch: got %s want %s", got, manifest.SHA256)
	}
	if len(data) < len(SegmentMagic) || string(data[:len(SegmentMagic)]) != SegmentMagic {
		return nil, errors.New("invalid segment magic")
	}
	payload, err := decompressPayload(data[len(SegmentMagic):], manifest.Compression, maxDecompressedFor(manifest))
	if err != nil {
		return nil, err
	}
	records, err := decodeRecords(payload)
	if err != nil {
		return nil, err
	}
	if len(records) != len(manifest.Blocks) {
		return nil, fmt.Errorf("segment record count %d, expected %d", len(records), len(manifest.Blocks))
	}
	for i, record := range records {
		index := manifest.Blocks[i]
		if record.Height != index.Height {
			return nil, fmt.Errorf("record %d height %d, expected %d", i, record.Height, index.Height)
		}
		if record.Hash != index.Hash {
			return nil, fmt.Errorf("record %d hash %s, expected %s", i, record.Hash, index.Hash)
		}
		if validateRecord != nil {
			if err := validateRecord(record); err != nil {
				return nil, err
			}
		}
	}
	return records, nil
}

func validateDecodedBlockRecord(record BlockRecord) error {
	_, err := RecordToBlock(record)
	return err
}

func SegmentKey(prefix, chainID string, segment SegmentManifest) string {
	shortHash := segment.SHA256
	if len(shortHash) > 16 {
		shortHash = shortHash[:16]
	}
	return fmt.Sprintf("%s/chains/%s/segments/%012d-%012d-%s.cba", cleanPrefix(prefix), chainID, segment.FirstHeight, segment.LastHeight, shortHash)
}

func ManifestKey(prefix, chainID, manifestName string) string {
	if manifestName == "" {
		manifestName = DefaultManifest
	}
	return fmt.Sprintf("%s/chains/%s/manifests/%s", cleanPrefix(prefix), chainID, manifestName)
}

func StateKey(prefix, chainID, manifestName string) string {
	if manifestName == "" {
		manifestName = DefaultManifest
	}
	return fmt.Sprintf("%s/chains/%s/migration/%s.state.json", cleanPrefix(prefix), chainID, manifestName)
}

func ResolveManifestKey(prefix, chainID, manifestName, manifestKey string) (string, error) {
	if err := ValidateArchiveNamespace(prefix, chainID); err != nil {
		return "", err
	}
	if manifestKey != "" {
		if err := ValidateObjectKey(manifestKey); err != nil {
			return "", fmt.Errorf("manifest key: %w", err)
		}
		return manifestKey, nil
	}
	key := ManifestKey(prefix, chainID, manifestName)
	if err := ValidateObjectKey(key); err != nil {
		return "", fmt.Errorf("manifest key: %w", err)
	}
	return key, nil
}

func MigrationStateKey(prefix, chainID, manifestName, manifestKey string) (string, error) {
	if err := ValidateArchiveNamespace(prefix, chainID); err != nil {
		return "", err
	}
	if manifestKey != "" {
		key := manifestKey + ".state.json"
		if err := ValidateObjectKey(key); err != nil {
			return "", fmt.Errorf("state key: %w", err)
		}
		return key, nil
	}
	key := StateKey(prefix, chainID, manifestName)
	if err := ValidateObjectKey(key); err != nil {
		return "", fmt.Errorf("state key: %w", err)
	}
	return key, nil
}

func ValidateArchiveNamespace(prefix, chainID string) error {
	if err := ValidateArchiveChainID(chainID); err != nil {
		return err
	}
	if err := ValidateObjectKey(StateKey(prefix, chainID, DefaultManifest)); err != nil {
		return fmt.Errorf("state key: %w", err)
	}
	return nil
}

func ValidateArchiveKeys(prefix, chainID, manifestName string) error {
	if err := ValidateArchiveNamespace(prefix, chainID); err != nil {
		return err
	}
	if err := ValidateObjectKey(ManifestKey(prefix, chainID, manifestName)); err != nil {
		return fmt.Errorf("manifest key: %w", err)
	}
	return nil
}

func cleanPrefix(prefix string) string {
	if prefix == "" {
		return defaultPrefix
	}
	for len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	for len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}
	if prefix == "" {
		return defaultPrefix
	}
	return prefix
}

func ValidateCompression(compression string) error {
	if compression == "" {
		return nil
	}
	switch compression {
	case CompressionNone, CompressionGzip:
		return nil
	default:
		return fmt.Errorf("unsupported compression %q", compression)
	}
}

func compressPayload(data []byte, compression string) ([]byte, error) {
	switch compression {
	case CompressionNone:
		return data, nil
	case CompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			_ = w.Close()
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
}

func decompressPayload(data []byte, compression string, maxSize int64) ([]byte, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("invalid max decompressed size %d", maxSize)
	}
	switch compression {
	case CompressionNone:
		if int64(len(data)) > maxSize {
			return nil, fmt.Errorf("uncompressed payload exceeds max size %d", maxSize)
		}
		return data, nil
	case CompressionGzip:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		limited := io.LimitReader(r, maxSize+1)
		out, err := io.ReadAll(limited)
		if err != nil {
			return nil, err
		}
		if int64(len(out)) > maxSize {
			return nil, fmt.Errorf("decompressed payload exceeds max size %d", maxSize)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
}

// maxDecompressedFor returns the upper bound on a segment's decompressed
// payload size, derived from its manifest. Used to bound decompression
// against malicious or corrupted segment data.
func maxDecompressedFor(m SegmentManifest) int64 {
	const perRecordOverhead = 8 + 1 + 255 + 4
	return min(maxDecompressedForBlockCount(int64(len(m.Blocks)), maxRecordPayloadLen+perRecordOverhead), MaxSegmentPayloadBytes)
}

func maxDecompressedForBlockCount(blocks, perRecordMax int64) int64 {
	if blocks <= 0 {
		blocks = 1
	}
	if perRecordMax <= 0 {
		return math.MaxInt64
	}
	if blocks > math.MaxInt64/perRecordMax {
		return math.MaxInt64
	}
	return blocks * perRecordMax
}

func segmentPayloadWouldExceedLimit(current int64, hashBytes, recordBytes int, maxSize int64) bool {
	const fixedRecordOverhead = 8 + 1 + 4
	if maxSize <= 0 {
		return true
	}
	recordLen := int64(fixedRecordOverhead + hashBytes + recordBytes)
	return recordLen > maxSize || current > maxSize-recordLen
}

func decodeRecords(payload []byte) ([]BlockRecord, error) {
	r := bytes.NewReader(payload)
	var records []BlockRecord
	for r.Len() > 0 {
		var height int64
		if err := binary.Read(r, binary.BigEndian, &height); err != nil {
			return nil, err
		}
		hashLen, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		hash := make([]byte, int(hashLen))
		if _, err := io.ReadFull(r, hash); err != nil {
			return nil, err
		}
		var payloadLen uint32
		if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
			return nil, err
		}
		if payloadLen == 0 || payloadLen > maxRecordPayloadLen {
			return nil, fmt.Errorf("invalid record payload length %d", payloadLen)
		}
		blockBytes := make([]byte, int(payloadLen))
		if _, err := io.ReadFull(r, blockBytes); err != nil {
			return nil, err
		}
		records = append(records, BlockRecord{Height: height, Hash: string(hash), Bytes: blockBytes})
	}
	return records, nil
}
