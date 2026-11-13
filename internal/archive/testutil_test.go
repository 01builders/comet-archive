package archive

import (
	"fmt"
	"sync"
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/store"
	ctypes "github.com/cometbft/cometbft/types"
)

const testChainID = "archive-test-chain"

// testBlockIDs memoizes the deterministic hash-linked test chain by height.
var (
	testBlockIDsMu sync.Mutex
	testBlockIDs   = map[int64]ctypes.BlockID{}
)

// makeTestBlock returns block height of a deterministic chain in which every
// block's LastBlockID references its predecessor.
func makeTestBlock(t *testing.T, height int64) *ctypes.Block {
	t.Helper()
	return makeUnlinkedTestBlock(t, height, testBlockID(t, height-1))
}

func testBlockID(t *testing.T, height int64) ctypes.BlockID {
	t.Helper()
	if height <= 0 {
		return ctypes.BlockID{}
	}
	testBlockIDsMu.Lock()
	defer testBlockIDsMu.Unlock()
	from := height
	for from > 0 {
		if _, ok := testBlockIDs[from]; ok {
			break
		}
		from--
	}
	for h := from + 1; h <= height; h++ {
		block := makeUnlinkedTestBlock(t, h, testBlockIDs[h-1])
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		testBlockIDs[h] = ctypes.BlockID{Hash: block.Hash(), PartSetHeader: parts.Header()}
	}
	return testBlockIDs[height]
}

func makeUnlinkedTestBlock(t *testing.T, height int64, parent ctypes.BlockID) *ctypes.Block {
	t.Helper()
	block := ctypes.MakeBlock(height, []ctypes.Tx{ctypes.Tx(fmt.Sprintf("tx-%d", height))}, &ctypes.Commit{}, nil)
	block.ChainID = testChainID
	block.LastBlockID = parent
	block.ProposerAddress = bytesOf(byte(height), crypto.AddressSize)
	block.ValidatorsHash = bytesOf(0x11, 32)
	block.NextValidatorsHash = bytesOf(0x22, 32)
	block.ConsensusHash = bytesOf(0x33, 32)
	block.Time = block.Time.UTC()
	return block
}

func makeTestRecord(t *testing.T, height int64) BlockRecord {
	t.Helper()
	record, err := BlockToRecord(makeTestBlock(t, height))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func bytesOf(value byte, size int) []byte {
	bz := make([]byte, size)
	for i := range bz {
		bz[i] = value
	}
	return bz
}

func createBlockStoreFixture(t *testing.T, dir string, heights int) {
	t.Helper()
	db, err := dbm.NewDB("blockstore", dbm.BackendType("goleveldb"), dir)
	if err != nil {
		t.Fatal(err)
	}
	bs := store.NewBlockStore(db)
	defer bs.Close()
	for h := int64(1); h <= int64(heights); h++ {
		block := makeTestBlock(t, h)
		parts, err := block.MakePartSet(ctypes.BlockPartSizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		seen := &ctypes.Commit{Height: h, Signatures: []ctypes.CommitSig{}}
		bs.SaveBlock(block, parts, seen)
	}
}
