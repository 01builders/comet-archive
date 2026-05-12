package archive

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/store"
	ctypes "github.com/cometbft/cometbft/types"
)

type BlockReader interface {
	Base() int64
	Height() int64
	LoadBlock(height int64) *ctypes.Block
	Close() error
}

const DefaultDBBackend = "goleveldb"

func OpenCometBlockStore(dbDir, backend string) (*store.BlockStore, error) {
	if err := ValidateCometBlockStoreConfig(dbDir, backend); err != nil {
		return nil, err
	}
	if backend == "" {
		backend = DefaultDBBackend
	}
	db, err := dbm.NewDB("blockstore", dbm.BackendType(backend), filepath.Clean(dbDir))
	if err != nil {
		return nil, err
	}
	return store.NewBlockStore(db), nil
}

func ValidateCometBlockStoreConfig(dbDir, backend string) error {
	if dbDir == "" {
		return errors.New("db-dir is required")
	}
	return ValidateCometDBBackend(backend)
}

func ValidateExistingCometBlockStoreConfig(dbDir, backend string) error {
	if err := ValidateCometBlockStoreConfig(dbDir, backend); err != nil {
		return err
	}
	if backend == "" {
		backend = DefaultDBBackend
	}
	if dbm.BackendType(backend) == dbm.MemDBBackend {
		return nil
	}
	blockstorePath := filepath.Join(filepath.Clean(dbDir), "blockstore.db")
	if _, err := os.Stat(blockstorePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("blockstore database %q does not exist", blockstorePath)
		}
		return err
	}
	return nil
}

func ValidateCometDBBackend(backend string) error {
	if backend == "" {
		return nil
	}
	switch dbm.BackendType(backend) {
	case dbm.GoLevelDBBackend,
		dbm.CLevelDBBackend,
		dbm.MemDBBackend,
		dbm.BoltDBBackend,
		dbm.RocksDBBackend,
		dbm.BadgerDBBackend,
		dbm.PebbleDBBackend:
		return nil
	default:
		return fmt.Errorf("unsupported db backend %q", backend)
	}
}

func ClampRange(reader BlockReader, start, end int64) (rangeStart int64, rangeEnd int64, err error) {
	if reader == nil {
		return 0, 0, errors.New("block reader is required")
	}
	base := reader.Base()
	height := reader.Height()
	if base == 0 || height == 0 {
		return 0, 0, errors.New("source blockstore is empty")
	}
	if start == 0 {
		start = base
	}
	if end == 0 {
		end = height
	}
	if start < base {
		return 0, 0, fmt.Errorf("start height %d is below blockstore base %d", start, base)
	}
	if end > height {
		return 0, 0, fmt.Errorf("end height %d is above blockstore height %d", end, height)
	}
	if end < start {
		return 0, 0, fmt.Errorf("invalid height range %d-%d", start, end)
	}
	return start, end, nil
}
