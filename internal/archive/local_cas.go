package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func (s *LocalObjectStore) GetVersioned(ctx context.Context, key string) ([]byte, string, error) {
	data, err := s.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	return data, objectRevision(data), nil
}

func objectRevision(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Lock a stable inode separate from the object: publication replaces the object
// inode. Flock serializes conditional writers across processes and store instances.
func (s *LocalObjectStore) CompareAndSwap(ctx context.Context, key string, data []byte, revision string) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	dir := filepath.Join(s.root, ".cas-locks")
	if err := rejectLocalObjectSymlink(s.root, dir); err != nil {
		return "", err
	}
	if err := makeDirectoriesDurable(dir); err != nil {
		return "", err
	}
	lockPath := filepath.Join(dir, objectRevision([]byte(key)))
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return "", err
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	defer lock.Close()
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		lockErr := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if lockErr == nil {
			break
		}
		if !errors.Is(lockErr, syscall.EWOULDBLOCK) {
			return "", lockErr
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	// Closing lock releases flock, including on errors.
	_, current, err := s.GetVersioned(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		current = ""
	} else if err != nil {
		return "", err
	}
	if current != revision {
		return "", ErrObjectConflict
	}
	if err := s.Put(ctx, key, data); err != nil {
		return "", err
	}
	return objectRevision(data), nil
}
