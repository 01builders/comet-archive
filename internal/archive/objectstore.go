package archive

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var (
	ErrObjectConflict      = errors.New("object changed concurrently")
	ErrObjectNotFound      = errors.New("object not found")
	ErrObjectAlreadyExists = errors.New("object already exists")
)

type ObjectInfo struct {
	Key  string
	Size int64
}

type ObjectStore interface {
	PutIfAbsent(ctx context.Context, key string, data []byte) error
	// An empty revision in CompareAndSwap means create only. Revisions are opaque.
	GetVersioned(ctx context.Context, key string) ([]byte, string, error)
	CompareAndSwap(ctx context.Context, key string, data []byte, revision string) (string, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
	Exists(ctx context.Context, key string) (bool, error)
	Stat(ctx context.Context, key string) (ObjectInfo, error)
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

type ImmutableObjectStore interface {
	PutIfAbsent(ctx context.Context, key string, data []byte) error
}

// ETagImmutableObjectStore is an optional capability for object stores
// (currently only S3) that can return an ETag from PutIfAbsent. Callers can
// compare the returned ETag against a locally computed MD5 and skip an extra
// round-trip only on an exact match. Other ETag forms are treated as opaque.
type ETagImmutableObjectStore interface {
	ImmutableObjectStore
	PutIfAbsentReturningETag(ctx context.Context, key string, data []byte) (etag string, err error)
}

// ETagPutter is the analogous capability for unconditional Put. Used when an
// object already-existed path needs revalidation but the store may also
// expose ETag verification.
type ETagPutter interface {
	PutReturningETag(ctx context.Context, key string, data []byte) (etag string, err error)
}

func OpenObjectStore(url string) (ObjectStore, error) {
	if err := ValidateObjectStoreURL(url); err != nil {
		return nil, err
	}
	if after, ok := strings.CutPrefix(url, "file://"); ok {
		return NewLocalObjectStore(after)
	}
	if strings.HasPrefix(url, "s3://") {
		return NewS3ObjectStore(context.Background(), url)
	}
	return nil, fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", url)
}

func OpenObjectStoreReadOnly(url string) (ObjectStore, error) {
	if err := ValidateObjectStoreURL(url); err != nil {
		return nil, err
	}
	if after, ok := strings.CutPrefix(url, "file://"); ok {
		store, err := NewLocalObjectStoreReadOnly(after)
		if err != nil {
			return nil, err
		}
		return readOnlyObjectStore{store: store}, nil
	}
	if strings.HasPrefix(url, "s3://") {
		store, err := NewS3ObjectStore(context.Background(), url)
		if err != nil {
			return nil, err
		}
		return readOnlyObjectStore{store: store}, nil
	}
	return nil, fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", url)
}

type readOnlyObjectStore struct {
	store ObjectStore
}

func (s readOnlyObjectStore) GetVersioned(ctx context.Context, key string) ([]byte, string, error) {
	return s.store.GetVersioned(ctx, key)
}

func (readOnlyObjectStore) CompareAndSwap(context.Context, string, []byte, string) (string, error) {
	return "", errors.New("object store is read-only")
}

func (s readOnlyObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.store.Get(ctx, key)
}

func (readOnlyObjectStore) PutIfAbsent(context.Context, string, []byte) error {
	return errors.New("object store is read-only")
}

func (readOnlyObjectStore) Put(context.Context, string, []byte) error {
	return errors.New("object store is read-only")
}

func (s readOnlyObjectStore) Exists(ctx context.Context, key string) (bool, error) {
	return s.store.Exists(ctx, key)
}

func (s readOnlyObjectStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	return s.store.Stat(ctx, key)
}

func (s readOnlyObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return s.store.List(ctx, prefix)
}

func ValidateObjectStoreURL(rawURL string) error {
	if rawURL == "" {
		return errors.New("object store URL is required")
	}
	if after, ok := strings.CutPrefix(rawURL, "file://"); ok {
		if after == "" {
			return errors.New("file object store URL requires a root path")
		}
		return nil
	}
	if strings.HasPrefix(rawURL, "s3://") {
		_, err := parseS3StoreURL(rawURL)
		return err
	}
	return fmt.Errorf("unsupported object store URL %q: supported schemes are file:// and s3://", rawURL)
}

func ValidateObjectKey(key string) error {
	if key == "" {
		return errors.New("object key is required")
	}
	return validateObjectPath("object key", key)
}

func ValidateObjectPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	pathValue := strings.TrimRight(prefix, "/")
	if pathValue == "" {
		return fmt.Errorf("invalid object prefix %q", prefix)
	}
	return validateObjectPath("object prefix", pathValue)
}

func validateObjectPath(label string, value string) error {
	if strings.Contains(value, "\\") {
		return fmt.Errorf("invalid %s %q", label, value)
	}
	for part := range strings.SplitSeq(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid %s %q", label, value)
		}
	}
	clean := filepath.Clean(filepath.FromSlash(value))
	if clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." || filepath.IsAbs(clean) {
		return fmt.Errorf("invalid %s %q", label, value)
	}
	return nil
}

type LocalObjectStore struct {
	root string
}

func NewLocalObjectStore(root string) (*LocalObjectStore, error) {
	if root == "" {
		return nil, errors.New("local object store root is required")
	}
	if err := rejectLocalObjectStoreRootSymlink(root); err != nil {
		return nil, err
	}
	if err := makeDirectoriesDurable(root); err != nil {
		return nil, err
	}
	return &LocalObjectStore{root: root}, nil
}

func NewLocalObjectStoreReadOnly(root string) (*LocalObjectStore, error) {
	if root == "" {
		return nil, errors.New("local object store root is required")
	}
	if err := rejectLocalObjectStoreRootSymlink(root); err != nil {
		return nil, err
	}
	return &LocalObjectStore{root: root}, nil
}

func (s *LocalObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return nil, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return nil, symlinkErr
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxS3ObjectBytes {
		return nil, errors.New("object exceeds maximum read size")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxS3ObjectBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxS3ObjectBytes {
		return nil, errors.New("object exceeds maximum read size")
	}
	return data, nil
}

func (s *LocalObjectStore) Put(_ context.Context, key string, data []byte) error {
	path, err := s.pathForKey(key)
	if err != nil {
		return err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, filepath.Dir(path)); symlinkErr != nil {
		return symlinkErr
	}
	mkdirErr := makeDirectoriesDurable(filepath.Dir(path))
	if mkdirErr != nil {
		return mkdirErr
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, filepath.Dir(path)); symlinkErr != nil {
		return symlinkErr
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (s *LocalObjectStore) PutIfAbsent(_ context.Context, key string, data []byte) error {
	path, err := s.pathForKey(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if symlinkErr := rejectLocalObjectSymlink(s.root, dir); symlinkErr != nil {
		return symlinkErr
	}
	if mkdirErr := makeDirectoriesDurable(dir); mkdirErr != nil {
		return mkdirErr
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, dir); symlinkErr != nil {
		return symlinkErr
	}
	tmp, err := os.CreateTemp(dir, ".put-if-absent-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmpName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrObjectAlreadyExists
		}
		return err
	}
	return syncDirectory(dir)
}

// Persist newly created directory entries as well as the final object entry.
func makeDirectoriesDurable(path string) error {
	var missing []string
	for dir := filepath.Clean(path); ; dir = filepath.Dir(dir) {
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("object directory %q is not a directory", dir)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, dir)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncDirectory(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	return errors.Join(syncErr, dir.Close())
}

func (s *LocalObjectStore) Exists(_ context.Context, key string) (bool, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return false, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return false, symlinkErr
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("local object %q is a symlink", key)
	}
	return true, nil
}

func (s *LocalObjectStore) Stat(_ context.Context, key string) (ObjectInfo, error) {
	path, err := s.pathForKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if symlinkErr := rejectLocalObjectSymlink(s.root, path); symlinkErr != nil {
		return ObjectInfo{}, symlinkErr
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ObjectInfo{}, ErrObjectNotFound
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ObjectInfo{}, fmt.Errorf("local object %q is a symlink", key)
	}
	return ObjectInfo{Key: key, Size: info.Size()}, nil
}

func (s *LocalObjectStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ValidateObjectPrefix(prefix); err != nil {
		return nil, err
	}
	var infos []ObjectInfo
	if err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".cas-locks" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		validKey := ValidateObjectKey(key) == nil
		if !validKey {
			return nil
		}
		if !objectKeyMatchesListPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		infos = append(infos, ObjectInfo{Key: key, Size: info.Size()})
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(infos, func(a, b ObjectInfo) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return infos, nil
}

func (s *LocalObjectStore) pathForKey(key string) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	return filepath.Join(s.root, clean), nil
}

func rejectLocalObjectSymlink(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := root
	for part := range strings.SplitSeq(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("local object path %q contains symlink %q", path, current)
		}
	}
	return nil
}

func rejectLocalObjectStoreRootSymlink(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("local object store root %q is a symlink", root)
	}
	return nil
}

func objectKeyMatchesListPrefix(key, prefix string) bool {
	if prefix == "" {
		return true
	}
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(key, prefix)
	}
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}
