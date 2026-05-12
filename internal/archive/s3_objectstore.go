package archive

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// maxS3ObjectBytes bounds in-memory reads from S3 GetObject as a defense
// against a hostile or misconfigured backend returning unexpectedly large
// payloads. Manifests and segments are validated against expected sizes by
// their callers; this cap is a backstop.
const maxS3ObjectBytes = 8 << 30

type S3ObjectStore struct {
	client *s3.Client
	bucket string
	prefix string
}

type s3StoreConfig struct {
	Bucket       string
	Prefix       string
	Region       string
	Endpoint     string
	UsePathStyle bool
}

func NewS3ObjectStore(ctx context.Context, rawURL string) (*S3ObjectStore, error) {
	parsed, err := parseS3StoreURL(rawURL)
	if err != nil {
		return nil, err
	}
	region := parsed.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.UsePathStyle = parsed.UsePathStyle
		if parsed.Endpoint != "" {
			options.BaseEndpoint = aws.String(parsed.Endpoint)
		}
	})
	return &S3ObjectStore{
		client: client,
		bucket: parsed.Bucket,
		prefix: parsed.Prefix,
	}, nil
}

func parseS3StoreURL(rawURL string) (s3StoreConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return s3StoreConfig{}, err
	}
	if u.Scheme != "s3" {
		return s3StoreConfig{}, fmt.Errorf("unsupported S3 store scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return s3StoreConfig{}, errors.New("s3 store URL must include a bucket")
	}
	query := u.Query()
	for key := range query {
		switch key {
		case "region", "endpoint", "path_style":
		default:
			return s3StoreConfig{}, fmt.Errorf("unsupported s3 store query parameter %q", key)
		}
	}
	endpoint := query.Get("endpoint")
	if err := validateS3Endpoint(endpoint); err != nil {
		return s3StoreConfig{}, err
	}
	pathStyle := endpoint != ""
	if raw := query.Get("path_style"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return s3StoreConfig{}, fmt.Errorf("invalid path_style value %q: %w", raw, err)
		}
		pathStyle = parsed
	}
	prefix := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if prefix != "" {
		if err := ValidateObjectKey(prefix); err != nil {
			return s3StoreConfig{}, fmt.Errorf("s3 prefix: %w", err)
		}
	}
	return s3StoreConfig{
		Bucket:       u.Host,
		Prefix:       prefix,
		Region:       query.Get("region"),
		Endpoint:     endpoint,
		UsePathStyle: pathStyle,
	}, nil
}

func validateS3Endpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid endpoint %q: scheme must be http or https", endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid endpoint %q: host is required", endpoint)
	}
	if u.User != nil {
		return fmt.Errorf("invalid endpoint %q: user info is not supported", endpoint)
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("invalid endpoint %q: path is not supported", endpoint)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid endpoint %q: query and fragment are not supported", endpoint)
	}
	return nil
}

func (s *S3ObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ValidateObjectKey(key); err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if isS3NotFound(err) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	if size := aws.ToInt64(out.ContentLength); size > maxS3ObjectBytes {
		return nil, fmt.Errorf("object %q size %d exceeds max %d", key, size, maxS3ObjectBytes)
	}
	data, err := io.ReadAll(io.LimitReader(out.Body, maxS3ObjectBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxS3ObjectBytes {
		return nil, fmt.Errorf("object %q exceeds max read size %d", key, maxS3ObjectBytes)
	}
	return data, nil
}

func (s *S3ObjectStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutReturningETag(ctx, key, data)
	return err
}

func (s *S3ObjectStore) PutReturningETag(ctx context.Context, key string, data []byte) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", err
	}
	return normalizeETag(aws.ToString(out.ETag)), nil
}

func (s *S3ObjectStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	_, err := s.PutIfAbsentReturningETag(ctx, key, data)
	return err
}

func (s *S3ObjectStore) PutIfAbsentReturningETag(ctx context.Context, key string, data []byte) (string, error) {
	if err := ValidateObjectKey(key); err != nil {
		return "", err
	}
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.fullKey(key)),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	})
	if isS3AlreadyExists(err) {
		return "", ErrObjectAlreadyExists
	}
	if err != nil {
		return "", err
	}
	return normalizeETag(aws.ToString(out.ETag)), nil
}

// normalizeETag strips the surrounding quotes that S3 includes in ETag
// header values and lowercases the hex digest. For non-multipart PUTs the
// returned value is the MD5 of the uploaded body. For multipart uploads the
// ETag is of the form "<md5>-<parts>" and callers must fall back to a
// re-download comparison.
func normalizeETag(etag string) string {
	etag = strings.Trim(etag, "\"")
	return strings.ToLower(etag)
}

func (s *S3ObjectStore) Exists(ctx context.Context, key string) (bool, error) {
	if err := ValidateObjectKey(key); err != nil {
		return false, err
	}
	_, err := s.Stat(ctx, key)
	if errors.Is(err, ErrObjectNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *S3ObjectStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ValidateObjectKey(key); err != nil {
		return ObjectInfo{}, err
	}
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if isS3NotFound(err) {
		return ObjectInfo{}, ErrObjectNotFound
	}
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: key, Size: aws.ToInt64(out.ContentLength)}, nil
}

func (s *S3ObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ValidateObjectPrefix(prefix); err != nil {
		return nil, err
	}
	fullPrefix := s.listFullPrefix(prefix)
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})
	var infos []ObjectInfo
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			objectKey := aws.ToString(object.Key)
			if s.prefix != "" && !strings.HasPrefix(objectKey, s.prefixWithSlash()) {
				continue
			}
			key := strings.TrimPrefix(objectKey, s.prefixWithSlash())
			if err := ValidateObjectKey(key); err != nil {
				continue
			}
			if !objectKeyMatchesListPrefix(key, prefix) {
				continue
			}
			infos = append(infos, ObjectInfo{
				Key:  key,
				Size: aws.ToInt64(object.Size),
			})
		}
	}
	slices.SortFunc(infos, func(a, b ObjectInfo) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return infos, nil
}

func (s *S3ObjectStore) listFullPrefix(prefix string) string {
	hasTrailingSlash := strings.HasSuffix(prefix, "/")
	prefix = strings.TrimLeft(path.Clean("/"+prefix), "/")
	if prefix == "." {
		prefix = ""
	}
	if prefix != "" && hasTrailingSlash {
		prefix += "/"
	}
	if s.prefix == "" {
		return prefix
	}
	if prefix == "" {
		return s.prefixWithSlash()
	}
	joined := path.Join(s.prefix, prefix)
	if hasTrailingSlash && !strings.HasSuffix(joined, "/") {
		joined += "/"
	}
	return joined
}

func (s *S3ObjectStore) fullKey(key string) string {
	key = strings.TrimLeft(path.Clean("/"+key), "/")
	if s.prefix == "" {
		return key
	}
	if key == "" || key == "." {
		return s.prefix
	}
	return path.Join(s.prefix, key)
}

func (s *S3ObjectStore) prefixWithSlash() string {
	if s.prefix == "" {
		return ""
	}
	return strings.Trim(s.prefix, "/") + "/"
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "404")
}

func isS3AlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "412")
}
