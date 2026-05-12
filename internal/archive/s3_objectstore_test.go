package archive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestParseS3StoreURL(t *testing.T) {
	got, err := parseS3StoreURL("s3://archive-bucket/root/prefix?region=us-west-2&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	if got.Bucket != "archive-bucket" {
		t.Fatalf("bucket = %q", got.Bucket)
	}
	if got.Prefix != "root/prefix" {
		t.Fatalf("prefix = %q", got.Prefix)
	}
	if got.Region != "us-west-2" {
		t.Fatalf("region = %q", got.Region)
	}
	if got.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("endpoint = %q", got.Endpoint)
	}
	if !got.UsePathStyle {
		t.Fatal("expected path-style addressing")
	}
}

func TestParseS3StoreURLDefaultsPathStyleForEndpoint(t *testing.T) {
	got, err := parseS3StoreURL("s3://bucket?endpoint=http%3A%2F%2Fminio.local%3A9000")
	if err != nil {
		t.Fatal(err)
	}
	if !got.UsePathStyle {
		t.Fatal("expected endpoint override to default to path-style addressing")
	}
}

func TestParseS3StoreURLRejectsInvalidEndpoint(t *testing.T) {
	for _, rawURL := range []string{
		"s3://bucket?endpoint=ftp%3A%2F%2Fminio.local",
		"s3://bucket?endpoint=http%3A%2F%2F",
		"s3://bucket?endpoint=http%3A%2F%2Fuser%3Apass%40minio.local",
		"s3://bucket?endpoint=http%3A%2F%2Fminio.local%2Fbucket",
		"s3://bucket?endpoint=http%3A%2F%2Fminio.local%3Ftoken%3Dsecret",
		"s3://bucket?endpoint=http%3A%2F%2Fminio.local%23fragment",
	} {
		t.Run(rawURL, func(t *testing.T) {
			_, err := parseS3StoreURL(rawURL)
			if err == nil || !strings.Contains(err.Error(), "invalid endpoint") {
				t.Fatalf("err=%v, want invalid endpoint", err)
			}
		})
	}
}

func TestParseS3StoreURLRejectsUnknownQueryParameters(t *testing.T) {
	_, err := parseS3StoreURL("s3://bucket/root?region=us-east-1&pathstyle=true")
	if err == nil || !strings.Contains(err.Error(), "unsupported s3 store query parameter") {
		t.Fatalf("err=%v, want unsupported query parameter", err)
	}
}

func TestParseS3StoreURLRequiresBucket(t *testing.T) {
	if _, err := parseS3StoreURL("s3:///missing-bucket"); err == nil {
		t.Fatal("expected missing bucket error")
	}
}

func TestParseS3StoreURLRejectsUnsafePrefix(t *testing.T) {
	for _, rawURL := range []string{
		"s3://bucket/../escape",
		"s3://bucket/root/../escape",
		"s3://bucket/root//escape",
		"s3://bucket/root%2F..%2Fescape",
		"s3://bucket/root%2F%2Fescape",
	} {
		t.Run(rawURL, func(t *testing.T) {
			_, err := parseS3StoreURL(rawURL)
			if err == nil || !strings.Contains(err.Error(), "s3 prefix") {
				t.Fatalf("err=%v, want s3 prefix validation error", err)
			}
		})
	}
}

func TestS3ObjectStoreOperationsAgainstCompatibleEndpoint(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := newFakeS3Endpoint(t)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	exists, existsErr := store.Exists(ctx, "segments/missing.cba")
	if existsErr != nil || exists {
		t.Fatalf("missing exists=%v err=%v, want false nil", exists, existsErr)
	}
	if _, getErr := store.Get(ctx, "segments/missing.cba"); !errors.Is(getErr, ErrObjectNotFound) {
		t.Fatalf("missing get err=%v, want ErrObjectNotFound", getErr)
	}
	if putErr := store.Put(ctx, "segments/2.cba", []byte("second")); putErr != nil {
		t.Fatal(putErr)
	}
	if putErr := store.PutIfAbsent(ctx, "segments/1.cba", []byte("first")); putErr != nil {
		t.Fatal(putErr)
	}
	if putErr := store.PutIfAbsent(ctx, "segments/1.cba", []byte("replacement")); !errors.Is(putErr, ErrObjectAlreadyExists) {
		t.Fatalf("PutIfAbsent existing err=%v, want ErrObjectAlreadyExists", putErr)
	}
	got, err := store.Get(ctx, "segments/1.cba")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Fatalf("get returned %q", got)
	}
	stat, err := store.Stat(ctx, "segments/2.cba")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Key != "segments/2.cba" || stat.Size != int64(len("second")) {
		t.Fatalf("unexpected stat: %+v", stat)
	}
	infos, err := store.List(ctx, "segments/")
	if err != nil {
		t.Fatal(err)
	}
	if gotKeys := objectInfoKeys(infos); strings.Join(gotKeys, ",") != "segments/1.cba,segments/2.cba" {
		t.Fatalf("list keys = %v", gotKeys)
	}
}

func TestS3ObjectStorePutReturningETagMatchesLocalMD5(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := newFakeS3Endpoint(t)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("etag-verify-payload")
	etag, err := store.PutIfAbsentReturningETag(context.Background(), "segments/etag.cba", body)
	if err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum(body)
	want := hex.EncodeToString(sum[:])
	if etag != want {
		t.Fatalf("etag = %q, want %q", etag, want)
	}
	etag2, err := store.PutReturningETag(context.Background(), "segments/etag2.cba", body)
	if err != nil {
		t.Fatal(err)
	}
	if etag2 != want {
		t.Fatalf("put etag = %q, want %q", etag2, want)
	}
}

func TestS3ObjectStoreListStaysWithinConfiguredPrefix(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := newFakeS3Endpoint(t)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if putErr := store.Put(ctx, "segments/1.cba", []byte("first")); putErr != nil {
		t.Fatal(putErr)
	}
	putFakeS3Object(t, endpoint.URL, "root/prefix-extra/segments/leak.cba", []byte("outside namespace"))
	putFakeS3Object(t, endpoint.URL, "root/prefix/segments-extra/leak.cba", []byte("outside requested prefix"))
	putFakeS3Object(t, endpoint.URL, `root/prefix/bad\key`, []byte("unsafe key"))

	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if gotKeys := objectInfoKeys(all); strings.Join(gotKeys, ",") != "segments-extra/leak.cba,segments/1.cba" {
		t.Fatalf("all list keys = %v", gotKeys)
	}
	segments, err := store.List(ctx, "segments/")
	if err != nil {
		t.Fatal(err)
	}
	if gotKeys := objectInfoKeys(segments); strings.Join(gotKeys, ",") != "segments/1.cba" {
		t.Fatalf("segments list keys = %v", gotKeys)
	}
	plainSegments, err := store.List(ctx, "segments")
	if err != nil {
		t.Fatal(err)
	}
	if gotKeys := objectInfoKeys(plainSegments); strings.Join(gotKeys, ",") != "segments/1.cba" {
		t.Fatalf("plain segments list keys = %v", gotKeys)
	}
}

func TestS3ObjectStoreRejectsUnsafeObjectKeys(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := newFakeS3Endpoint(t)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "get", run: func() error { _, err := store.Get(ctx, "../escape"); return err }},
		{name: "put", run: func() error { return store.Put(ctx, "../escape", []byte("x")) }},
		{name: "put-if-absent", run: func() error { return store.PutIfAbsent(ctx, "../escape", []byte("x")) }},
		{name: "exists", run: func() error { _, err := store.Exists(ctx, "../escape"); return err }},
		{name: "stat", run: func() error { _, err := store.Stat(ctx, "../escape"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), "invalid object key") {
				t.Fatalf("err=%v, want invalid object key", err)
			}
		})
	}
}

func TestS3ObjectStoreRejectsUnsafeListPrefix(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := newFakeS3Endpoint(t)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.List(context.Background(), "../escape")
	if err == nil || !strings.Contains(err.Error(), "invalid object prefix") {
		t.Fatalf("err=%v, want invalid object prefix", err)
	}
}

func TestS3ObjectStoreRejectsOversizedGetByContentLength(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(maxS3ObjectBytes+1, 10))
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("too large")); err != nil {
			t.Errorf("write oversized response: %v", err)
		}
	}))
	t.Cleanup(endpoint.Close)
	store, err := NewS3ObjectStore(context.Background(), "s3://archive-bucket/root/prefix?region=us-east-1&endpoint="+url.QueryEscape(endpoint.URL)+"&path_style=true")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Get(context.Background(), "segments/huge.cba")
	if err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("err=%v, want max object size error", err)
	}
}

func newFakeS3Endpoint(t *testing.T) *httptest.Server {
	t.Helper()
	objects := make(map[string][]byte)
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/archive-bucket" && !strings.HasPrefix(r.URL.Path, "/archive-bucket/") {
			http.Error(w, "unknown bucket", http.StatusNotFound)
			return
		}
		key := strings.Trim(strings.TrimPrefix(r.URL.Path, "/archive-bucket"), "/")
		if r.URL.Query().Get("list-type") == "2" {
			prefix := r.URL.Query().Get("prefix")
			mu.Lock()
			var contents []fakeS3Object
			for objectKey, data := range objects {
				if strings.HasPrefix(objectKey, prefix) {
					contents = append(contents, fakeS3Object{Key: objectKey, Size: len(data)})
				}
			}
			mu.Unlock()
			slices.SortFunc(contents, func(a, b fakeS3Object) int {
				if a.Key < b.Key {
					return -1
				}
				if a.Key > b.Key {
					return 1
				}
				return 0
			})
			w.Header().Set("Content-Type", "application/xml")
			if err := xml.NewEncoder(w).Encode(fakeS3ListResult{XMLName: xml.Name{Local: "ListBucketResult"}, Contents: contents}); err != nil {
				t.Errorf("encode list response: %v", err)
			}
			return
		}
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") == "*" {
				mu.Lock()
				_, exists := objects[key]
				mu.Unlock()
				if exists {
					writeS3Error(t, w, "PreconditionFailed", http.StatusPreconditionFailed)
					return
				}
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mu.Lock()
			objects[key] = data
			mu.Unlock()
			sum := md5.Sum(data)
			w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			data, ok := objects[key]
			mu.Unlock()
			if !ok {
				writeS3Error(t, w, "NoSuchKey", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", stringLen(data))
			if _, writeErr := w.Write(data); writeErr != nil {
				t.Errorf("write get response: %v", writeErr)
			}
		case http.MethodHead:
			mu.Lock()
			data, ok := objects[key]
			mu.Unlock()
			if !ok {
				writeS3Error(t, w, "NotFound", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", stringLen(data))
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

type fakeS3ListResult struct {
	XMLName  xml.Name       `xml:"ListBucketResult"`
	Contents []fakeS3Object `xml:"Contents"`
}

type fakeS3Object struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func writeS3Error(t *testing.T, w http.ResponseWriter, code string, status int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if _, err := io.WriteString(w, `<Error><Code>`+code+`</Code></Error>`); err != nil {
		t.Errorf("write S3 error response: %v", err)
	}
}

func stringLen(data []byte) string {
	return strconv.Itoa(len(data))
}

func objectInfoKeys(infos []ObjectInfo) []string {
	keys := make([]string, 0, len(infos))
	for _, info := range infos {
		keys = append(keys, info.Key)
	}
	return keys
}

func putFakeS3Object(t *testing.T, endpointURL string, key string, data []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, endpointURL+"/archive-bucket/"+key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put fake S3 object status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}
