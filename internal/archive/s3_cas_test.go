package archive

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestS3ConditionalPublicationPreservesOpaqueETagAndRetries409(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var attempts atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", `"MiXeD-Opaque"`)
			if _, checkErr := w.Write([]byte("old")); checkErr != nil {
				t.Errorf("write: %v", checkErr)
			}
		case http.MethodPut:
			if r.Header.Get("If-Match") != `"MiXeD-Opaque"` {
				writeS3Error(t, w, "PreconditionFailed", http.StatusPreconditionFailed)
				return
			}
			if attempts.Add(1) <= 2 {
				writeS3Error(t, w, "ConditionalRequestConflict", http.StatusConflict)
				return
			}
			w.Header().Set("ETag", `"NeXt-Opaque"`)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(endpoint.Close)
	store, err := NewS3ObjectStore(t.Context(), "s3://bucket?endpoint="+url.QueryEscape(endpoint.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, revision, err := store.GetVersioned(t.Context(), "manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	next, err := store.CompareAndSwap(t.Context(), "manifest.json", []byte("new"), revision)
	if err != nil || next != `"NeXt-Opaque"` || attempts.Load() != 3 {
		t.Fatalf("next=%q attempts=%d err=%v", next, attempts.Load(), err)
	}
	if _, checkErr := store.CompareAndSwap(t.Context(), "manifest.json", []byte("stale"), `"other"`); !errors.Is(checkErr, ErrObjectConflict) {
		t.Fatalf("stale write: %v", checkErr)
	}
}

func TestS3ConditionalCreationRetryIsBounded(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var attempts atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if r.Header.Get("If-None-Match") != "*" {
			t.Error("missing create-only precondition")
		}
		writeS3Error(t, w, "ConditionalRequestConflict", http.StatusConflict)
	}))
	t.Cleanup(endpoint.Close)
	store, err := NewS3ObjectStore(t.Context(), "s3://bucket?endpoint="+url.QueryEscape(endpoint.URL))
	if err != nil {
		t.Fatal(err)
	}
	if checkErr := store.PutIfAbsent(t.Context(), "segment", []byte("data")); checkErr == nil {
		t.Fatal("persistent conflict succeeded")
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, checkErr := store.CompareAndSwap(ctx, "manifest.json", nil, ""); !errors.Is(checkErr, context.Canceled) {
		t.Fatalf("canceled write: %v", checkErr)
	}
}
