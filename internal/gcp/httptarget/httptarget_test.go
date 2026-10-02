package httptarget

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestDeliverSuccessAndHeaders(t *testing.T) {
	var gotMethod, gotBody, gotCT, gotOverride string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotCT = r.Header.Get("Content-Type")
		gotOverride = r.Header.Get("User-Agent")
		buf := make([]byte, 16)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	res := Deliver(context.Background(), server.Client(), Request{
		Method:  "", // defaults to POST
		URL:     server.URL,
		Headers: map[string]string{"User-Agent": "Google-Cloud-Tasks"},
		Body:    []byte("hello"),
	})
	if res.Code != 0 || res.StatusCode != http.StatusCreated {
		t.Fatalf("res = %+v, want 2xx success", res)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotBody != "hello" || gotCT != "application/octet-stream" {
		t.Fatalf("body=%q contentType=%q", gotBody, gotCT)
	}
	if gotOverride != "Google-Cloud-Tasks" {
		t.Fatalf("User-Agent = %q", gotOverride)
	}
}

func TestDeliverNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	res := Deliver(context.Background(), server.Client(), Request{Method: "GET", URL: server.URL})
	if res.Code != http.StatusInternalServerError || res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("res = %+v, want 500", res)
	}
}

func TestDeliverTransportError(t *testing.T) {
	// A closed server yields a transport-level failure (Unavailable).
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	res := Deliver(context.Background(), server.Client(), Request{Method: "GET", URL: url, Deadline: time.Second})
	if res.Code != int32(codes.Unavailable) || res.StatusCode != 0 {
		t.Fatalf("res = %+v, want Unavailable", res)
	}
}

func TestDeliverSkipContentTypeDefault(t *testing.T) {
	var gotCT string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	Deliver(context.Background(), server.Client(), Request{
		Method:                 "POST",
		URL:                    server.URL,
		Body:                   []byte("{}"),
		SkipContentTypeDefault: true,
	})
	if gotCT != "" {
		t.Fatalf("content type = %q, want unset (Cloud Tasks does not set it)", gotCT)
	}
}

func TestDeliverRespectsContentTypeHeader(t *testing.T) {
	var gotCT string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	Deliver(context.Background(), server.Client(), Request{
		Method:  "POST",
		URL:     server.URL,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    []byte(`{}`),
	})
	if gotCT != "application/json" {
		t.Fatalf("content type = %q, want caller value", gotCT)
	}
}
