package testdepth

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// This file proves the kit's adapter contract end to end over real HTTP: the
// caller supplies a closure over its own client, and Walk must thread the
// pageToken, pageSize, filter and orderBy it is given. The fake endpoint below
// speaks the documented list shape (items + nextPageToken) so the kit is
// exercised without importing a service client — which would make
// tools/gcpstatus attribute the kit's own suite to that service.

type fakeItem struct {
	Name string `json:"name"`
}

// fakeList is a minimal list endpoint with real cursor mechanics.
type fakeList struct {
	names []string
	// drop removes one resource from every page, the seeded negative for a
	// "client loses a resource across a page boundary" bug.
	drop string

	mu       sync.Mutex
	requests []url.Values
}

func (f *fakeList) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		f.mu.Lock()
		f.requests = append(f.requests, q)
		f.mu.Unlock()

		start, _ := strconv.Atoi(q.Get("pageToken"))
		size, _ := strconv.Atoi(q.Get("pageSize"))
		if size <= 0 || start+size > len(f.names) {
			size = len(f.names) - start
		}
		items := make([]fakeItem, 0, size)
		for _, name := range f.names[start : start+size] {
			if name == f.drop {
				continue
			}
			items = append(items, fakeItem{Name: name})
		}
		token := ""
		if start+size < len(f.names) {
			token = strconv.Itoa(start + size)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "nextPageToken": token})
	})
}

func (f *fakeList) seenTokens() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.requests))
	for _, q := range f.requests {
		out = append(out, q.Get("pageToken"))
	}
	return out
}

// fakeLister is the adapter a suite writes for its own client: one list call per
// page, decoding the documented response shape into a Page.
func fakeLister(base string, client *http.Client) Lister[fakeItem] {
	return func(req ListRequest) (Page[fakeItem], error) {
		u, err := url.Parse(base + "/v1/pagey")
		if err != nil {
			return Page[fakeItem]{}, err
		}
		q := u.Query()
		if req.PageSize > 0 {
			q.Set("pageSize", strconv.FormatInt(req.PageSize, 10))
		}
		if req.PageToken != "" {
			q.Set("pageToken", req.PageToken)
		}
		if req.Filter != "" {
			q.Set("filter", req.Filter)
		}
		if req.OrderBy != "" {
			q.Set("orderBy", req.OrderBy)
		}
		u.RawQuery = q.Encode()

		resp, err := client.Get(u.String())
		if err != nil {
			return Page[fakeItem]{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return Page[fakeItem]{}, fmt.Errorf("list: HTTP %d", resp.StatusCode)
		}
		var body struct {
			Items         []fakeItem `json:"items"`
			NextPageToken string     `json:"nextPageToken"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return Page[fakeItem]{}, err
		}
		return Page[fakeItem]{Items: body.Items, NextPageToken: body.NextPageToken}, nil
	}
}

func TestPagingContractOverHTTP(t *testing.T) {
	fake := &fakeList{names: []string{"a", "b", "c", "d", "e"}}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	cfg := Paging[fakeItem]{
		Name:     func(i fakeItem) string { return i.Name },
		PageSize: 2,
		Filter:   "name:a*",
		OrderBy:  "name",
		Order:    OrderAscendingName,
		Expect:   fake.names,
	}
	factory := func() Lister[fakeItem] { return fakeLister(server.URL, server.Client()) }

	got := Names(AssertPaging(t, factory, cfg), cfg)
	if strings.Join(got, "") != "abcde" {
		t.Fatalf("walked %v, want every resource once in order", got)
	}

	// The contract walk runs twice: the fake must have seen both walks, with the
	// token, size, filter and orderBy the config asked for.
	tokens := fake.seenTokens()
	if len(tokens) != 6 {
		t.Fatalf("tokens = %v, want three pages per walk over two walks", tokens)
	}
	if strings.Join(tokens[:3], ",") != ",2,4" {
		t.Fatalf("first walk tokens = %v, want the server cursor threaded through", tokens[:3])
	}
	fake.mu.Lock()
	first := fake.requests[0]
	fake.mu.Unlock()
	if first.Get("pageSize") != "2" || first.Get("filter") != cfg.Filter || first.Get("orderBy") != cfg.OrderBy {
		t.Fatalf("first request = %v, want pageSize/filter/orderBy forwarded", first)
	}
}

func TestPagingContractOverHTTPFiresOnALostResource(t *testing.T) {
	fake := &fakeList{names: []string{"a", "b", "c", "d", "e"}, drop: "c"}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	cfg := Paging[fakeItem]{
		Name:     func(i fakeItem) string { return i.Name },
		PageSize: 2,
		Order:    OrderAscendingName,
		Expect:   fake.names,
	}
	pages, err := Walk(fakeLister(server.URL, server.Client()), cfg)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	got := strings.Join(CheckPages(pages, cfg), "\n")
	if !strings.Contains(got, `expected resource "c" is missing from the walk`) {
		t.Fatalf("CheckPages reported %q, want the lost-resource violation", got)
	}
}
