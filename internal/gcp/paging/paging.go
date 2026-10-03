// Package paging provides the cursor-based pagination shared by the GCP v1
// service providers (Pub/Sub, Secret Manager, KMS, IAM). It honors the
// pageSize/pageToken query parameters and emits nextPageToken, matching the
// GCP REST convention (distinct from GCS's maxResults).
package paging

import (
	"encoding/base64"
	"sort"
	"strconv"

	"jaiscloud/internal/store"
)

// Apply returns the page of ResourceEntry values for the current request and
// the next-page token (empty when there are no more results).
func Apply(entries []store.ResourceEntry, params map[string]any) (page []store.ResourceEntry, nextToken string) {
	return Page(entries, func(e store.ResourceEntry) string { return e.ID }, params)
}

// Page is the generic cursor-pagination core: it sorts items by key and
// applies pageSize/pageToken cursor pagination, returning the page and the
// next-page token (empty when there are no more results).
func Page[T any](items []T, key func(T) string, params map[string]any) (page []T, nextToken string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })

	size := PageSize(params)
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
			for start < len(items) && key(items[start]) <= cursor {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextToken = encodeCursor(key(items[end-1]))
	}
	return page, nextToken
}

// PageSize parses the pageSize query parameter (string, int, or float64),
// falling back to the GCP default of 1000.
func PageSize(params map[string]any) int {
	switch v := params["pageSize"].(type) {
	case string:
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	case int:
		if v > 0 {
			return v
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	}
	return 1000
}

func encodeCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeCursor(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}
	return string(b)
}
