package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBadDir verifies that the checker finds violations in testdata/bad/.
func TestBadDir(t *testing.T) {
	violations, err := check("testdata/bad")
	if err != nil {
		t.Fatalf("check(testdata/bad): unexpected error: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("expected at least one violation in testdata/bad, got none")
	}
	joined := strings.Join(violations, "\n")
	// Both flagged methods must appear in the output.
	for _, want := range []string{"ListFoo", "DescribeBar"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected violation for %q; violations:\n%s", want, joined)
		}
	}
	// CreateFoo is not a List/Describe method — must NOT be reported.
	if strings.Contains(joined, "CreateFoo") {
		t.Errorf("CreateFoo should not be flagged; violations:\n%s", joined)
	}
	t.Logf("violations found in testdata/bad (%d):\n%s", len(violations), joined)
}

// TestProviderDir verifies that the checker runs without panic against the
// real provider tree. We expect violations there; this test just ensures the
// tool is stable and does not error out.
func TestProviderDir(t *testing.T) {
	violations, err := check("../../../internal/aws/provider")
	if err != nil {
		t.Fatalf("check(internal/aws/provider): unexpected error: %v", err)
	}
	t.Logf("violations found in internal/aws/provider (%d)", len(violations))
	// Deliberately not asserting zero — that would break as providers evolve.
}

// TestCheck_NoPagination verifies that a method without pagination keywords is
// flagged.
func TestCheck_NoPagination(t *testing.T) {
	violations, err := check("testdata/bad")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) == 0 {
		t.Fatal("expected violations from testdata/bad, got none")
	}
}

// TestCheck_WithPagination verifies that a method whose body contains
// "NextToken" (as a map key or field name) is NOT flagged.
func TestCheck_WithPagination(t *testing.T) {
	src := `package p
type MyProvider struct{}
func (p *MyProvider) ListItems(tok string) []string {
	token, _ := nr.Params["NextToken"].(string)
	_ = token
	return nil
}
`
	dir := writeTempFile(t, "ok.go", src)
	violations, err := check(dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for method with NextToken, got: %v", violations)
	}
}

// TestCheck_NonProviderReceiver verifies that List* methods on types whose
// name does not end in "Provider" are never flagged.
func TestCheck_NonProviderReceiver(t *testing.T) {
	src := `package p
type MyStore struct{}
func (s *MyStore) ListAll() []string { return nil }
`
	dir := writeTempFile(t, "store.go", src)
	violations, err := check(dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for non-Provider receiver, got: %v", violations)
	}
}

// TestCheck_IsTruncated verifies that "IsTruncated" counts as pagination.
func TestCheck_IsTruncated(t *testing.T) {
	src := `package p
type S3Provider struct{}
func (p *S3Provider) ListBuckets() interface{} {
	var IsTruncated bool
	_ = IsTruncated
	return nil
}
`
	dir := writeTempFile(t, "istruncated.go", src)
	violations, err := check(dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations when IsTruncated is present, got: %v", violations)
	}
}

// TestCheck_PagingHelper verifies that the GCP paging.Page helper counts as
// pagination.
func TestCheck_PagingHelper(t *testing.T) {
	src := `package p
import "jaiscloud/internal/gcp/paging"
type MKProvider struct{}
func (p *MKProvider) ListClusters() []string {
	items := []string{"a", "b"}
	page, _ := paging.Page(items, func(s string) string { return s }, nil)
	return page
}
`
	dir := writeTempFile(t, "paging.go", src)
	violations, err := check(dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for method using paging.Page, got: %v", violations)
	}
}

// TestCheck_CrossFileDelegation verifies that a List method which delegates
// pagination to a same-receiver helper in another file is recognized.
func TestCheck_CrossFileDelegation(t *testing.T) {
	listSrc := `package p
type PProvider struct{}
func (p *PProvider) ListClusters() []string {
	return p.pageClusters()
}
`
	helperSrc := `package p
import "jaiscloud/internal/gcp/paging"
func (p *PProvider) pageClusters() []string {
	items := []string{"a"}
	page, _ := paging.Page(items, func(s string) string { return s }, nil)
	return page
}
`
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "list.go"), []byte(listSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "helper.go"), []byte(helperSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	violations, err := check(dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected no violations for cross-file delegated pagination, got: %v", violations)
	}
}

// writeTempFile creates a file named name inside a temp dir and returns the
// directory path (so callers can pass it straight to check).
func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeTempFile: %v", err)
	}
	return dir
}
