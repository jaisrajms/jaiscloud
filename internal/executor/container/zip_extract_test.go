package container

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractZip_Basic(t *testing.T) {
	// Build a zip in memory with one file "hello.txt".
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world")
	f.Write(content)
	w.Close()

	dest := t.TempDir()
	if err := ExtractZip(buf.Bytes(), dest); err != nil {
		t.Fatalf("ExtractZip error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q want %q", got, content)
	}
}

func TestExtractZip_PathTraversal(t *testing.T) {
	// A zip entry with "../escape.txt" must be skipped.
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, _ := w.Create("../escape.txt")
	f.Write([]byte("should not appear"))
	w.Close()

	dest := t.TempDir()
	if err := ExtractZip(buf.Bytes(), dest); err != nil {
		t.Fatalf("ExtractZip error: %v", err)
	}

	// The file must not exist outside dest.
	parent := filepath.Dir(dest)
	entries, _ := os.ReadDir(parent)
	for _, e := range entries {
		if e.Name() == "escape.txt" {
			t.Error("path traversal: escape.txt found outside dest")
		}
	}
}
