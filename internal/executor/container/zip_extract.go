package container

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ExtractZip unzips zipBytes into dest directory (created if missing).
// Dirs get mode 0755, files 0644. Skips entries with path traversal.
func ExtractZip(zipBytes []byte, dest string) error {
	r, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return fmt.Errorf("zip_extract: open: %w", err)
	}
	if err := os.MkdirAll(dest, 0755); err != nil {
		return fmt.Errorf("zip_extract: mkdir: %w", err)
	}
	for _, f := range r.File {
		name := filepath.Clean(f.Name)
		if strings.HasPrefix(name, "..") {
			continue
		}
		target := filepath.Join(dest, name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(target, 0755)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		wf, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(wf, rc)
		rc.Close()
		wf.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
