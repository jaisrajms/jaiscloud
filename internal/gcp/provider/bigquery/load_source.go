package bigquery

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"

	"jaiscloud/internal/model"
)

// loadSpillThreshold is the in-memory size beyond which a load source spills to
// a temp file, so a large gs:// object or uploaded body never occupies the heap
// whole. It is a variable rather than a constant so a test can lower it and
// exercise the spilled path with a small fixture.
var loadSpillThreshold int64 = 4 << 20 // 4 MiB

// errLoadSourceTooLarge reports a source that grew past the load-size cap.
var errLoadSourceTooLarge = errors.New("load source exceeds the size cap")

// loadSpool holds one load source's plaintext bytes: in memory up to
// loadSpillThreshold, then in a temp file this spool owns. It is reopenable —
// each Open yields an independent reader, which is what lets a format probe read
// a schema and then the rows — and random-access for Parquet. Every spool must
// be closed; Close removes the temp file, so a failed load leaves nothing
// behind.
type loadSpool struct {
	buf    []byte
	file   *os.File
	size   int64
	closed bool
}

// newLoadSpool returns an empty spool.
func newLoadSpool() *loadSpool { return &loadSpool{} }

// spoolBytes returns a spool holding b without copying it. b must not be
// modified afterwards (callers pass a decoded request body they no longer use).
func spoolBytes(b []byte) *loadSpool {
	return &loadSpool{buf: b, size: int64(len(b))}
}

// Write appends p, spilling to a temp file once the in-memory part would pass
// the threshold.
func (s *loadSpool) Write(p []byte) (int, error) {
	if s.closed {
		return 0, os.ErrClosed
	}
	if s.file == nil && s.size+int64(len(p)) > loadSpillThreshold {
		if err := s.spill(); err != nil {
			return 0, err
		}
	}
	if s.file != nil {
		n, err := s.file.Write(p)
		s.size += int64(n)
		return n, err
	}
	s.buf = append(s.buf, p...)
	s.size += int64(len(p))
	return len(p), nil
}

// spill flushes the in-memory part to a temp file and switches to file-backed
// accumulation.
func (s *loadSpool) spill() error {
	f, err := os.CreateTemp("", "jaiscloud-gcp-load-*")
	if err != nil {
		return model.NewProviderError("Internal", "failed to spill the load source: "+err.Error(), 500)
	}
	if _, err := f.Write(s.buf); err != nil {
		f.Close()
		os.Remove(f.Name())
		return model.NewProviderError("Internal", "failed to spill the load source: "+err.Error(), 500)
	}
	s.file = f
	s.buf = nil
	return nil
}

// Spilled reports whether the source is file-backed (a test hook).
func (s *loadSpool) Spilled() bool { return s.file != nil }

// Size returns the number of plaintext bytes held.
func (s *loadSpool) Size() int64 { return s.size }

// Open returns a reader over the source from its first byte. The reader is
// independent of any other Open, so a format can be probed for a schema and then
// decoded.
func (s *loadSpool) Open() (io.ReadCloser, error) {
	if s.closed {
		return nil, os.ErrClosed
	}
	if s.file == nil {
		return io.NopCloser(bytes.NewReader(s.buf)), nil
	}
	return io.NopCloser(io.NewSectionReader(s.file, 0, s.size)), nil
}

// ReaderAt returns random access over the source (Parquet needs it) and its
// size.
func (s *loadSpool) ReaderAt() (io.ReaderAt, int64) {
	if s.file == nil {
		return bytes.NewReader(s.buf), s.size
	}
	return io.NewSectionReader(s.file, 0, s.size), s.size
}

// appendReader copies r into the spool, failing with errLoadSourceTooLarge when
// that would pass limit (a non-positive limit means no cap). At most one byte
// past the cap is read, so an oversized body is rejected without buffering it.
func (s *loadSpool) appendReader(r io.Reader, limit int64) error {
	if limit > 0 {
		remaining := limit - s.size + 1
		if remaining < 1 {
			return errLoadSourceTooLarge
		}
		r = io.LimitReader(r, remaining)
	}
	if _, err := io.Copy(s, r); err != nil {
		return err
	}
	if limit > 0 && s.size > limit {
		return errLoadSourceTooLarge
	}
	return nil
}

// Close releases the spool, removing its temp file. It is idempotent.
func (s *loadSpool) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.buf = nil
	if s.file != nil {
		name := s.file.Name()
		err := s.file.Close()
		os.Remove(name) // best effort: the handle is gone either way
		s.file = nil
		return err
	}
	return nil
}

// loadSource is one load source file: its gs:// URI (empty for an uploaded
// media body) and the spool holding its plaintext. The creator owns the spool
// and closes it.
type loadSource struct {
	uri string
	src *loadSpool
}

// closeLoadSources closes every source spool.
func closeLoadSources(sources []loadSource) {
	for _, s := range sources {
		if s.src != nil {
			s.src.Close()
		}
	}
}

// openTextSource opens a source for line/CSV decoding. A gzip-wrapped payload is
// decompressed as a stream — real BigQuery has no `compression` option on
// JobConfigurationLoad, it auto-detects gzip, so detection is by magic bytes —
// and never expanded whole into memory.
func openTextSource(src *loadSpool) (io.ReadCloser, error) {
	rc, err := src.Open()
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(rc, 64*1024)
	magic, _ := br.Peek(2)
	if len(magic) < 2 || magic[0] != 0x1f || magic[1] != 0x8b {
		return &joinedCloser{Reader: br, closers: []io.Closer{rc}}, nil
	}
	zr, err := gzip.NewReader(br)
	if err != nil {
		rc.Close()
		return nil, invalidArgument("failed to read gzip source: " + err.Error())
	}
	return &joinedCloser{Reader: zr, closers: []io.Closer{zr, rc}}, nil
}

// joinedCloser reads from Reader and closes every closer on Close.
type joinedCloser struct {
	io.Reader
	closers []io.Closer
}

func (j *joinedCloser) Close() error {
	var err error
	for _, c := range j.closers {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
