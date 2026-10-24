package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Deterministic fixtures shared verbatim across the four languages so the
// observable results can be compared byte-for-byte.

// resumablePayload is the multi-MiB object body used by the resumable-upload
// and streaming-download scenarios. It is deterministic so Python/Java/Node
// produce the same sha256 observable.
func resumablePayload() []byte {
	unit := []byte("jaiscloud-sdk-tour-resumable-payload-")
	const target = 4 << 20 // 4 MiB
	buf := bytes.NewBuffer(make([]byte, 0, target+len(unit)))
	for buf.Len() < target {
		buf.Write(unit)
	}
	return buf.Bytes()[:target]
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// rid joins a leaf with the project + run id so resource names are unique per
// run and per language (the language is not part of the name so every language
// exercises the same logical fixture).
func rid(cfg Config, leaf string) string {
	return fmt.Sprintf("%s-%s", leaf, cfg.RunID)
}
