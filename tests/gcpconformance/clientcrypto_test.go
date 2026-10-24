//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// TestAESKeyWrapWithPadding checks the RFC 5649 §6 test vector: 20 octets of
// key data wrapped with a 192-bit KEK.
func TestAESKeyWrapWithPadding(t *testing.T) {
	kek, _ := hex.DecodeString("5840df6e29b02af1ab493b705bf16ea1ae8338f4dcc176a8")
	key, _ := hex.DecodeString("c37b7e6492584340bed12207808941155068f738")
	want, _ := hex.DecodeString("138bdeaa9b8fa7fc61f97742e72248ee5ae6ae5360d1ae6a5f54f373fa543b6a")
	got, err := aesKeyWrapWithPadding(kek, key)
	if err != nil {
		t.Fatalf("aesKeyWrapWithPadding: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("wrapped = %x, want %x", got, want)
	}
}
