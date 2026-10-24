package sdk_firestore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func padID(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat("a", n-len(s))
}

// deepPath builds a relative collection/document path with n collection levels,
// each collection/document id padded to idLen bytes.
func deepPath(n, idLen int) string {
	parts := make([]string, 0, 2*n)
	for i := 0; i < n; i++ {
		parts = append(parts, padID(fmt.Sprintf("c%d", i), idLen))
		parts = append(parts, padID(fmt.Sprintf("d%d", i), idLen))
	}
	return strings.Join(parts, "/")
}

// TestSDKFirestoreWriteLimits exercises the Firestore write limits through the
// official client: a document at the 100-collection depth cap is accepted, while
// a 101-level path and a >6 KiB document name are both rejected with
// INVALID_ARGUMENT.
func TestSDKFirestoreWriteLimits(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)

	// 100 collection levels: allowed.
	_, err := client.Doc(deepPath(100, 3)).Set(ctx, map[string]any{"ok": true})
	require.NoError(t, err)

	// 101 collection levels: INVALID_ARGUMENT.
	_, err = client.Doc(deepPath(101, 3)).Set(ctx, map[string]any{"ok": true})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "err=%v", err)

	// A document name over 6 KiB (all ids under the 1500-byte id limit):
	// INVALID_ARGUMENT.
	_, err = client.Doc(deepPath(8, 381)).Set(ctx, map[string]any{"ok": true})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "err=%v", err)
}
