package logging

import (
	"encoding/base64"
	"strconv"
)

// encodeOffset / decodeOffset implement the opaque page cursor used by
// ListLogEntries (a base64url-encoded decimal offset), matching the real
// Logging list pagination contract closely enough for a cursor to be stable
// across calls.
func encodeOffset(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeOffset(token string) int {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(b))
	if n < 0 {
		// A negative offset is not a token this service issues; clamp it so a
		// forged token cannot slice the page below zero.
		return 0
	}
	return n
}
