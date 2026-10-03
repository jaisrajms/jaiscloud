package monitoring

import (
	"encoding/base64"
	"strconv"
)

// pageSlice applies the cursor pageSize/pageToken convention shared by the
// Monitoring list methods. A non-positive pageSize means the default (10000).
func pageSlice[T any](items []T, pageSize int, token string) ([]T, string) {
	size := pageSize
	if size <= 0 {
		size = 10000
	}
	start := decodeOffset(token)
	if start > len(items) {
		start = len(items)
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeOffset(end)
	}
	return items[start:end], next
}

func encodeOffset(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeOffset(token string) int {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(b))
	return n
}
