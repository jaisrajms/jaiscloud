package storage

import (
	"encoding/base64"
	"strconv"
)

// GCS entity tags are opaque base64 strings in the JSON API, but real GCS
// derives them from the resource's identity so a client can use the etag as a
// cheap change detector:
//
//	Bucket.etag = base64(proto{ 1: metageneration })
//	Object.etag = base64(proto{ 1: generation, 2: metageneration })
//
// The proto encoding is the wire form of the fields' non-zero values, so
// metageneration 1 is bytes 0x08 0x01 → "CAE=", generation 1502391044760665 and
// metageneration 1 is "CNnIpeSqzdUCEAE=". Both were verified against the live
// JSON API (gcp-public-data-landsat: bucket "CBE=" = metageneration 17, object
// "CNnIpeSqzdUCEAE=" = generation 1502391044760665, metageneration 1).
//
// A constant etag (the previous behaviour) breaks conditional-request clients
// that compare etags, so both helpers derive the value from the resource.

// BucketEtag returns the GCS bucket etag for a metageneration.
func BucketEtag(metageneration string) string {
	n, err := strconv.ParseUint(metageneration, 10, 64)
	if err != nil || n == 0 {
		return "CAE=" // GCS buckets start at metageneration 1
	}
	return base64.StdEncoding.EncodeToString(append([]byte{0x08}, varint(n)...))
}

// ObjectEtag returns the GCS object etag for a generation/metageneration pair.
func ObjectEtag(generation, metageneration string) string {
	g, gerr := strconv.ParseUint(generation, 10, 64)
	if gerr != nil || g == 0 {
		return "CAE="
	}
	m, merr := strconv.ParseUint(metageneration, 10, 64)
	if merr != nil || m == 0 {
		m = 1 // an object's metageneration starts at 1
	}
	buf := append([]byte{0x08}, varint(g)...)
	buf = append(append(buf, 0x10), varint(m)...)
	return base64.StdEncoding.EncodeToString(buf)
}

// varint encodes v as a protobuf base-128 varint.
func varint(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v&0x7f)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}
