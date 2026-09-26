package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"jaiscloud/internal/gcp/downscope"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bidiReadChunkSize bounds the object bytes carried by a single
// BidiReadObjectResponse message. It matches the ReadObject chunk size so the
// two read paths put the same message-size pressure on clients.
const bidiReadChunkSize = 2 << 20 // 2 MiB

// maxBidiReadRanges is the documented upper bound on the number of ranges a
// single BidiReadObjectRequest may carry (google.storage.v2.ReadRange list).
const maxBidiReadRanges = 100

// BidiReadObject implements the bidirectional streaming read used by the
// official Go client: the single-range Reader when constructed with
// experimental.WithGRPCBidiReads, and MultiRangeDownloader at any setting.
//
// The first client message must carry the read_object_spec; later messages
// carry read_ranges (a range may be added at any point while the stream is
// open). Each range is served independently as one or more ObjectRangeData
// messages, each keyed by the client's read_id and delivered in increasing
// offset order, with range_end set on the final message of the range.
//
// The first response carries the object metadata and a read_handle the client
// may pass back as BidiReadObjectSpec.read_handle to open a subsequent stream
// without repeating the object identity — the optimization the Go client's
// MultiRangeDownloader relies on when it grows to an additional connection.
// Metadata is omitted only for a handle-only spec (no explicit bucket/object);
// the official client always sends bucket/object with a handle and needs the
// metadata to size the read.
//
// Range-serve note (mirrors ReadObject): objects are stored as a single
// AES-256-GCM blob (`iv || ciphertext+tag`) for both CSEK and the server/CMEK
// envelope. One GCM tag authenticates the whole ciphertext, so a byte window
// cannot be decrypted without processing the entire object; true partial
// decryption is not possible for the on-disk format. The plaintext is therefore
// fetched through the provider (which reads the blob with blobfs.GetStream) and
// decrypted at most once per stream, then each requested range is streamed as a
// zero-copy slice in bounded chunks and never copied into a whole-object
// response message. Unsupported request surface fails loud (see below).
func (s *Service) BidiReadObject(stream storagepb.Storage_BidiReadObjectServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return mapError(model.NewProviderError("InvalidArgument", "BidiReadObject stream closed before read_object_spec", 400))
		}
		return err
	}
	spec := first.GetReadObjectSpec()
	if spec == nil {
		return mapError(model.NewProviderError("InvalidArgument", "the first BidiReadObject message must set read_object_spec", 400))
	}
	// Fail loud on request surface this emulator does not model rather than
	// silently ignoring it: read_mask is deprecated in the proto, and
	// routing_token is only meaningful for the redirect flow (which the
	// emulator never issues).
	if spec.GetReadMask() != nil {
		return status.Error(codes.Unimplemented, "BidiReadObject read_mask is not supported; the response carries the full object metadata")
	}
	if spec.GetRoutingToken() != "" {
		return status.Error(codes.Unimplemented, "BidiReadObject routing_token is not supported (no read redirects)")
	}
	if err := validateBidiReadCommonParams(spec.GetCommonObjectRequestParams()); err != nil {
		return err
	}

	bucket, object, generation, err := s.resolveBidiReadTarget(spec)
	if err != nil {
		return err
	}
	if err := s.requireDownscope(ctx, downscope.ReadObject, bucket, object); err != nil {
		return err
	}
	project := s.projectForBucket(ctx, bucket)
	meta, err := s.bidiReadObjectMeta(ctx, bucket, object, generation)
	if err != nil {
		return err
	}
	if perr := checkObjectPreconditions(meta, spec.IfGenerationMatch, spec.IfGenerationNotMatch, spec.IfMetagenerationMatch, spec.IfMetagenerationNotMatch); perr != nil {
		return mapError(perr)
	}

	var cseKey []byte
	if cop := spec.GetCommonObjectRequestParams(); cop != nil {
		cseKey, _ = cseKeyFromParams(cop)
	}

	// First response: a freshly minted handle bound to the resolved generation,
	// plus the object metadata unless this is the pure "handle-only" optimization
	// (no explicit bucket/object). Real GCS omits metadata whenever a handle is
	// present, but the official Go client always sends bucket+object alongside a
	// handle and derives Reader.Attrs.Size from the metadata; withholding it
	// there makes a negative-offset handle read terminate at size 0. Populating
	// it is a harmless superset for the handle-only case's callers too.
	handleOnly := spec.GetReadHandle() != nil && spec.GetBucket() == "" && spec.GetObject() == ""
	firstResp := &storagepb.BidiReadObjectResponse{
		ReadHandle: &storagepb.BidiReadHandle{Handle: encodeBidiReadHandle(meta.Bucket, meta.Name, meta.Generation)},
	}
	if !handleOnly {
		firstResp.Metadata = objectToProto(meta)
	}
	if err := stream.Send(firstResp); err != nil {
		return err
	}

	// Decrypt the object at most once per stream: a multi-range stream reuses the
	// same plaintext for every range instead of re-reading the blob.
	var plain []byte
	plainLoaded := false
	loadPlain := func() ([]byte, error) {
		if plainLoaded {
			return plain, nil
		}
		_, p, derr := s.provider.GetObjectData(ctx, project, bucket, object, meta.Generation, cseKey)
		if derr != nil {
			return nil, mapError(derr)
		}
		plain, plainLoaded = p, true
		return plain, nil
	}

	serve := func(req *storagepb.BidiReadObjectRequest) error {
		ranges := req.GetReadRanges()
		if len(ranges) > maxBidiReadRanges {
			return mapError(model.NewProviderError("InvalidArgument", "BidiReadObjectRequest carries more than 100 read_ranges", 400))
		}
		// Ranges are served synchronously and in order, so the only ones
		// outstanding at once are those in this message: read_ids must be
		// unique within a message, but the client may reuse an id on a later
		// message once the prior range has completed (the proto requires
		// distinct ids only for concurrently outstanding ranges).
		inFlight := make(map[int64]bool, len(ranges))
		for _, r := range ranges {
			if inFlight[r.GetReadId()] {
				return mapError(model.NewProviderError("InvalidArgument", "read_id values must be unique among a message's read_ranges", 400))
			}
			inFlight[r.GetReadId()] = true
			if err := serveBidiReadRange(stream, meta, r, loadPlain); err != nil {
				return err
			}
		}
		return nil
	}

	if err := serve(first); err != nil {
		return err
	}
	for {
		req, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			return nil
		}
		if rerr != nil {
			return rerr
		}
		if req.GetReadObjectSpec() != nil {
			return mapError(model.NewProviderError("InvalidArgument", "read_object_spec must only be set on the first BidiReadObject message", 400))
		}
		if err := serve(req); err != nil {
			return err
		}
	}
}

// serveBidiReadRange streams one ReadRange as bounded ObjectRangeData messages.
func serveBidiReadRange(stream storagepb.Storage_BidiReadObjectServer, meta gcs.ObjectMeta, r *storagepb.ReadRange, loadPlain func() ([]byte, error)) error {
	start, end, perr := bidiRangeBounds(meta.Size, r.GetReadOffset(), r.GetReadLength())
	if perr != nil {
		return mapError(perr)
	}
	// The echoed range identifies the source request to the client.
	echo := &storagepb.ReadRange{
		ReadOffset: r.GetReadOffset(),
		ReadLength: r.GetReadLength(),
		ReadId:     r.GetReadId(),
	}
	sendEnd := func() error {
		return stream.Send(&storagepb.BidiReadObjectResponse{
			ObjectDataRanges: []*storagepb.ObjectRangeData{{ReadRange: echo, RangeEnd: true}},
		})
	}
	if start == end {
		// A zero-length window (an offset at EOF, or a zero read_length on an
		// empty object) still terminates the range explicitly.
		return sendEnd()
	}

	plain, err := loadPlain()
	if err != nil {
		return err
	}
	// Defensive clamp: metadata size and the decrypted length should agree, but
	// never index past the plaintext actually in hand.
	if end > int64(len(plain)) {
		end = int64(len(plain))
	}
	if start > end {
		start = end
	}
	if start == end {
		return sendEnd()
	}

	data := plain[start:end]
	for len(data) > 0 {
		n := min(bidiReadChunkSize, len(data))
		content := data[:n]
		data = data[n:]
		crc := crc32cOf(content)
		if err := stream.Send(&storagepb.BidiReadObjectResponse{
			ObjectDataRanges: []*storagepb.ObjectRangeData{{
				ChecksummedData: &storagepb.ChecksummedData{Content: content, Crc32C: &crc},
				ReadRange:       echo,
				RangeEnd:        len(data) == 0,
			}},
		}); err != nil {
			return err
		}
	}
	return nil
}

// bidiRangeBounds resolves a ReadRange's read_offset/read_length into a
// [start,end) byte window, per the google.storage.v2.ReadRange contract: a
// negative offset counts back from EOF (clamped to 0 when it overshoots), an
// offset past EOF is OutOfRange, a negative length is OutOfRange, and a length
// of 0 reads to EOF.
func bidiRangeBounds(size, offset, length int64) (start, end int64, perr *model.ProviderError) {
	if length < 0 {
		return 0, 0, model.NewProviderError("OutOfRange", "read_length must not be negative", 400)
	}
	switch {
	case offset < 0:
		start = size + offset
		if start < 0 {
			start = 0
		}
	case offset > size:
		return 0, 0, model.NewProviderError("OutOfRange", "read_offset is past the end of the object", 400)
	default:
		start = offset
	}
	end = size
	// `length <= size-start` avoids the start+length overflow a huge
	// read_length would otherwise cause; length > remaining clamps to EOF.
	if length > 0 && length <= size-start {
		end = start + length
	}
	return start, end, nil
}

// resolveBidiReadTarget resolves the object a BidiReadObjectSpec names, from
// the explicit bucket/object/generation fields, the read_handle, or both. A
// handle that disagrees with the explicit fields is rejected rather than
// silently preferring one.
func (s *Service) resolveBidiReadTarget(spec *storagepb.BidiReadObjectSpec) (bucket, object, generation string, err error) {
	bucket = parseBucketName(spec.GetBucket())
	object = spec.GetObject()
	if g := spec.GetGeneration(); g > 0 {
		generation = int64ToGen(g)
	}
	if h := spec.GetReadHandle(); h != nil {
		hb, ho, hg, ok := decodeBidiReadHandle(h.GetHandle())
		if !ok {
			return "", "", "", mapError(model.NewProviderError("InvalidArgument", "invalid BidiReadObject read_handle", 400))
		}
		if (bucket != "" && bucket != hb) || (object != "" && object != ho) {
			return "", "", "", mapError(model.NewProviderError("InvalidArgument", "read_handle does not match the read_object_spec object", 400))
		}
		// A handle is bound to a generation; an explicit conflicting generation
		// is rejected rather than silently preferred.
		if generation != "" && hg != "" && generation != hg {
			return "", "", "", mapError(model.NewProviderError("InvalidArgument", "read_handle generation does not match read_object_spec generation", 400))
		}
		if bucket == "" {
			bucket = hb
		}
		if object == "" {
			object = ho
		}
		if generation == "" {
			generation = hg
		}
	}
	if bucket == "" || object == "" {
		return "", "", "", mapError(model.NewProviderError("InvalidArgument", "BidiReadObject requires a bucket and object or a read_handle", 400))
	}
	return bucket, object, generation, nil
}

// bidiReadObjectMeta loads the metadata for the target object, mapping the
// store's not-found sentinel to the wire NotFound.
func (s *Service) bidiReadObjectMeta(ctx context.Context, bucket, object, generation string) (gcs.ObjectMeta, error) {
	var (
		meta gcs.ObjectMeta
		err  error
	)
	if generation != "" {
		meta, err = s.objects.GetObjectGeneration(ctx, bucket, object, generation)
	} else {
		meta, err = s.objects.GetObjectMeta(ctx, bucket, object)
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return gcs.ObjectMeta{}, mapError(model.NewProviderError("NotFound", "object not found", 404))
		}
		return gcs.ObjectMeta{}, mapError(err)
	}
	return meta, nil
}

// validateBidiReadCommonParams rejects an encryption algorithm the emulator
// cannot honor. The official clients only ever send "AES256" (alongside the raw
// CSEK) or leave it unset.
func validateBidiReadCommonParams(cop *storagepb.CommonObjectRequestParams) error {
	if cop == nil {
		return nil
	}
	if alg := cop.GetEncryptionAlgorithm(); alg != "" && alg != "AES256" {
		return mapError(model.NewProviderError("InvalidArgument", "unsupported encryption_algorithm "+alg, 400))
	}
	// A malformed key/hash is rejected up front rather than being treated as "no
	// key" (cseKeyFromParams drops a non-32-byte key silently).
	if k := cop.GetEncryptionKeyBytes(); len(k) != 0 && len(k) != 32 {
		return mapError(model.NewProviderError("InvalidArgument", "encryption_key_bytes must be 32 bytes", 400))
	}
	if s := cop.GetEncryptionKeySha256Bytes(); len(s) != 0 && len(s) != 32 {
		return mapError(model.NewProviderError("InvalidArgument", "encryption_key_sha256_bytes must be 32 bytes", 400))
	}
	return nil
}

// ─── read handles ────────────────────────────────────────────────────────────
//
// The handle is opaque to clients. It is the base64url encoding of three
// base64url-encoded parts (bucket, object, generation) joined by '.', so an
// object name containing any byte cannot be confused with a separator. Binding
// the handle to the resolved generation makes a handle-opened stream read the
// same revision the handle was minted from.

func encodeBidiReadHandle(bucket, object, generation string) []byte {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return []byte(enc(bucket) + "." + enc(object) + "." + enc(generation))
}

func decodeBidiReadHandle(handle []byte) (bucket, object, generation string, ok bool) {
	parts := strings.Split(string(handle), ".")
	if len(parts) != 3 {
		return "", "", "", false
	}
	fields := make([]string, 3)
	for i, p := range parts {
		b, err := base64.RawURLEncoding.DecodeString(p)
		if err != nil {
			return "", "", "", false
		}
		fields[i] = string(b)
	}
	if fields[0] == "" || fields[1] == "" {
		return "", "", "", false
	}
	return fields[0], fields[1], fields[2], true
}
