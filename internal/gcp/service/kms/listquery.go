package kms

// This file wires the documented `filter`/`orderBy` list parameters Cloud KMS
// used to drop: keyRings.list, cryptoKeys.list, cryptoKeyVersions.list and
// importJobs.list all document an AIP-160 `filter` and a `name` `orderBy`
// (https://cloud.google.com/kms/docs/sorting-and-filtering). The transports
// used to hand every list straight to paging.Page, so a client that filtered or
// sorted silently received the unfiltered, id-ascending list. Anything outside
// the supported grammar is INVALID_ARGUMENT rather than silently ignored.
//
// The filter and order are applied in the transport-neutral core so REST and
// gRPC cannot drift. The filter is a conjunction of clauses over the identity,
// purpose, label and timestamp fields each resource's schema exposes; the
// orderBy is the documented `name` (ascending, or `name desc`), and for
// crypto-key versions the name sort is numeric by version number, as the docs
// require.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	"jaiscloud/internal/gcp/paging"
	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// ListQuery is the documented list request shared by the KMS list methods: the
// AIP-160 filter, the name orderBy, and the pageSize/pageToken cursor.
type ListQuery struct {
	Filter    string
	OrderBy   string
	PageSize  int
	PageToken string
}

// pageParams renders the cursor parameters paging.Page/PageOrdered consume.
func (q ListQuery) pageParams() map[string]any {
	return map[string]any{"pageSize": q.PageSize, "pageToken": q.PageToken}
}

// PageKeyRings filters, orders by name and pages a location's key rings,
// returning the page, the next-page token (empty on the last page) and the
// number of key rings that matched the filter.
func (s *Service) PageKeyRings(ctx context.Context, project, location string, q ListQuery) (page []kmsstore.KeyRing, next string, total int, err error) {
	krs, err := s.ListKeyRings(ctx, project, location)
	if err != nil {
		return nil, "", 0, err
	}
	return pageKMS(krs, q, "kms keyRings.list", keyRingFilterFields(project, location), keyRingOrderSpec)
}

// PageCryptoKeys filters, orders by name and pages a key ring's crypto keys,
// applying any due rotation first (as ListCryptoKeys does).
func (s *Service) PageCryptoKeys(ctx context.Context, project, location, kr string, q ListQuery) (page []kmsstore.CryptoKey, next string, total int, err error) {
	keys, err := s.ListCryptoKeys(ctx, project, location, kr)
	if err != nil {
		return nil, "", 0, err
	}
	return pageKMS(keys, q, "kms cryptoKeys.list", cryptoKeyFilterFields(project), cryptoKeyOrderSpec)
}

// PageVersions filters, orders by name and pages a crypto key's versions,
// applying any elapsed destruction window first (as ListVersions does).
func (s *Service) PageVersions(ctx context.Context, project, location, kr, key string, q ListQuery) (page []kmsstore.Version, next string, total int, err error) {
	versions, err := s.ListVersions(ctx, project, location, kr, key)
	if err != nil {
		return nil, "", 0, err
	}
	return pageKMS(versions, q, "kms cryptoKeyVersions.list", versionFilterFields(project, location, kr, key), versionOrderSpec)
}

// PageImportJobs filters, orders by name and pages a key ring's import jobs,
// applying the expiry state first (as ListImportJobs does) so a filter on
// `state` sees the value the response renders. importJobs.list documents the
// same filter/orderBy pair as the other KMS lists.
func (s *Service) PageImportJobs(ctx context.Context, project, location, kr string, q ListQuery) (page []ImportJob, next string, total int, err error) {
	jobs, err := s.ListImportJobs(ctx, project, location, kr)
	if err != nil {
		return nil, "", 0, err
	}
	return pageKMS(jobs, q, "kms importJobs.list", importJobFilterFields(), importJobOrderSpec)
}

// pageKMS compiles the filter, retains the matching items, applies the orderBy
// (or the default key order) and pages. total is the matched count, which the
// response's totalSize carries.
func pageKMS[T any](items []T, q ListQuery, label string, fields aip160.Fields[T], spec aip160.OrderSpec[T]) ([]T, string, int, error) {
	filter, err := aip160.Compile(label, q.Filter, fields)
	if err != nil {
		return nil, "", 0, err
	}
	kept := make([]T, 0, len(items))
	for _, it := range items {
		if filter.Match(it) {
			kept = append(kept, it)
		}
	}
	params := q.pageParams()
	if strings.TrimSpace(q.OrderBy) != "" {
		order, err := spec.Parse(q.OrderBy)
		if err != nil {
			return nil, "", 0, invalidArgument(label + " orderBy: " + err.Error())
		}
		order.Sort(kept)
		page, next, err := paging.PageOrdered(kept, params, order.Cursor, order.AfterCursor)
		return page, next, len(kept), err
	}
	page, next, err := paging.Page(kept, spec.Tie, params)
	return page, next, len(kept), err
}

// ─── filter fields ────────────────────────────────────────────────────────────

func keyRingFilterFields(project, location string) aip160.Fields[kmsstore.KeyRing] {
	return aip160.Fields[kmsstore.KeyRing]{
		Desc: "name and createTime",
		Name: func(kr kmsstore.KeyRing) string { return KeyRingName(project, kr.Location, kr.ID) },
		Times: map[string]func(kmsstore.KeyRing) time.Time{
			"createtime": func(kr kmsstore.KeyRing) time.Time { return kr.CreateTime },
		},
	}
}

func cryptoKeyFilterFields(project string) aip160.Fields[kmsstore.CryptoKey] {
	return aip160.Fields[kmsstore.CryptoKey]{
		Desc:   "name, purpose, labels.<key>, labels and createTime",
		Name:   func(k kmsstore.CryptoKey) string { return CryptoKeyName(project, k.Location, k.KeyRingID, k.ID) },
		Labels: func(k kmsstore.CryptoKey) map[string]string { return k.Labels },
		Strings: map[string]func(kmsstore.CryptoKey) string{
			"purpose": func(k kmsstore.CryptoKey) string { return k.Purpose },
		},
		Times: map[string]func(kmsstore.CryptoKey) time.Time{
			"createtime": func(k kmsstore.CryptoKey) time.Time { return k.CreateTime },
		},
	}
}

func versionFilterFields(project, location, kr, key string) aip160.Fields[kmsstore.Version] {
	name := func(v kmsstore.Version) string { return VersionName(project, location, kr, key, v.Version) }
	state := func(v kmsstore.Version) string { return v.State }
	algorithm := func(v kmsstore.Version) string { return v.Algorithm }
	return aip160.Fields[kmsstore.Version]{
		Desc: "name, state, algorithm, protectionLevel, createTime and destroyTime",
		Name: name,
		Strings: map[string]func(kmsstore.Version) string{
			"state":     state,
			"algorithm": algorithm,
			// The store leaves ProtectionLevel empty for a software key; the API
			// renders that as SOFTWARE, so the filter must too.
			"protectionlevel": func(v kmsstore.Version) string {
				if v.ProtectionLevel == "" {
					return "SOFTWARE"
				}
				return v.ProtectionLevel
			},
		},
		Times: map[string]func(kmsstore.Version) time.Time{
			"createtime":  func(v kmsstore.Version) time.Time { return v.CreateTime },
			"destroytime": func(v kmsstore.Version) time.Time { return v.DestroyTime },
		},
	}
}

// importJobFilterFields describes the bounded subset of ImportJob fields the
// emulator models and the KMS "Sorting and filtering list results" page makes
// filterable. The job's canonical name is stored on the record, so the name
// clause matches the rendered resource name; protectionLevel and publicKeyFormat
// are normalized exactly as both renderers normalize them, so a filter can never
// disagree with the response body. Schema fields the emulator does not model
// (expireEventTime, attestation, cryptoKeyBackend) are rejected with
// INVALID_ARGUMENT rather than silently ignored, and are recorded as DC42.
func importJobFilterFields() aip160.Fields[ImportJob] {
	return aip160.Fields[ImportJob]{
		Desc: "name, importMethod, protectionLevel, publicKeyFormat, state, createTime, generateTime and expireTime",
		Name: func(ij ImportJob) string { return ij.Name },
		Strings: map[string]func(ImportJob) string{
			"importmethod": func(ij ImportJob) string { return ij.ImportMethod },
			// An empty stored protection level renders as SOFTWARE, so the
			// filter must normalize it the same way.
			"protectionlevel": func(ij ImportJob) string {
				if ij.ProtectionLevel == "" {
					return "SOFTWARE"
				}
				return ij.ProtectionLevel
			},
			"state": func(ij ImportJob) string { return ij.State },
			// publicKeyFormat is derived exactly as both renderers derive it, so
			// the filter cannot disagree with the response body.
			"publickeyformat": func(ij ImportJob) string {
				switch {
				case len(ij.PublicKeyRaw) > 0:
					if ij.ImportMethod == "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM" {
						return "XWING_RAW_BYTES"
					}
					return "NIST_PQC"
				case ij.PublicKeyPEM != "":
					return "PEM"
				}
				return ""
			},
		},
		Times: map[string]func(ImportJob) time.Time{
			"createtime":   func(ij ImportJob) time.Time { return ij.CreateTime },
			"generatetime": func(ij ImportJob) time.Time { return ij.GenerateTime },
			"expiretime":   func(ij ImportJob) time.Time { return ij.ExpireTime },
		},
	}
}

// ─── orderBy specs ────────────────────────────────────────────────────────────
//
// Cloud KMS documents a single orderable field, `name` (ascending, or
// "name desc"); its tie key is the item's id, so the page cursor of the default
// order is the id the previous page ended on.

// keyRingOrderSpec orders key rings by name (the ring id within a location).
var keyRingOrderSpec = aip160.NewOrderSpec(
	func(kr kmsstore.KeyRing) string { return kr.ID },
	aip160.OrderField[kmsstore.KeyRing]{Name: "name", Value: func(kr kmsstore.KeyRing) string { return kr.ID }},
)

// cryptoKeyOrderSpec orders crypto keys by name (the key id within a key ring).
var cryptoKeyOrderSpec = aip160.NewOrderSpec(
	func(k kmsstore.CryptoKey) string { return k.ID },
	aip160.OrderField[kmsstore.CryptoKey]{Name: "name", Value: func(k kmsstore.CryptoKey) string { return k.ID }},
)

// versionOrderSpec orders crypto-key versions by name; the docs require the
// version number sort to be numeric ("1" before "2", not "10" before "2"), so
// the value is zero-padded.
var versionOrderSpec = aip160.NewOrderSpec(
	func(v kmsstore.Version) string { return versionOrderKey(v.Version) },
	aip160.OrderField[kmsstore.Version]{Name: "name", Value: func(v kmsstore.Version) string { return versionOrderKey(v.Version) }},
)

// importJobOrderSpec orders import jobs by name. Every job in a list shares the
// key-ring prefix, so the job id is the sort key (and the default page cursor).
var importJobOrderSpec = aip160.NewOrderSpec(
	importJobSortKey,
	aip160.OrderField[ImportJob]{Name: "name", Value: importJobSortKey},
)

// importJobSortKey is the trailing segment of an import job's resource name.
func importJobSortKey(ij ImportJob) string { return lastSegment(ij.Name) }

// lastSegment returns the segment after the last slash of a resource name.
func lastSegment(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// versionOrderKey renders a version number so a lexicographic string comparison
// matches the numeric order the API documents. A non-numeric id (which the
// store never allocates) falls back to its literal value.
func versionOrderKey(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return v
	}
	return fmt.Sprintf("%020d", n)
}
