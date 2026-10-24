package kms

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
)

// listItemNames extracts the "name" field of each item in a list response body.
func listItemNames(t *testing.T, resp *model.ProviderResponse, collection string) []string {
	t.Helper()
	raw, ok := resp.Data[collection].([]any)
	if !ok {
		t.Fatalf("%s is not a list: %#v", collection, resp.Data[collection])
	}
	names := make([]string, 0, len(raw))
	for _, it := range raw {
		m, _ := it.(map[string]any)
		n, _ := m["name"].(string)
		names = append(names, n)
	}
	return names
}

// createRings creates key rings under location global.
func createRings(t *testing.T, p *Provider, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := p.KeyRingCreate(context.Background(), newNR(map[string]any{"location": "global", "keyRingId": id})); err != nil {
			t.Fatalf("create key ring %s: %v", id, err)
		}
	}
}

// TestKeyRingListFilterOrderBy verifies keyRings.list honors the documented
// AIP-160 `filter` (over name) and `name`/`name desc` `orderBy`, and that the
// matched count reaches totalSize.
func TestKeyRingListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr-a", "kr-b", "cloud-kr")

	// Substring filter on the full resource name.
	resp, err := p.KeyRingList(ctx, newNR(map[string]any{
		"location": "global", "filter": `name:"cloud"`,
	}))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	names := listItemNames(t, resp, "keyRings")
	if len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/cloud-kr" {
		t.Fatalf("filter name:\"cloud\" = %v, want only cloud-kr", names)
	}
	if resp.Data["totalSize"] != 1 {
		t.Errorf("totalSize = %v, want the matched count 1", resp.Data["totalSize"])
	}

	// Exact filter.
	resp, err = p.KeyRingList(ctx, newNR(map[string]any{
		"location": "global", "filter": `name="projects/proj/locations/global/keyRings/kr-a"`,
	}))
	if err != nil {
		t.Fatalf("exact-filter list: %v", err)
	}
	if names := listItemNames(t, resp, "keyRings"); len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/kr-a" {
		t.Fatalf("exact filter = %v, want only kr-a", names)
	}

	// orderBy name desc.
	resp, err = p.KeyRingList(ctx, newNR(map[string]any{"location": "global", "orderBy": "name desc"}))
	if err != nil {
		t.Fatalf("orderBy list: %v", err)
	}
	got := listItemNames(t, resp, "keyRings")
	want := []string{
		"projects/proj/locations/global/keyRings/kr-b",
		"projects/proj/locations/global/keyRings/kr-a",
		"projects/proj/locations/global/keyRings/cloud-kr",
	}
	if len(got) != len(want) {
		t.Fatalf("orderBy name desc = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderBy name desc = %v, want %v", got, want)
		}
	}
}

// TestKeyRingListFilterComposesWithPaging verifies a filtered, ordered list
// pages without dropping or duplicating a match across the page boundary.
func TestKeyRingListFilterComposesWithPaging(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr-a", "kr-b", "kr-c", "cloud-a", "cloud-b")

	var seen []string
	token := ""
	for i := 0; i < 10; i++ {
		params := map[string]any{"location": "global", "filter": `name:"cloud"`, "orderBy": "name", "pageSize": "1"}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.KeyRingList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		seen = append(seen, listItemNames(t, resp, "keyRings")...)
		token, _ = resp.Data["nextPageToken"].(string)
		if token == "" {
			break
		}
	}
	want := []string{
		"projects/proj/locations/global/keyRings/cloud-a",
		"projects/proj/locations/global/keyRings/cloud-b",
	}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("paged filtered walk = %v, want %v", seen, want)
	}
}

// TestCryptoKeyListFilterLabels verifies cryptoKeys.list filters on labels and
// purpose, and reports the matched count.
func TestCryptoKeyListFilterLabels(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	create := func(id, team string) {
		t.Helper()
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name":        "locations/global/keyRings/kr",
			"cryptoKeyId": id,
			"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT", "labels": map[string]any{"team": team}},
		})); err != nil {
			t.Fatalf("create crypto key %s: %v", id, err)
		}
	}
	create("key-a", "alpha")
	create("key-b", "beta")

	resp, err := p.CryptoKeyList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys", "filter": `labels.team=alpha`,
	}))
	if err != nil {
		t.Fatalf("label-filtered list: %v", err)
	}
	names := listItemNames(t, resp, "cryptoKeys")
	if len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/kr/cryptoKeys/key-a" {
		t.Fatalf("labels.team=alpha = %v, want only key-a", names)
	}
	if resp.Data["totalSize"] != 1 {
		t.Errorf("totalSize = %v, want 1", resp.Data["totalSize"])
	}

	resp, err = p.CryptoKeyList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys", "filter": `purpose=ENCRYPT_DECRYPT`,
	}))
	if err != nil {
		t.Fatalf("purpose-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeys"); len(names) != 2 {
		t.Fatalf("purpose=ENCRYPT_DECRYPT = %v, want both keys", names)
	}
}

// TestCryptoKeyVersionListFilterOrderBy verifies cryptoKeyVersions.list honors
// state filter and numeric `name` orderBy.
func TestCryptoKeyVersionListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "k",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("create key: %v", err)
	}
	parent := "locations/global/keyRings/kr/cryptoKeys/k"
	for i := 0; i < 11; i++ { // version 1 already exists, add 2..12
		if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{"name": parent})); err != nil {
			t.Fatalf("create version: %v", err)
		}
	}

	resp, err := p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "orderBy": "name desc", "pageSize": "20",
	}))
	if err != nil {
		t.Fatalf("version list: %v", err)
	}
	names := listItemNames(t, resp, "cryptoKeyVersions")
	if len(names) != 12 {
		t.Fatalf("got %d versions, want 12", len(names))
	}
	// Numeric descending: version 12 first, version 1 last.
	full := "projects/proj/locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/"
	if names[0] != full+"12" || names[len(names)-1] != full+"1" {
		t.Fatalf("orderBy name desc = %v, want numeric 12..1", names)
	}

	// A state filter that matches nothing returns an empty, non-error page.
	resp, err = p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "filter": `state=DESTROYED`,
	}))
	if err != nil {
		t.Fatalf("state-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeyVersions"); len(names) != 0 {
		t.Fatalf("state=DESTROYED = %v, want none", names)
	}
	if resp.Data["totalSize"] != 0 {
		t.Errorf("filtered totalSize = %v, want 0", resp.Data["totalSize"])
	}

	// A software key's version has an empty stored ProtectionLevel, which the
	// API renders as SOFTWARE; the filter must normalize it the same way.
	resp, err = p.CryptoKeyVersionList(ctx, newNR(map[string]any{
		"name": parent + "/cryptoKeyVersions", "filter": `protectionLevel=SOFTWARE`, "pageSize": "20",
	}))
	if err != nil {
		t.Fatalf("protectionLevel-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "cryptoKeyVersions"); len(names) != 12 {
		t.Fatalf("protectionLevel=SOFTWARE = %d versions, want all 12", len(names))
	}
}

// TestKMSListRejectsBadFilterOrderBy verifies an unsupported filter clause or
// orderBy field is INVALID_ARGUMENT (400), never a silently ignored parameter.
func TestKMSListRejectsBadFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")

	cases := []struct {
		name   string
		params map[string]any
	}{
		{"or-filter", map[string]any{"location": "global", "filter": `name:"a" OR name:"b"`}},
		{"unknown-field", map[string]any{"location": "global", "filter": `bogus=1`}},
		{"bad-operator", map[string]any{"location": "global", "filter": `name<"a"`}},
		{"unknown-order", map[string]any{"location": "global", "orderBy": "createTime"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.KeyRingList(ctx, newNR(tc.params))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			pe, ok := err.(*model.ProviderError)
			if !ok || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
				t.Fatalf("err = %#v, want InvalidArgument/400", err)
			}
		})
	}
}

// createImportJobs creates import jobs under the kr key ring, one per
// id/protection-level pair.
func createImportJobs(t *testing.T, p *Provider, levels map[string]string) {
	t.Helper()
	for id, level := range levels {
		body := map[string]any{"importMethod": "RSA_OAEP_3072_SHA256"}
		if level != "" {
			body["protectionLevel"] = level
		}
		if _, err := p.ImportJobCreate(context.Background(), newNR(map[string]any{
			"name": "locations/global/keyRings/kr", "importJobId": id, "body": body,
		})); err != nil {
			t.Fatalf("create import job %s: %v", id, err)
		}
	}
}

// TestImportJobListFilterOrderBy closes DC23: importJobs.list documents the same
// filter/orderBy pair as the other KMS lists, and used to ignore both (and to
// return every job with no cursor).
func TestImportJobListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	createImportJobs(t, p, map[string]string{
		"job-a": "SOFTWARE", "job-b": "HSM", "cloud-job": "",
	})

	// Substring filter on the full resource name.
	resp, err := p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "filter": `name:"cloud"`,
	}))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	names := listItemNames(t, resp, "importJobs")
	if len(names) != 1 || names[0] != "projects/proj/locations/global/keyRings/kr/importJobs/cloud-job" {
		t.Fatalf("filter name:\"cloud\" = %v, want only cloud-job", names)
	}
	if resp.Data["totalSize"] != 1 {
		t.Errorf("totalSize = %v, want the matched count 1", resp.Data["totalSize"])
	}

	// protectionLevel: the empty stored level renders as SOFTWARE, so the filter
	// must normalize it the same way (and must still tell the HSM job apart).
	resp, err = p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "filter": `protectionLevel=SOFTWARE`,
	}))
	if err != nil {
		t.Fatalf("protectionLevel-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "importJobs"); len(names) != 2 {
		t.Fatalf("protectionLevel=SOFTWARE = %v, want the two SOFTWARE jobs", names)
	}
	resp, err = p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "filter": `protectionLevel=HSM`,
	}))
	if err != nil {
		t.Fatalf("HSM-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "importJobs"); len(names) != 1 ||
		names[0] != "projects/proj/locations/global/keyRings/kr/importJobs/job-b" {
		t.Fatalf("protectionLevel=HSM = %v, want only job-b", names)
	}

	// importMethod and state are filterable ImportJob fields too.
	resp, err = p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "filter": `importMethod=RSA_OAEP_3072_SHA256 state=ACTIVE`,
	}))
	if err != nil {
		t.Fatalf("importMethod+state list: %v", err)
	}
	if names := listItemNames(t, resp, "importJobs"); len(names) != 3 {
		t.Fatalf("importMethod+state filter = %v, want all three ACTIVE jobs", names)
	}
	resp, err = p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "filter": `state=EXPIRED`,
	}))
	if err != nil {
		t.Fatalf("EXPIRED-filtered list: %v", err)
	}
	if names := listItemNames(t, resp, "importJobs"); len(names) != 0 || resp.Data["totalSize"] != 0 {
		t.Fatalf("state=EXPIRED = %v (totalSize %v), want none", names, resp.Data["totalSize"])
	}

	// publicKeyFormat is derived from the stored key, exactly as the renderers
	// derive it (the RSA-wrapped jobs carry a PEM public key).
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`publicKeyFormat=PEM`, 3},
		{`publicKeyFormat=NIST_PQC`, 0},
	} {
		resp, err := p.ImportJobList(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr/importJobs", "filter": tc.filter,
		}))
		if err != nil {
			t.Fatalf("%s: %v", tc.filter, err)
		}
		if names := listItemNames(t, resp, "importJobs"); len(names) != tc.want {
			t.Fatalf("%s = %v, want %d jobs", tc.filter, names, tc.want)
		}
	}

	// Timestamp clauses reach the job's create/generate/expire times. Each pair
	// is discriminating: the lower bound must match all three and the identical
	// upper bound none, so an accessor that returned the zero time cannot pass.
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`createTime>="2000-01-01T00:00:00Z"`, 3},
		{`createTime<"2000-01-01T00:00:00Z"`, 0},
		{`generateTime>"2000-01-01T00:00:00Z"`, 3},
		{`generateTime<"2000-01-01T00:00:00Z"`, 0},
		{`expireTime>"2000-01-01"`, 3},
		{`expireTime<"2000-01-01"`, 0},
	} {
		resp, err := p.ImportJobList(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr/importJobs", "filter": tc.filter,
		}))
		if err != nil {
			t.Fatalf("%s: %v", tc.filter, err)
		}
		if names := listItemNames(t, resp, "importJobs"); len(names) != tc.want {
			t.Fatalf("%s = %v, want %d jobs", tc.filter, names, tc.want)
		}
	}

	// orderBy name desc over the whole key ring.
	resp, err = p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "orderBy": "name desc",
	}))
	if err != nil {
		t.Fatalf("orderBy list: %v", err)
	}
	got := listItemNames(t, resp, "importJobs")
	want := []string{
		"projects/proj/locations/global/keyRings/kr/importJobs/job-b",
		"projects/proj/locations/global/keyRings/kr/importJobs/job-a",
		"projects/proj/locations/global/keyRings/kr/importJobs/cloud-job",
	}
	if len(got) != len(want) {
		t.Fatalf("orderBy name desc = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderBy name desc = %v, want %v", got, want)
		}
	}
	// The unfiltered total is every job in the key ring, not the page length.
	if resp.Data["totalSize"] != 3 {
		t.Errorf("unfiltered totalSize = %v, want 3", resp.Data["totalSize"])
	}
}

// TestImportJobListComposesWithPaging verifies the new cursor: a filtered,
// ordered import-job walk neither drops nor duplicates a job across the page
// boundary, and totalSize reports the matched count once, not per page.
func TestImportJobListComposesWithPaging(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	createImportJobs(t, p, map[string]string{"job-a": "", "job-b": "", "cloud-job": ""})

	var seen []string
	token := ""
	for i := 0; i < 10; i++ {
		params := map[string]any{
			"name":   "locations/global/keyRings/kr/importJobs",
			"filter": `name:"job-"`, "orderBy": "name", "pageSize": "1",
		}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.ImportJobList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		if resp.Data["totalSize"] != 2 {
			t.Errorf("page %d totalSize = %v, want the matched count 2", i, resp.Data["totalSize"])
		}
		page := listItemNames(t, resp, "importJobs")
		if len(page) != 1 {
			t.Fatalf("page %d returned %d jobs (%v), want the requested 1", i, len(page), page)
		}
		seen = append(seen, page...)
		token, _ = resp.Data["nextPageToken"].(string)
		if token == "" {
			// The last page must not carry a token.
			if _, present := resp.Data["nextPageToken"]; present {
				t.Fatalf("the last page of the walk must not carry nextPageToken: %v", resp.Data)
			}
			break
		}
	}
	if len(seen) != 2 {
		t.Fatalf("the walk visited %d pages worth of jobs (%v), want 2", len(seen), seen)
	}
	want := []string{
		"projects/proj/locations/global/keyRings/kr/importJobs/job-a",
		"projects/proj/locations/global/keyRings/kr/importJobs/job-b",
	}
	if len(seen) != len(want) || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("paged filtered walk = %v, want %v", seen, want)
	}

	// A negative pageSize is INVALID_ARGUMENT on the new cursor (AIP-158).
	_, err := p.ImportJobList(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/importJobs", "pageSize": "-1",
	}))
	if err == nil || errStatus(err) != 400 {
		t.Fatalf("pageSize=-1: err = %v, want 400", err)
	}
}

// TestImportJobListRejectsBadFilterOrderBy verifies an import job filter or
// orderBy outside the documented grammar is INVALID_ARGUMENT, never silently
// ignored.
func TestImportJobListRejectsBadFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	createImportJobs(t, p, map[string]string{"job-a": ""})

	cases := []struct {
		name   string
		params map[string]any
	}{
		{"or-filter", map[string]any{"filter": `name:"a" OR name:"b"`}},
		{"unknown-field", map[string]any{"filter": `bogus=1`}},
		{"unknown-order", map[string]any{"orderBy": "createTime"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["name"] = "locations/global/keyRings/kr/importJobs"
			_, err := p.ImportJobList(ctx, newNR(tc.params))
			if err == nil || errStatus(err) != 400 {
				t.Fatalf("err = %v, want 400", err)
			}
		})
	}
}

// TestImportJobListAppliesExpiryBeforeFilter pins the one semantic the filter
// depends on: ListImportJobs marks a job EXPIRED once its window passed, so a
// `state` clause must select the state the response renders, not a stale stored
// ACTIVE that the client never sees.
func TestImportJobListAppliesExpiryBeforeFilter(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	createImportJobs(t, p, map[string]string{"job-a": ""})

	states := func(filter string) []any {
		t.Helper()
		resp, err := p.ImportJobList(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr/importJobs", "filter": filter,
		}))
		if err != nil {
			t.Fatalf("%s: %v", filter, err)
		}
		items, _ := resp.Data["importJobs"].([]any)
		return items
	}

	// Inside the 72h import window the job is ACTIVE.
	if items := states("state=ACTIVE"); len(items) != 1 {
		t.Fatalf("state=ACTIVE within the window = %v, want the job", items)
	}
	if items := states("state=EXPIRED"); len(items) != 0 {
		t.Fatalf("state=EXPIRED within the window = %v, want none", items)
	}

	// Past expire_time the same job lists as EXPIRED.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(73 * time.Hour)})
	items := states("state=EXPIRED")
	if len(items) != 1 {
		t.Fatalf("state=EXPIRED past expire_time = %v, want the job", items)
	}
	item, _ := items[0].(map[string]any)
	if state, _ := item["state"].(string); state != "EXPIRED" {
		t.Fatalf("rendered state = %q, want EXPIRED", state)
	}
	if items := states("state=ACTIVE"); len(items) != 0 {
		t.Fatalf("state=ACTIVE past expire_time = %v, want none", items)
	}
}

// TestListNonIntegerPageSize pins the int32 query binding on every KMS list
// method: an unparseable or fractional pageSize is 400 INVALID_ARGUMENT, never
// folded into zero and served as an absent parameter.
func TestListNonIntegerPageSize(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	calls := map[string]func(*model.NormalizedRequest) (*model.ProviderResponse, error){
		"keyRings": func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.KeyRingList(ctx, nr)
		},
		"cryptoKeys": func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.CryptoKeyList(ctx, nr)
		},
		"versions": func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.CryptoKeyVersionList(ctx, nr)
		},
		"importJobs": func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.ImportJobList(ctx, nr)
		},
	}
	names := map[string]string{
		"keyRings":   "locations/global/keyRings",
		"cryptoKeys": "locations/global/keyRings/kr/cryptoKeys",
		"versions":   "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions",
		"importJobs": "locations/global/keyRings/kr/importJobs",
	}
	for name, call := range calls {
		for _, v := range []any{"abc", "-0.5", "1.5"} {
			_, err := call(newNR(map[string]any{"name": names[name], "pageSize": v}))
			perr, ok := err.(*model.ProviderError)
			if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s pageSize=%#v err = %v, want InvalidArgument/400", name, v, err)
			}
		}
		if _, err := call(newNR(map[string]any{"name": names[name], "pageSize": "5"})); err != nil {
			t.Errorf("%s pageSize=5: %v", name, err)
		}
	}
}
