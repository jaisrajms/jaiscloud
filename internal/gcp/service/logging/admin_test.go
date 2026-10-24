package logging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
	store "jaiscloud/internal/store"
)

func newAdminService() *Service {
	return NewService(loggingstore.NewMemoryStore(), "test", WithResources(store.NewMemoryResourceStore()))
}

const (
	testBucketParent = "projects/p/locations/global"
	testBucketName   = "projects/p/locations/global/buckets/b1"
	testViewName     = testBucketName + "/views/v1"
	testLinkName     = testBucketName + "/links/l1"
	testScopeName    = testBucketParent + "/logScopes/s1"
)

func seedBucket(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.CreateBucket(context.Background(), testBucketParent, "b1", loggingstore.LogBucket{Description: "b"}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func TestBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	b, err := s.GetBucket(ctx, testBucketName)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if b.Name != "b1" || b.RetentionDays != 30 || b.LifecycleState != "ACTIVE" {
		t.Fatalf("bucket defaults = %+v", b)
	}

	if _, err := s.CreateBucket(ctx, testBucketParent, "b1", loggingstore.LogBucket{}); err == nil {
		t.Fatalf("duplicate CreateBucket succeeded")
	}

	upd, err := s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{Description: "new", RetentionDays: 7}, []string{"description", "retentionDays"})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if upd.Description != "new" || upd.RetentionDays != 7 {
		t.Fatalf("update = %+v", upd)
	}

	page, _, err := s.ListBuckets(ctx, testBucketParent, 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListBuckets = %v, %v", page, err)
	}

	if err := s.DeleteBucket(ctx, testBucketName); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	b, _ = s.GetBucket(ctx, testBucketName)
	if b.LifecycleState != "DELETE_REQUESTED" {
		t.Fatalf("after delete state = %q", b.LifecycleState)
	}
	if err := s.UndeleteBucket(ctx, testBucketName); err != nil {
		t.Fatalf("UndeleteBucket: %v", err)
	}
	b, _ = s.GetBucket(ctx, testBucketName)
	if b.LifecycleState != "ACTIVE" {
		t.Fatalf("after undelete state = %q", b.LifecycleState)
	}
}

func TestBucketUpdateRestrictedFieldsAndInvalidMask(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	upd, err := s.UpdateBucket(ctx, testBucketName,
		loggingstore.LogBucket{RestrictedFields: []string{"jsonPayload.secret", "labels"}},
		[]string{"restrictedFields"})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if len(upd.RestrictedFields) != 2 || upd.RestrictedFields[0] != "jsonPayload.secret" {
		t.Fatalf("restrictedFields = %v", upd.RestrictedFields)
	}

	locked, err := s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{Locked: true}, []string{"locked"})
	if err != nil || !locked.Locked {
		t.Fatalf("UpdateBucket locked: %+v, %v", locked, err)
	}

	// A path that names no bucket field is a client error, not Unimplemented.
	_, err = s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{}, []string{"labels"})
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("invalid mask = %v, want InvalidArgument/400", err)
	}
}

func TestBucketUpdateNestedCmekMaskAndWildcard(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	created, err := s.CreateBucket(ctx, testBucketParent, "b1", loggingstore.LogBucket{
		Description: "b",
		Cmek: &loggingstore.LogCmekSettings{
			KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/old",
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if created.Cmek == nil || created.Cmek.ServiceAccountID == "" {
		t.Fatalf("bucket cmek = %+v", created.Cmek)
	}
	wantSA := created.Cmek.ServiceAccountID

	// A nested leaf path updates only kmsKeyName; the read-only serviceAccountId
	// is preserved rather than clobbered by a whole-message replace.
	upd, err := s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{
		Cmek: &loggingstore.LogCmekSettings{
			KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/new",
		},
	}, []string{"cmekSettings.kmsKeyName"})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if upd.Cmek == nil || upd.Cmek.KmsKeyName != "projects/p/locations/global/keyRings/r/cryptoKeys/new" {
		t.Fatalf("cmek = %+v", upd.Cmek)
	}
	if upd.Cmek.ServiceAccountID != wantSA {
		t.Fatalf("read-only serviceAccountId clobbered: %q want %q", upd.Cmek.ServiceAccountID, wantSA)
	}

	// A nested path that names no writable subfield is a 400.
	_, err = s.UpdateBucket(ctx, testBucketName, loggingstore.LogBucket{}, []string{"cmekSettings.bogus"})
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("unknown nested cmek mask = %v, want InvalidArgument/400", err)
	}

	// The AIP-134 `*` wildcard replaces every writable field.
	wild, err := s.UpdateBucket(ctx, testBucketName,
		loggingstore.LogBucket{Description: "wild", RetentionDays: 7}, []string{"*"})
	if err != nil {
		t.Fatalf("UpdateBucket *: %v", err)
	}
	if wild.Description != "wild" || wild.RetentionDays != 7 {
		t.Fatalf("wildcard update = %+v", wild)
	}
}

func TestViewLifecycleAndIAM(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	v, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{Filter: "severity>=ERROR"})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if v.Filter != "severity>=ERROR" {
		t.Fatalf("view = %+v", v)
	}
	if _, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{}); err == nil {
		t.Fatalf("duplicate CreateView succeeded")
	}

	upd, err := s.UpdateView(ctx, testViewName, loggingstore.LogView{Description: "d"}, []string{"description"})
	if err != nil {
		t.Fatalf("UpdateView: %v", err)
	}
	if upd.Description != "d" || upd.Filter != "severity>=ERROR" {
		t.Fatalf("view update = %+v", upd)
	}

	page, _, err := s.ListViews(ctx, testBucketName, 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListViews = %v, %v", page, err)
	}

	pol, err := s.ViewGetIamPolicy(ctx, testViewName)
	if err != nil {
		t.Fatalf("ViewGetIamPolicy: %v", err)
	}
	pol, err = s.ViewSetIamPolicy(ctx, testViewName, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:a@example.com"}}}},
	})
	if err != nil {
		t.Fatalf("ViewSetIamPolicy: %v", err)
	}
	if len(pol.Bindings) != 1 {
		t.Fatalf("policy bindings = %v", pol.Bindings)
	}
	perms, err := s.ViewTestIamPermissions(ctx, testViewName, []string{"logging.views.get"})
	if err != nil || len(perms) != 1 {
		t.Fatalf("ViewTestIamPermissions = %v, %v", perms, err)
	}

	if err := s.DeleteView(ctx, testViewName); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if _, err := s.GetView(ctx, testViewName); err == nil {
		t.Fatalf("GetView after delete succeeded")
	}
}

func TestLinkAndLogScope(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)

	l, err := s.CreateLink(ctx, testBucketName, "l1", loggingstore.LogLink{BigQueryDatasetID: "ds"})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	if l.BigQueryDatasetID != "ds" || l.LifecycleState != "ACTIVE" {
		t.Fatalf("link = %+v", l)
	}
	if _, err := s.CreateLink(ctx, testBucketName, "l1", loggingstore.LogLink{BigQueryDatasetID: "ds"}); err == nil {
		t.Fatalf("duplicate CreateLink succeeded")
	}
	if _, err := s.CreateLink(ctx, testBucketName, "l2", loggingstore.LogLink{}); err == nil {
		t.Fatalf("CreateLink without dataset succeeded")
	}
	if got, err := s.GetLink(ctx, testLinkName); err != nil || got.Name != "l1" {
		t.Fatalf("GetLink = %+v, %v", got, err)
	}
	if page, _, err := s.ListLinks(ctx, testBucketName, 0, ""); err != nil || len(page) != 1 {
		t.Fatalf("ListLinks = %v, %v", page, err)
	}
	if err := s.DeleteLink(ctx, testLinkName); err != nil {
		t.Fatalf("DeleteLink: %v", err)
	}

	ls, err := s.CreateLogScope(ctx, testBucketParent, "s1", loggingstore.LogScope{ResourceNames: []string{"projects/p"}})
	if err != nil {
		t.Fatalf("CreateLogScope: %v", err)
	}
	if len(ls.ResourceNames) != 1 {
		t.Fatalf("log scope = %+v", ls)
	}
	if _, err := s.CreateLogScope(ctx, testBucketParent, "s2", loggingstore.LogScope{}); err == nil {
		t.Fatalf("CreateLogScope without resource_names succeeded")
	}
	if _, err := s.UpdateLogScope(ctx, testScopeName, loggingstore.LogScope{Description: "d"}, []string{"description"}); err != nil {
		t.Fatalf("UpdateLogScope: %v", err)
	}
	if page, _, err := s.ListLogScopes(ctx, testBucketParent, 0, ""); err != nil || len(page) != 1 {
		t.Fatalf("ListLogScopes = %v, %v", page, err)
	}
	if err := s.DeleteLogScope(ctx, testScopeName); err != nil {
		t.Fatalf("DeleteLogScope: %v", err)
	}
}

// TestAdminListNonPositivePageSize pins the admin list page-size contract:
// buckets/views/logScopes.list document "Non-positive values are ignored" and
// an over-maximum size is clamped (AIP-158), while links.list is the one config
// list without that documented exception, so a negative size there keeps
// InvalidArgument.
func TestAdminListNonPositivePageSize(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)
	if _, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{Filter: "severity>=ERROR"}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if _, err := s.CreateLink(ctx, testBucketName, "l1", loggingstore.LogLink{BigQueryDatasetID: "ds"}); err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	if _, err := s.CreateLogScope(ctx, testBucketParent, "s1", loggingstore.LogScope{ResourceNames: []string{"projects/p"}}); err != nil {
		t.Fatalf("CreateLogScope: %v", err)
	}

	for _, pageSize := range []int{-1, 1001} {
		if page, _, err := s.ListBuckets(ctx, testBucketParent, pageSize, ""); err != nil || len(page) != 1 {
			t.Fatalf("ListBuckets(%d) = %v, %v; want the seeded bucket", pageSize, page, err)
		}
		if page, _, err := s.ListViews(ctx, testBucketName, pageSize, ""); err != nil || len(page) != 1 {
			t.Fatalf("ListViews(%d) = %v, %v; want the seeded view", pageSize, page, err)
		}
		if page, _, err := s.ListLogScopes(ctx, testBucketParent, pageSize, ""); err != nil || len(page) != 1 {
			t.Fatalf("ListLogScopes(%d) = %v, %v; want the seeded scope", pageSize, page, err)
		}
	}
	if page, _, err := s.ListLinks(ctx, testBucketName, 1001, ""); err != nil || len(page) != 1 {
		t.Fatalf("ListLinks(1001) = %v, %v; want the seeded link (clamped)", page, err)
	}

	_, _, err := s.ListLinks(ctx, testBucketName, -1, "")
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("ListLinks(-1) = %v, want InvalidArgument/400", err)
	}
}

func TestSettingsAndCmek(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	const name = "projects/p"

	st, err := s.GetSettings(ctx, name)
	if err != nil {
		t.Fatalf("GetSettings: %v", err)
	}
	if st.KmsServiceAccountID == "" {
		t.Fatalf("default settings has no service account id")
	}
	upd, err := s.UpdateSettings(ctx, name, loggingstore.LogSettings{StorageLocation: "us"}, []string{"storageLocation"})
	if err != nil {
		t.Fatalf("UpdateSettings: %v", err)
	}
	if upd.StorageLocation != "us" || upd.KmsServiceAccountID == "" {
		t.Fatalf("settings = %+v", upd)
	}

	cm, err := s.GetCmekSettings(ctx, name)
	if err != nil {
		t.Fatalf("GetCmekSettings: %v", err)
	}
	if cm.ServiceAccountID == "" {
		t.Fatalf("default cmek has no service account id")
	}
	if _, err := s.UpdateCmekSettings(ctx, name, loggingstore.LogCmekSettings{}, []string{"kmsKeyName"}); err == nil {
		t.Fatalf("UpdateCmekSettings without kms_key_name succeeded")
	}
	cm, err = s.UpdateCmekSettings(ctx, name, loggingstore.LogCmekSettings{KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/k"}, []string{"kmsKeyName"})
	if err != nil {
		t.Fatalf("UpdateCmekSettings: %v", err)
	}
	if cm.KmsKeyName == "" {
		t.Fatalf("cmek = %+v", cm)
	}
}

func TestCmekSettingsUpdateRejectsReadOnlyMaskPath(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	// kms_key_version_name is read-only (Discovery schema google.logging.v2.CmekSettings),
	// so a mask naming it is a client error even though the request also sets
	// kms_key_name.
	_, err := s.UpdateCmekSettings(ctx, "projects/p", loggingstore.LogCmekSettings{
		KmsKeyName:        "projects/p/locations/global/keyRings/r/cryptoKeys/k",
		KmsKeyVersionName: "projects/p/locations/global/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
	}, []string{"kmsKeyVersionName"})
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("read-only cmek mask = %v, want InvalidArgument/400", err)
	}
}

func TestViewScopeSettingsWildcardMask(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	seedBucket(t, s)
	if _, err := s.CreateView(ctx, testBucketName, "v1", loggingstore.LogView{
		Filter: "severity>=ERROR", Description: "old",
	}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	// `*` replaces every writable view field, clearing an omitted filter.
	upd, err := s.UpdateView(ctx, testViewName, loggingstore.LogView{Description: "new"}, []string{"*"})
	if err != nil {
		t.Fatalf("UpdateView *: %v", err)
	}
	if upd.Description != "new" || upd.Filter != "" {
		t.Fatalf("view wildcard = %+v", upd)
	}

	if _, err := s.CreateLogScope(ctx, testBucketParent, "s1", loggingstore.LogScope{
		ResourceNames: []string{"projects/p"},
	}); err != nil {
		t.Fatalf("CreateLogScope: %v", err)
	}
	sc, err := s.UpdateLogScope(ctx, testScopeName, loggingstore.LogScope{Description: "new"}, []string{"*"})
	if err != nil {
		t.Fatalf("UpdateLogScope *: %v", err)
	}
	if sc.Description != "new" || len(sc.ResourceNames) != 0 {
		t.Fatalf("scope wildcard = %+v", sc)
	}

	st, err := s.UpdateSettings(ctx, "projects/p", loggingstore.LogSettings{StorageLocation: "us"}, []string{"*"})
	if err != nil {
		t.Fatalf("UpdateSettings *: %v", err)
	}
	if st.StorageLocation != "us" || st.KmsServiceAccountID == "" {
		t.Fatalf("settings wildcard = %+v", st)
	}
}

func TestSettingsDefaultSinkConfigNestedMask(t *testing.T) {
	ctx := context.Background()
	s := newAdminService()
	const name = "projects/p"

	// A message-path update replaces the whole DefaultSinkConfig object.
	decoded := func(raw []byte) map[string]any {
		t.Helper()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("decode defaultSinkConfig: %v", err)
		}
		return m
	}
	st, err := s.UpdateSettings(ctx, name, loggingstore.LogSettings{
		DefaultSinkConfig: json.RawMessage(`{"filter":"severity>=ERROR","mode":"APPEND","exclusions":[{"name":"e1","filter":"x"}]}`),
	}, []string{"defaultSinkConfig"})
	if err != nil {
		t.Fatalf("UpdateSettings message: %v", err)
	}
	if got := decoded(st.DefaultSinkConfig); got["filter"] != "severity>=ERROR" || got["mode"] != "APPEND" {
		t.Fatalf("message update = %v", got)
	}

	// A nested leaf path updates only the named leaf, preserving its siblings.
	st, err = s.UpdateSettings(ctx, name, loggingstore.LogSettings{
		DefaultSinkConfig: json.RawMessage(`{"filter":"severity>=WARNING"}`),
	}, []string{"defaultSinkConfig.filter"})
	if err != nil {
		t.Fatalf("UpdateSettings filter: %v", err)
	}
	got := decoded(st.DefaultSinkConfig)
	if got["filter"] != "severity>=WARNING" || got["mode"] != "APPEND" {
		t.Fatalf("filter leaf = %v", got)
	}
	if ex, _ := got["exclusions"].([]any); len(ex) != 1 {
		t.Fatalf("filter leaf dropped exclusions: %v", got)
	}

	// The mode leaf leaves filter/exclusions intact.
	st, err = s.UpdateSettings(ctx, name, loggingstore.LogSettings{
		DefaultSinkConfig: json.RawMessage(`{"mode":"OVERWRITE"}`),
	}, []string{"defaultSinkConfig.mode"})
	if err != nil {
		t.Fatalf("UpdateSettings mode: %v", err)
	}
	got = decoded(st.DefaultSinkConfig)
	if got["mode"] != "OVERWRITE" || got["filter"] != "severity>=WARNING" {
		t.Fatalf("mode leaf = %v", got)
	}

	// The exclusions leaf replaces the repeated field wholesale.
	st, err = s.UpdateSettings(ctx, name, loggingstore.LogSettings{
		DefaultSinkConfig: json.RawMessage(`{"exclusions":[{"name":"e2","filter":"y"},{"name":"e3","filter":"z"}]}`),
	}, []string{"defaultSinkConfig.exclusions"})
	if err != nil {
		t.Fatalf("UpdateSettings exclusions: %v", err)
	}
	got = decoded(st.DefaultSinkConfig)
	if ex, _ := got["exclusions"].([]any); len(ex) != 2 || got["mode"] != "OVERWRITE" {
		t.Fatalf("exclusions leaf = %v", got)
	}

	// An unmappable leaf and a repeated-field element path are client errors.
	for _, path := range []string{"defaultSinkConfig.bogus", "defaultSinkConfig.exclusions.*"} {
		_, uerr := s.UpdateSettings(ctx, name, loggingstore.LogSettings{
			DefaultSinkConfig: json.RawMessage(`{"filter":"severity>=ERROR"}`),
		}, []string{path})
		var perr *model.ProviderError
		if !errors.As(uerr, &perr) || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Fatalf("mask path %q = %v, want InvalidArgument/400", path, uerr)
		}
	}
}

func TestAdminNameValidation(t *testing.T) {
	if _, _, err := ParseBucketName("projects/p/buckets/b"); err == nil {
		t.Fatalf("ParseBucketName accepted a missing location")
	}
	if _, err := ParseLocationParent("projects/p"); err == nil {
		t.Fatalf("ParseLocationParent accepted a bare container")
	}
	if _, _, err := ParseViewName(testBucketName); err == nil {
		t.Fatalf("ParseViewName accepted a bucket name")
	}
	if _, err := ParseSettingsName("projects/p/locations/global/settings"); err == nil {
		t.Fatalf("ParseSettingsName accepted a settings-suffixed name")
	}
}
