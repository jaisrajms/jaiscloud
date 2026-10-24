// kms_secrets_depth_test.go closes DC4 (plan_docs/gcp-depth-closure-wave-plan.md
// W2.2): the behavioural-depth cells Cloud KMS and Secret Manager left exempt —
// p2 contention (concurrent crypto-key-version create/destroy/primary update;
// concurrent secret-version add/disable) and p4 paging over key rings,
// crypto-key versions, secrets and secret versions — asserted through the
// official apiary REST clients and the shared depth-test kit
// (tests/integration/gcp/testdepth, DC2).
//
// p4 uses the kit's plain cursor walk (the filtered/ordered forms are asserted
// separately below and in the DC14/DC23 cases). The list order is compared as a
// set: neither Discovery document specifies a default order for these methods.
//
// p2 asserts the documented state under contention, not a one-winner race the
// API does not define. A KMS crypto-key-version id is allocated atomically
// (concurrent creates get distinct numbers, no lost update) and its destruction
// is idempotent (every concurrent caller settles it on DESTROY_SCHEDULED, a
// second destroy returns the version unchanged); updating the primary version
// is atomic (the key settles on one of the requested versions, never a torn
// value). A Secret Manager version number is likewise allocated atomically, and
// disable is idempotent. The etag preconditions (secrets.delete and the version
// disable/enable/destroy requests) are asserted separately in
// secretmanager_etag_test.go (DC13). The kit's negative-pageSize negative is not
// asserted either (DC12: the emulator coerces a negative pageSize to its
// default).
//
// The concurrency cases run under -race (the module's integration step adds it);
// asserting a contention property in a binary without the race detector is a
// failure, not a silent downgrade (testdepth.AssertRaceEnabled).
package sdkrest_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	cloudkms "google.golang.org/api/cloudkms/v1"
	secretmanager "google.golang.org/api/secretmanager/v1"
	testdepth "jaiscloud-gcp-testdepth"
)

// kmsParent is the key-ring collection every KMS case operates in.
const kmsParent = "projects/proj/locations/global"

// createKeyRing creates a key ring and returns its full resource name.
func createKeyRing(t *testing.T, svc *cloudkms.Service) string {
	t.Helper()
	kr, err := svc.Projects.Locations.KeyRings.Create(kmsParent, &cloudkms.KeyRing{}).KeyRingId(unique("kr")).Do()
	require.NoError(t, err)
	return kr.Name
}

// createCryptoKey creates an ENCRYPT_DECRYPT crypto key (the store also creates
// version 1) and returns its full resource name.
func createCryptoKey(t *testing.T, svc *cloudkms.Service, keyRing string) string {
	t.Helper()
	key, err := svc.Projects.Locations.KeyRings.CryptoKeys.Create(keyRing, &cloudkms.CryptoKey{
		Purpose: "ENCRYPT_DECRYPT",
	}).CryptoKeyId(unique("k")).Do()
	require.NoError(t, err)
	return key.Name
}

// createSecret creates a secret with automatic replication and returns its full
// resource name.
func createSecret(t *testing.T, svc *secretmanager.Service) string {
	t.Helper()
	s, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("depth")).Do()
	require.NoError(t, err)
	return s.Name
}

// walkKeyRingNames walks keyRings.list through the kit and returns the full ring
// names in walk order.
func walkKeyRingNames(t *testing.T, svc *cloudkms.Service, cfg testdepth.Paging[*cloudkms.KeyRing]) []string {
	t.Helper()
	rings := testdepth.AssertPaging(t, func() testdepth.Lister[*cloudkms.KeyRing] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudkms.KeyRing], error) {
			call := svc.Projects.Locations.KeyRings.List(kmsParent)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			if req.Filter != "" {
				call = call.Filter(req.Filter)
			}
			if req.OrderBy != "" {
				call = call.OrderBy(req.OrderBy)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudkms.KeyRing]{}, err
			}
			return testdepth.Page[*cloudkms.KeyRing]{Items: resp.KeyRings, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(rings, cfg)
}

// walkVersionNames walks cryptoKeyVersions.list through the kit and returns the
// full version names in walk order.
func walkVersionNames(t *testing.T, svc *cloudkms.Service, key string, cfg testdepth.Paging[*cloudkms.CryptoKeyVersion]) []string {
	t.Helper()
	versions := testdepth.AssertPaging(t, func() testdepth.Lister[*cloudkms.CryptoKeyVersion] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudkms.CryptoKeyVersion], error) {
			call := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.List(key)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			if req.Filter != "" {
				call = call.Filter(req.Filter)
			}
			if req.OrderBy != "" {
				call = call.OrderBy(req.OrderBy)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudkms.CryptoKeyVersion]{}, err
			}
			return testdepth.Page[*cloudkms.CryptoKeyVersion]{Items: resp.CryptoKeyVersions, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(versions, cfg)
}

// walkSecretNames walks secrets.list through the kit and returns the full secret
// names in walk order.
func walkSecretNames(t *testing.T, svc *secretmanager.Service, cfg testdepth.Paging[*secretmanager.Secret]) []string {
	t.Helper()
	secrets := testdepth.AssertPaging(t, func() testdepth.Lister[*secretmanager.Secret] {
		return func(req testdepth.ListRequest) (testdepth.Page[*secretmanager.Secret], error) {
			call := svc.Projects.Secrets.List("projects/proj")
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			if req.Filter != "" {
				call = call.Filter(req.Filter)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*secretmanager.Secret]{}, err
			}
			return testdepth.Page[*secretmanager.Secret]{Items: resp.Secrets, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(secrets, cfg)
}

// walkSecretVersionNames walks versions.list through the kit and returns the
// full version names in walk order.
func walkSecretVersionNames(t *testing.T, svc *secretmanager.Service, secret string, cfg testdepth.Paging[*secretmanager.SecretVersion]) []string {
	t.Helper()
	versions := testdepth.AssertPaging(t, func() testdepth.Lister[*secretmanager.SecretVersion] {
		return func(req testdepth.ListRequest) (testdepth.Page[*secretmanager.SecretVersion], error) {
			call := svc.Projects.Secrets.Versions.List(secret)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			if req.Filter != "" {
				call = call.Filter(req.Filter)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*secretmanager.SecretVersion]{}, err
			}
			return testdepth.Page[*secretmanager.SecretVersion]{Items: resp.Versions, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(versions, cfg)
}

// distinctNames reports the number of distinct non-empty names in names.
func distinctNames(names []string) int {
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if n != "" {
			seen[n] = struct{}{}
		}
	}
	return len(seen)
}

// versionID returns the trailing segment of a full CryptoKeyVersion resource
// name — the id UpdateCryptoKeyPrimaryVersionRequest expects.
func versionID(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// TestSDKKMSDepthListing is kms p4: key rings and a key's crypto-key versions both
// page through pageSize/pageToken without dropping, duplicating or reordering a
// resource across a page boundary. The walk is compared as a set — the Discovery
// documents pin neither method's default order — and `RequireNonEmptyPages` holds
// because the server fills every non-terminal page.
func TestSDKKMSDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	// keyRings.list: five rings walked at page size 2.
	const rings = 5
	ringNames := make([]string, 0, rings)
	for i := 0; i < rings; i++ {
		ringNames = append(ringNames, createKeyRing(t, svc))
	}
	gotRings := walkKeyRingNames(t, svc, testdepth.Paging[*cloudkms.KeyRing]{
		Name:                 func(kr *cloudkms.KeyRing) string { return kr.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               ringNames,
	})
	require.Subset(t, gotRings, ringNames, "the paged key-ring walk must return every created ring")

	// cryptoKeyVersions.list: the key's auto-created version 1 plus four more,
	// walked at page size 2.
	keyName := createCryptoKey(t, svc, ringNames[0])
	versionNames := []string{keyName + "/cryptoKeyVersions/1"}
	for i := 0; i < 4; i++ {
		v, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Create(keyName, &cloudkms.CryptoKeyVersion{}).Do()
		require.NoError(t, err)
		versionNames = append(versionNames, v.Name)
	}
	gotVersions := walkVersionNames(t, svc, keyName, testdepth.Paging[*cloudkms.CryptoKeyVersion]{
		Name:                 func(v *cloudkms.CryptoKeyVersion) string { return v.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               versionNames,
	})
	require.Subset(t, gotVersions, versionNames, "the paged version walk must return every version")
}

// TestSDKKMSListFilterOrderBy closes the DC14 remainder of the kms p4 cell: the
// documented `filter` and `orderBy` on keyRings.list and cryptoKeyVersions.list
// must reach the cursor walk rather than being silently ignored. The filter is
// asserted by only ever walking rings carrying a unique marker substring (the
// kit's Include rejects anything outside it), and the composed filter+orderBy
// walk pins filter, order and the page cursor together (the kit's
// OrderAscendingName checks the order across a page boundary).
func TestSDKKMSListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	// Five marker rings walked through a name-substring filter at page size 2.
	// The marker is unique to this run, so a walk over a shared emulator that
	// already holds other runs' rings still returns exactly this run's five.
	const rings = 5
	marker := unique("depth-fo")
	want := make([]string, 0, rings)
	for i := 0; i < rings; i++ {
		kr, err := svc.Projects.Locations.KeyRings.Create(kmsParent, &cloudkms.KeyRing{}).KeyRingId(fmt.Sprintf("%s-%d", marker, i)).Do()
		require.NoError(t, err)
		want = append(want, kr.Name)
	}
	// A decoy ring the filter must exclude.
	createKeyRing(t, svc)

	// filter + orderBy + paging composed: exactly the marker rings, ascending,
	// across the page boundary.
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)
	got := walkKeyRingNames(t, svc, testdepth.Paging[*cloudkms.KeyRing]{
		Name:                 func(kr *cloudkms.KeyRing) string { return kr.Name },
		PageSize:             2,
		Filter:               `name:"` + marker + `"`,
		OrderBy:              "name",
		Order:                testdepth.OrderAscendingName,
		RequireNonEmptyPages: true,
		Expect:               want,
		Include:              func(kr *cloudkms.KeyRing) bool { return strings.Contains(kr.Name, marker) },
	})
	require.Equal(t, sorted, got, "the filtered, name-ordered key-ring walk returns exactly the marker rings in order")

	// orderBy name desc: every ring in the location, descending, across pages.
	all := walkKeyRingNames(t, svc, testdepth.Paging[*cloudkms.KeyRing]{
		Name:                 func(kr *cloudkms.KeyRing) string { return kr.Name },
		PageSize:             2,
		OrderBy:              "name desc",
		RequireNonEmptyPages: true,
	})
	require.GreaterOrEqual(t, len(all), rings)
	require.Truef(t, sort.SliceIsSorted(all, func(i, j int) bool { return all[i] > all[j] }),
		"orderBy name desc must return the key rings in descending name order: %v", all)

	// cryptoKeyVersions.list: the name orderBy is numeric by version number,
	// descending, across pages.
	keyName := createCryptoKey(t, svc, want[0])
	for i := 0; i < 4; i++ { // version 1 already exists
		_, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Create(keyName, &cloudkms.CryptoKeyVersion{}).Do()
		require.NoError(t, err)
	}
	versionsDesc := walkVersionNames(t, svc, keyName, testdepth.Paging[*cloudkms.CryptoKeyVersion]{
		Name:     func(v *cloudkms.CryptoKeyVersion) string { return v.Name },
		PageSize: 2,
		OrderBy:  "name desc",
	})
	wantVersions := []string{
		keyName + "/cryptoKeyVersions/5",
		keyName + "/cryptoKeyVersions/4",
		keyName + "/cryptoKeyVersions/3",
		keyName + "/cryptoKeyVersions/2",
		keyName + "/cryptoKeyVersions/1",
	}
	require.Equal(t, wantVersions, versionsDesc, "orderBy name desc must return the versions in numeric descending order")
}

// TestSDKSecretManagerListFilter closes the DC14 remainder of the secretmanager
// p4 cell: secrets.list must apply the documented `filter`, not return the whole
// collection.
func TestSDKSecretManagerListFilter(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	marker := unique("depth-sm")
	want := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		s, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
			Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
		}).SecretId(fmt.Sprintf("%s-%d", marker, i)).Do()
		require.NoError(t, err)
		want = append(want, s.Name)
	}
	createSecret(t, svc) // decoy

	got := walkSecretNames(t, svc, testdepth.Paging[*secretmanager.Secret]{
		Name:                 func(s *secretmanager.Secret) string { return s.Name },
		PageSize:             2,
		Filter:               `name:"` + marker + `"`,
		RequireNonEmptyPages: true,
		Expect:               want,
		Include:              func(s *secretmanager.Secret) bool { return strings.Contains(s.Name, marker) },
	})
	require.ElementsMatch(t, want, got, "the filtered secret walk returns exactly the marker secrets")
}

// TestSDKKMSDepthConcurrency is kms p2: concurrent mutations of a crypto key
// under -race. The version-number allocation is atomic (distinct numbers, no lost
// update), a destroy is idempotent (all callers settle one version on
// DESTROY_SCHEDULED), and updating the primary version is atomic (the key settles
// on one of the requested versions).
func TestSDKKMSDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertRaceEnabled(t)

	versions := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions

	t.Run("ConcurrentVersionCreateAllocatesDistinctVersions", func(t *testing.T) {
		keyName := createCryptoKey(t, svc, createKeyRing(t, svc))

		const creators = 8
		created := make([]string, creators)
		errs := testdepth.Race(creators, func(i int) error {
			v, err := versions.Create(keyName, &cloudkms.CryptoKeyVersion{}).Do()
			if err != nil {
				return err
			}
			created[i] = v.Name
			return nil
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}
		require.Equal(t, creators, distinctNames(created),
			"concurrent version creates must each allocate a distinct version number")

		// Every allocated version is addressable: the walk returns created
		// version 1 (auto-created with the key) plus the %d concurrent ones.
		got := walkVersionNames(t, svc, keyName, testdepth.Paging[*cloudkms.CryptoKeyVersion]{
			Name:     func(v *cloudkms.CryptoKeyVersion) string { return v.Name },
			PageSize: 3,
			Order:    testdepth.OrderUnspecified,
			Expect:   created,
		})
		require.Subset(t, got, created, "every concurrently created version must be listed")
	})

	t.Run("ConcurrentDestroyIsIdempotent", func(t *testing.T) {
		keyName := createCryptoKey(t, svc, createKeyRing(t, svc))
		v, err := versions.Create(keyName, &cloudkms.CryptoKeyVersion{}).Do()
		require.NoError(t, err)

		errs := testdepth.Race(8, func(int) error {
			_, err := versions.Destroy(v.Name, &cloudkms.DestroyCryptoKeyVersionRequest{}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "destroy %d", i)
		}

		got, err := versions.Get(v.Name).Do()
		require.NoError(t, err)
		require.Equal(t, "DESTROY_SCHEDULED", got.State,
			"concurrent destroys settle the version on DESTROY_SCHEDULED")
	})

	t.Run("ConcurrentPrimaryVersionUpdateSettlesOnOne", func(t *testing.T) {
		keyName := createCryptoKey(t, svc, createKeyRing(t, svc))
		// The auto-created version 1 plus a second version to alternate between.
		second, err := versions.Create(keyName, &cloudkms.CryptoKeyVersion{}).Do()
		require.NoError(t, err)
		candidates := []string{"1", versionID(second.Name)}

		const writers = 8
		errs := testdepth.Race(writers, func(i int) error {
			target := candidates[i%len(candidates)]
			_, err := svc.Projects.Locations.KeyRings.CryptoKeys.
				UpdatePrimaryVersion(keyName, &cloudkms.UpdateCryptoKeyPrimaryVersionRequest{CryptoKeyVersionId: target}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "update primary %d", i)
		}

		got, err := svc.Projects.Locations.KeyRings.CryptoKeys.Get(keyName).Do()
		require.NoError(t, err)
		require.NotNil(t, got.Primary)
		require.Contains(t, candidates, versionID(got.Primary.Name),
			"the key's primary must be one of the versions the writers actually pointed at")
	})
}

// TestSDKSecretManagerDepthListing is secretmanager p4: secrets and a secret's
// versions both page through pageSize/pageToken without dropping or duplicating a
// resource across a page boundary. The walk is compared as a set (neither method
// documents a default order).
func TestSDKSecretManagerDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	// secrets.list: five secrets walked at page size 2.
	const secrets = 5
	secretNames := make([]string, 0, secrets)
	for i := 0; i < secrets; i++ {
		secretNames = append(secretNames, createSecret(t, svc))
	}
	gotSecrets := walkSecretNames(t, svc, testdepth.Paging[*secretmanager.Secret]{
		Name:                 func(s *secretmanager.Secret) string { return s.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               secretNames,
	})
	require.Subset(t, gotSecrets, secretNames, "the paged secret walk must return every created secret")

	// versions.list: five versions of one secret, walked at page size 2.
	target := secretNames[0]
	versionNames := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		v, err := svc.Projects.Secrets.AddVersion(target, &secretmanager.AddSecretVersionRequest{
			Payload: &secretmanager.SecretPayload{Data: "aGk="},
		}).Do()
		require.NoError(t, err)
		versionNames = append(versionNames, v.Name)
	}
	gotVersions := walkSecretVersionNames(t, svc, target, testdepth.Paging[*secretmanager.SecretVersion]{
		Name:                 func(v *secretmanager.SecretVersion) string { return v.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               versionNames,
	})
	require.Subset(t, gotVersions, versionNames, "the paged version walk must return every version")
}

// TestSDKSecretManagerDepthConcurrency is secretmanager p2: concurrent version
// mutations under -race. Adding a version allocates a fresh, distinct version
// number (no lost update), and disabling a version is idempotent (every caller
// succeeds and the version settles DISABLED).
func TestSDKSecretManagerDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertRaceEnabled(t)

	t.Run("ConcurrentAddVersionAllocatesDistinctVersions", func(t *testing.T) {
		secret := createSecret(t, svc)

		const adders = 8
		added := make([]string, adders)
		errs := testdepth.Race(adders, func(i int) error {
			v, err := svc.Projects.Secrets.AddVersion(secret, &secretmanager.AddSecretVersionRequest{
				Payload: &secretmanager.SecretPayload{Data: "aGk="},
			}).Do()
			if err != nil {
				return err
			}
			added[i] = v.Name
			return nil
		})
		for i, err := range errs {
			require.NoError(t, err, "add %d", i)
		}
		require.Equal(t, adders, distinctNames(added),
			"concurrent version adds must each allocate a distinct version number")

		got := walkSecretVersionNames(t, svc, secret, testdepth.Paging[*secretmanager.SecretVersion]{
			Name:     func(v *secretmanager.SecretVersion) string { return v.Name },
			PageSize: 3,
			Order:    testdepth.OrderUnspecified,
			Expect:   added,
		})
		require.Subset(t, got, added, "every concurrently added version must be listed")
	})

	t.Run("ConcurrentDisableIsIdempotent", func(t *testing.T) {
		secret := createSecret(t, svc)
		v, err := svc.Projects.Secrets.AddVersion(secret, &secretmanager.AddSecretVersionRequest{
			Payload: &secretmanager.SecretPayload{Data: "aGk="},
		}).Do()
		require.NoError(t, err)

		errs := testdepth.Race(8, func(int) error {
			_, err := svc.Projects.Secrets.Versions.Disable(v.Name, &secretmanager.DisableSecretVersionRequest{}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "disable %d", i)
		}

		got, err := svc.Projects.Secrets.Versions.Get(v.Name).Do()
		require.NoError(t, err)
		require.Equal(t, "DISABLED", got.State, "concurrent disables settle the version on DISABLED")
	})
}

// walkImportJobNames walks importJobs.list through the kit and returns the full
// job names in walk order.
func walkImportJobNames(t *testing.T, svc *cloudkms.Service, keyRing string, cfg testdepth.Paging[*cloudkms.ImportJob]) []string {
	t.Helper()
	jobs := testdepth.AssertPaging(t, func() testdepth.Lister[*cloudkms.ImportJob] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudkms.ImportJob], error) {
			call := svc.Projects.Locations.KeyRings.ImportJobs.List(keyRing)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			if req.Filter != "" {
				call = call.Filter(req.Filter)
			}
			if req.OrderBy != "" {
				call = call.OrderBy(req.OrderBy)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudkms.ImportJob]{}, err
			}
			return testdepth.Page[*cloudkms.ImportJob]{Items: resp.ImportJobs, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(jobs, cfg)
}

// TestSDKKMSImportJobsListFilterOrderBy closes DC23's client-side half:
// importJobs.list documents filter/orderBy/pageSize/pageToken (KMS Discovery and
// the "Sorting and filtering list results" page), so the official apiary client
// must see all four honoured, and the filter and name order must compose with
// the cursor instead of returning the unfiltered key ring in one unpaged slice.
func TestSDKKMSImportJobsListFilterOrderBy(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	ring := createKeyRing(t, svc)
	marker := unique("depth-ij")
	want := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		job, err := svc.Projects.Locations.KeyRings.ImportJobs.Create(ring, &cloudkms.ImportJob{
			ImportMethod:    "RSA_OAEP_3072_SHA256",
			ProtectionLevel: "SOFTWARE",
		}).ImportJobId(fmt.Sprintf("%s-%d", marker, i)).Do()
		require.NoError(t, err)
		want = append(want, job.Name)
	}
	// A decoy job the filter must exclude.
	_, err = svc.Projects.Locations.KeyRings.ImportJobs.Create(ring, &cloudkms.ImportJob{
		ImportMethod: "RSA_OAEP_3072_SHA256",
	}).ImportJobId(unique("decoy-ij")).Do()
	require.NoError(t, err)

	// filter + orderBy + paging composed: exactly the marker jobs, ascending,
	// across the page boundary.
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)
	got := walkImportJobNames(t, svc, ring, testdepth.Paging[*cloudkms.ImportJob]{
		Name:                 func(ij *cloudkms.ImportJob) string { return ij.Name },
		PageSize:             2,
		Filter:               `name:"` + marker + `"`,
		OrderBy:              "name",
		Order:                testdepth.OrderAscendingName,
		RequireNonEmptyPages: true,
		Expect:               want,
		Include:              func(ij *cloudkms.ImportJob) bool { return strings.Contains(ij.Name, marker) },
	})
	require.Equal(t, sorted, got, "the filtered, name-ordered import-job walk returns exactly the marker jobs in order")

	// orderBy name desc over the whole key ring, across pages.
	all := walkImportJobNames(t, svc, ring, testdepth.Paging[*cloudkms.ImportJob]{
		Name:     func(ij *cloudkms.ImportJob) string { return ij.Name },
		PageSize: 2,
		OrderBy:  "name desc",
	})
	require.Len(t, all, len(want)+1, "the unfiltered walk returns every job in the key ring")
	require.Truef(t, sort.SliceIsSorted(all, func(i, j int) bool { return all[i] > all[j] }),
		"orderBy name desc must return the import jobs in descending name order: %v", all)

	// A protectionLevel filter distinguishes the HSM job from the SOFTWARE ones.
	_, err = svc.Projects.Locations.KeyRings.ImportJobs.Create(ring, &cloudkms.ImportJob{
		ImportMethod:    "RSA_OAEP_3072_SHA256",
		ProtectionLevel: "HSM",
	}).ImportJobId(unique("hsm-ij")).Do()
	require.NoError(t, err)
	resp, err := svc.Projects.Locations.KeyRings.ImportJobs.List(ring).Filter("protectionLevel=HSM").Do()
	require.NoError(t, err)
	require.Len(t, resp.ImportJobs, 1, "protectionLevel=HSM must match only the HSM job")
	require.Equal(t, "HSM", resp.ImportJobs[0].ProtectionLevel)

	// An unsupported filter is a 400, not a silently ignored parameter.
	_, err = svc.Projects.Locations.KeyRings.ImportJobs.List(ring).Filter("bogus=1").Do()
	require.Error(t, err, "an unsupported import-job filter must be INVALID_ARGUMENT")
}

// TestSDKKMSNonIntegerPageSize pins the int32 query binding on the KMS REST
// lists over raw HTTP (the apiary client's PageSize is an int64 and cannot emit
// a non-integer): a malformed or fractional pageSize is 400 INVALID_ARGUMENT,
// not a silent default page.
func TestSDKKMSNonIntegerPageSize(t *testing.T) {
	base := strings.TrimRight(endpoint(), "/")
	for _, path := range []string{
		"/v1/projects/proj/locations/global/keyRings",
		"/v1/projects/proj/locations/global/keyRings/kr/cryptoKeys",
		"/v1/projects/proj/locations/global/keyRings/kr/importJobs",
	} {
		for _, v := range []string{"abc", "-0.5", "1.5"} {
			resp, err := http.Get(base + path + "?pageSize=" + v)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s?pageSize=%s body=%s", path, v, body)
			require.Contains(t, string(body), "INVALID_ARGUMENT")
		}
	}
}
