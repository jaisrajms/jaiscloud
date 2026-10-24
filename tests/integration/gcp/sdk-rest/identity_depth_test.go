// identity_depth_test.go closes DC3 (plan_docs/gcp-depth-closure-wave-plan.md
// W2.1): the behavioural-depth cells the identity/resource control planes left
// exempt — p4 paging over IAM service accounts, Resource Manager projects and
// Service Usage enabled services, plus Resource Manager p2 (concurrent
// create/delete) and p5 (many-projects boundary) — asserted through the official
// apiary REST clients and the shared depth-test kit
// (tests/integration/gcp/testdepth, DC2).
//
// The kit drives one list contract for all three services: pageSize/pageToken
// walks composed with the service's filter, every resource exactly once, a
// terminal empty token and an identical second walk. IAM
// projects.serviceAccounts.keys.list is the exception the Discovery document
// pins down: it carries no pageToken (and its response no nextPageToken), so it
// is an unpaged list and is asserted as such rather than faked into a paging
// contract.
//
// The list order is asserted as a set for all three methods: none of their
// Discovery documents specifies an order (projects.list says so explicitly,
// "returns Projects in an unspecified order"), so a sequence assertion would pin
// the emulator's own sort as a contract real GCP does not promise.
//
// The kit's documented negative (a negative pageSize → INVALID_ARGUMENT,
// AssertInvalidPageSize) is asserted here as TestSDKNegativePageSizeRejected —
// the shared paging contract now rejects it (DC12) instead of coercing it.
//
// The concurrency cases run under -race (the module's gate adds it); the create
// arbiter and the accept-and-succeed upsert are the store's, not the client's.
package sdkrest_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iam/v1"
	serviceusage "google.golang.org/api/serviceusage/v1"
	"google.golang.org/grpc/codes"
	testdepth "jaiscloud-gcp-testdepth"
)

// conflictStatus reports whether a losing writer observed the documented
// conflict (HTTP 409 over REST, ALREADY_EXISTS over gRPC). It is the predicate
// the contention contract uses for a create-arbitered resource.
func conflictStatus(err error) bool {
	obs, ok := testdepth.StatusOf(err)
	if !ok {
		return false
	}
	switch obs.Kind {
	case testdepth.KindHTTP:
		return obs.HTTPStatus == 409
	case testdepth.KindGRPC:
		return obs.Code == codes.AlreadyExists
	default:
		return false
	}
}

// walkServiceAccountNames walks projects.serviceAccounts.list through the kit
// and returns the full account names in walk order.
func walkServiceAccountNames(t *testing.T, svc *iam.Service, cfg testdepth.Paging[*iam.ServiceAccount]) []string {
	t.Helper()
	accounts := testdepth.AssertPaging(t, func() testdepth.Lister[*iam.ServiceAccount] {
		return func(req testdepth.ListRequest) (testdepth.Page[*iam.ServiceAccount], error) {
			call := svc.Projects.ServiceAccounts.List("projects/proj")
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*iam.ServiceAccount]{}, err
			}
			return testdepth.Page[*iam.ServiceAccount]{Items: resp.Accounts, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(accounts, cfg)
}

// walkProjectIDs walks projects.list through the kit and returns the project ids
// in walk order.
func walkProjectIDs(t *testing.T, svc *cloudresourcemanager.Service, cfg testdepth.Paging[*cloudresourcemanager.Project]) []string {
	t.Helper()
	projects := testdepth.AssertPaging(t, func() testdepth.Lister[*cloudresourcemanager.Project] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudresourcemanager.Project], error) {
			call := svc.Projects.List()
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
				return testdepth.Page[*cloudresourcemanager.Project]{}, err
			}
			return testdepth.Page[*cloudresourcemanager.Project]{Items: resp.Projects, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(projects, cfg)
}

// walkServiceNames walks services.list through the kit and returns the full
// service names in walk order.
func walkServiceNames(t *testing.T, svc *serviceusage.Service, cfg testdepth.Paging[*serviceusage.GoogleApiServiceusageV1Service]) []string {
	t.Helper()
	services := testdepth.AssertPaging(t, func() testdepth.Lister[*serviceusage.GoogleApiServiceusageV1Service] {
		return func(req testdepth.ListRequest) (testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service], error) {
			call := svc.Services.List("projects/proj")
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
				return testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service]{}, err
			}
			return testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service]{Items: resp.Services, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(services, cfg)
}

// TestSDKIAMDepthListing is iam p4: the service-account list pages through
// pageSize/pageToken without dropping, duplicating or reordering an account
// across a page boundary. projects.serviceAccounts.list documents no filter and
// no order, so the contract is the cursor walk alone (compared as a set).
//
// The keys surface is the second half of the iam record: real GCP's
// projects.serviceAccounts.keys.list has no pageSize/pageToken parameter (and
// its response no nextPageToken), so the emulator must return the account's full
// key set in one response — asserted here rather than forced into a page walk.
func TestSDKIAMDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	const accounts = 5
	want := make([]string, 0, accounts)
	for i := 0; i < accounts; i++ {
		sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
			AccountId:      unique(fmt.Sprintf("sa%d", i)),
			ServiceAccount: &iam.ServiceAccount{DisplayName: "Depth SA"},
		}).Do()
		require.NoError(t, err)
		want = append(want, sa.Name)
	}

	got := walkServiceAccountNames(t, svc, testdepth.Paging[*iam.ServiceAccount]{
		Name:                 func(sa *iam.ServiceAccount) string { return sa.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               want,
	})
	require.Subset(t, got, want, "the paged walk must return every created account")

	// keys.list is unpaged by Discovery: one response carries every key, so two
	// keys prove the response is complete rather than truncated at one.
	keyNames := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		key, err := svc.Projects.ServiceAccounts.Keys.Create(want[0], &iam.CreateServiceAccountKeyRequest{}).Do()
		require.NoError(t, err)
		require.NotEmpty(t, key.Name)
		keyNames = append(keyNames, key.Name)
	}

	keys, err := svc.Projects.ServiceAccounts.Keys.List(want[0]).Do()
	require.NoError(t, err)
	require.Len(t, keys.Keys, 2, "keys.list returns the account's whole key set in one response")
	var listed []string
	for _, k := range keys.Keys {
		listed = append(listed, k.Name)
	}
	require.ElementsMatch(t, keyNames, listed)
}

// TestSDKResourceManagerDepthListing is resourcemanager p4: projects.list pages
// through pageSize/pageToken composed with a labels filter, so the cursor walks
// the filtered set (never the unfiltered one).
//
// The order assertion is a set comparison: the Discovery document states that
// projects.list "returns Projects in an unspecified order", so a sequence
// assertion would contradict the documented contract. The document also calls
// the method eventually consistent; the emulator is strongly consistent (one
// store, no replication lag), so a just-created project is expected to appear
// immediately here.
func TestSDKResourceManagerDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	const projects = 5
	want := make([]string, 0, projects)
	for i := 0; i < projects; i++ {
		id := unique(fmt.Sprintf("page%d", i))
		_, err := svc.Projects.Create(&cloudresourcemanager.Project{
			ProjectId: id,
			Name:      "Depth Page",
			Labels:    map[string]string{"suite": "depth"},
		}).Do()
		require.NoError(t, err)
		want = append(want, id)
	}

	got := walkProjectIDs(t, svc, testdepth.Paging[*cloudresourcemanager.Project]{
		Name:                 func(p *cloudresourcemanager.Project) string { return p.ProjectId },
		PageSize:             2,
		Filter:               "labels.suite:depth",
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               want,
		Include:              func(p *cloudresourcemanager.Project) bool { return p.Labels["suite"] == "depth" },
	})
	require.Subset(t, got, want, "the filtered, paged walk must return every labelled project")
}

// TestSDKResourceManagerDepthConcurrency is resourcemanager p2: concurrent
// mutations of the same project under -race. The project create is arbitrated by
// the store's atomic upsert (exactly one create wins, every loser is
// AlreadyExists), while a delete is idempotent (concurrent deletes of one
// project all succeed and the project settles DELETE_REQUESTED).
func TestSDKResourceManagerDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertRaceEnabled(t)

	t.Run("SameIdCreateRace", func(t *testing.T) {
		id := unique("proj")
		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: id, Name: "Race"}).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, conflictStatus)

		got, err := svc.Projects.Get(id).Do()
		require.NoError(t, err)
		require.Equal(t, "ACTIVE", got.LifecycleState)
		require.Equal(t, id, got.ProjectId)
	})

	t.Run("DistinctCreatesAllLand", func(t *testing.T) {
		const creators = 6
		ids := make([]string, creators)
		for i := range ids {
			ids[i] = unique(fmt.Sprintf("dist%d", i))
		}
		errs := testdepth.Race(creators, func(i int) error {
			_, err := svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: ids[i], Name: "Distinct"}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}
		for i, id := range ids {
			got, err := svc.Projects.Get(id).Do()
			require.NoError(t, err, "get %d", i)
			require.Equal(t, id, got.ProjectId)
		}
	})

	t.Run("ConcurrentDeleteIsIdempotent", func(t *testing.T) {
		id := unique("proj")
		_, err := svc.Projects.Create(&cloudresourcemanager.Project{ProjectId: id, Name: "DelRace"}).Do()
		require.NoError(t, err)

		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Delete(id).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "delete %d", i)
		}
		got, err := svc.Projects.Get(id).Do()
		require.NoError(t, err)
		require.Equal(t, "DELETE_REQUESTED", got.LifecycleState)
	})
}

// TestSDKResourceManagerDepthScale is resourcemanager p5: a many-projects
// boundary. Twenty-five labelled projects are created and then walked with a
// small page size, so the boundary is the cursor chain over a large collection,
// not a single response.
func TestSDKResourceManagerDepthScale(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	const projects = 25
	want := make([]string, 0, projects)
	for i := 0; i < projects; i++ {
		id := unique(fmt.Sprintf("scale%d", i))
		_, err := svc.Projects.Create(&cloudresourcemanager.Project{
			ProjectId: id,
			Name:      "Depth Scale",
			Labels:    map[string]string{"suite": "scale"},
		}).Do()
		require.NoError(t, err)
		want = append(want, id)
	}

	got := walkProjectIDs(t, svc, testdepth.Paging[*cloudresourcemanager.Project]{
		Name:                 func(p *cloudresourcemanager.Project) string { return p.ProjectId },
		PageSize:             4,
		Filter:               "labels.suite:scale",
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               want,
		Include:              func(p *cloudresourcemanager.Project) bool { return p.Labels["suite"] == "scale" },
	})
	require.Subset(t, got, want, "every one of the %d projects must survive the page walk", projects)
}

// TestSDKServiceUsageDepthListing is serviceusage p4: services.list pages through
// pageSize/pageToken composed with the state:ENABLED / state:DISABLED filter, so
// the cursor walks the filtered set and never leaks a service of the other
// state. The Discovery document specifies no order, so the walk is compared as a
// set.
func TestSDKServiceUsageDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj"
	const enabled = 5
	enabledNames := make([]string, 0, enabled)
	for i := 0; i < enabled; i++ {
		name := parent + "/services/" + serviceID(fmt.Sprintf("depth-on%d", i))
		_, err := svc.Services.Enable(name, &serviceusage.EnableServiceRequest{}).Do()
		require.NoError(t, err)
		enabledNames = append(enabledNames, name)
	}

	// One service tracked as DISABLED: enabled at least once, then disabled, so
	// the emulator enumerates it (a never-touched id is absent, not DISABLED).
	disabledName := parent + "/services/" + serviceID("depth-off")
	_, err = svc.Services.Enable(disabledName, &serviceusage.EnableServiceRequest{}).Do()
	require.NoError(t, err)
	_, err = svc.Services.Disable(disabledName, &serviceusage.DisableServiceRequest{}).Do()
	require.NoError(t, err)

	gotEnabled := walkServiceNames(t, svc, testdepth.Paging[*serviceusage.GoogleApiServiceusageV1Service]{
		Name:                 func(s *serviceusage.GoogleApiServiceusageV1Service) string { return s.Name },
		PageSize:             2,
		Filter:               "state:ENABLED",
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               enabledNames,
		Include:              func(s *serviceusage.GoogleApiServiceusageV1Service) bool { return s.State == "ENABLED" },
	})
	require.Subset(t, gotEnabled, enabledNames, "the state:ENABLED walk must return every enabled service")

	gotDisabled := walkServiceNames(t, svc, testdepth.Paging[*serviceusage.GoogleApiServiceusageV1Service]{
		Name:                 func(s *serviceusage.GoogleApiServiceusageV1Service) string { return s.Name },
		PageSize:             2,
		Filter:               "state:DISABLED",
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               []string{disabledName},
		Include:              func(s *serviceusage.GoogleApiServiceusageV1Service) bool { return s.State == "DISABLED" },
	})
	require.Contains(t, gotDisabled, disabledName)
}

// TestSDKServiceUsageDepthConcurrency is serviceusage p2: concurrent enables
// under -race. Enabling is the accept-and-succeed upsert the core documents, so
// the contention property is idempotency (one enforced resource, final state
// ENABLED) rather than a one-winner conflict, and concurrent enables of distinct
// services each land.
func TestSDKServiceUsageDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertRaceEnabled(t)

	const parent = "projects/proj"

	t.Run("SameServiceEnableIsIdempotent", func(t *testing.T) {
		name := parent + "/services/" + serviceID("depth-race")
		errs := testdepth.Race(8, func(int) error {
			_, err := svc.Services.Enable(name, &serviceusage.EnableServiceRequest{}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "enable %d", i)
		}

		got, err := svc.Services.Get(name).Do()
		require.NoError(t, err)
		require.Equal(t, "ENABLED", got.State)

		// Count across every page: the tracked set grows with the emulator's
		// lifetime, so one 200-item page is not guaranteed to contain it.
		token, seen := "", 0
		for {
			page, err := svc.Services.List(parent).Filter("state:ENABLED").PageSize(200).PageToken(token).Do()
			require.NoError(t, err)
			seen += countService(page.Services, name)
			if token = page.NextPageToken; token == "" {
				break
			}
		}
		assert.Equal(t, 1, seen, "concurrent enables must not duplicate the service")
	})

	t.Run("DistinctEnablesAllLand", func(t *testing.T) {
		const enablers = 8
		names := make([]string, enablers)
		for i := range names {
			names[i] = parent + "/services/" + serviceID(fmt.Sprintf("depth-dist%d", i))
		}
		errs := testdepth.Race(enablers, func(i int) error {
			_, err := svc.Services.Enable(names[i], &serviceusage.EnableServiceRequest{}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "enable %d", i)
		}
		for i, name := range names {
			got, err := svc.Services.Get(name).Do()
			require.NoError(t, err, "get %d", i)
			require.Equal(t, "ENABLED", got.State)
		}
	})
}

// countService counts the entries of the given full name in a service page.
func countService(services []*serviceusage.GoogleApiServiceusageV1Service, name string) int {
	n := 0
	for _, s := range services {
		if s.Name == name {
			n++
		}
	}
	return n
}

// TestSDKNegativePageSizeRejected closes DC12 end to end: a negative pageSize is
// INVALID_ARGUMENT over the official REST clients (AIP-158), not a silent
// fall-back to the server default. It exercises the three identity/control-plane
// list methods DC3 walks — the shared internal/gcp/paging contract they and every
// other cursor-paged list method rely on.
func TestSDKNegativePageSizeRejected(t *testing.T) {
	ctx := context.Background()

	iamSvc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertInvalidPageSize(t, func(req testdepth.ListRequest) (testdepth.Page[*iam.ServiceAccount], error) {
		resp, err := iamSvc.Projects.ServiceAccounts.List("projects/proj").PageSize(req.PageSize).Do()
		if err != nil {
			return testdepth.Page[*iam.ServiceAccount]{}, err
		}
		return testdepth.Page[*iam.ServiceAccount]{Items: resp.Accounts, NextPageToken: resp.NextPageToken}, nil
	}, -1, testdepth.SpecInvalidArgument())

	rmSvc, err := cloudresourcemanager.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertInvalidPageSize(t, func(req testdepth.ListRequest) (testdepth.Page[*cloudresourcemanager.Project], error) {
		resp, err := rmSvc.Projects.List().PageSize(req.PageSize).Do()
		if err != nil {
			return testdepth.Page[*cloudresourcemanager.Project]{}, err
		}
		return testdepth.Page[*cloudresourcemanager.Project]{Items: resp.Projects, NextPageToken: resp.NextPageToken}, nil
	}, -1, testdepth.SpecInvalidArgument())

	suSvc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertInvalidPageSize(t, func(req testdepth.ListRequest) (testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service], error) {
		resp, err := suSvc.Services.List("projects/proj").PageSize(req.PageSize).Do()
		if err != nil {
			return testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service]{}, err
		}
		return testdepth.Page[*serviceusage.GoogleApiServiceusageV1Service]{Items: resp.Services, NextPageToken: resp.NextPageToken}, nil
	}, -1, testdepth.SpecInvalidArgument())
}
