package dataproc

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const wtIAMName = "projects/proj/regions/us-central1/workflowTemplates/t1"

// seedTemplate creates the template every IAM test targets.
func seedTemplate(t *testing.T, p *Service) {
	t.Helper()
	if _, err := p.CreateWorkflowTemplate(context.Background(), "proj", "us-central1",
		WorkflowTemplateInput{ID: "t1", Definition: validTemplateDef("t1")}); err != nil {
		t.Fatalf("CreateWorkflowTemplate: %v", err)
	}
}

func TestParseIAMResource(t *testing.T) {
	got, ok := ParseIAMResource(wtIAMName)
	if !ok {
		t.Fatalf("ParseIAMResource(%q) not recognized", wtIAMName)
	}
	want := IAMResource{Project: "proj", Region: "us-central1", Kind: IAMWorkflowTemplate, ID: "t1"}
	if got != want {
		t.Fatalf("parsed = %+v, want %+v", got, want)
	}
	if want.Key() != "us-central1/t1" {
		t.Fatalf("key = %q, want us-central1/t1", want.Key())
	}

	bad := []string{
		"",
		"projects/proj/regions/us-central1/autoscalingPolicies/p1",
		"projects/proj/regions/us-central1/operations/o1",
		"projects/proj/locations/us-central1/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates",
		"projects//regions/us-central1/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates/t1/extra",
		"projects/proj/regions/us-central1/workflowTemplates/t1:getIamPolicy",
	}
	for _, name := range bad {
		if _, ok := ParseIAMResource(name); ok {
			t.Errorf("ParseIAMResource(%q) = ok, want rejected", name)
		}
	}
}

// TestParseIAMResourceCollections covers the other two collections the mixin is
// published on. Each keeps a distinct policy type, so a cluster and a job that
// share an id do not share a policy row.
func TestParseIAMResourceCollections(t *testing.T) {
	cases := []struct {
		name string
		want IAMResource
	}{
		{"projects/proj/regions/us-central1/clusters/c1",
			IAMResource{Project: "proj", Region: "us-central1", Kind: IAMCluster, ID: "c1"}},
		{"projects/proj/regions/us-central1/jobs/9e0d4c31",
			IAMResource{Project: "proj", Region: "us-central1", Kind: IAMJob, ID: "9e0d4c31"}},
	}
	for _, tc := range cases {
		got, ok := ParseIAMResource(tc.name)
		if !ok || got != tc.want {
			t.Fatalf("ParseIAMResource(%q) = %+v/%v, want %+v", tc.name, got, ok, tc.want)
		}
	}
	if ClusterPolicyType == JobPolicyType || JobPolicyType == WorkflowTemplatePolicyType || ClusterPolicyType == WorkflowTemplatePolicyType {
		t.Fatal("policy types must be distinct per collection")
	}
	if (IAMResource{Kind: IAMCluster}).PolicyType() != ClusterPolicyType ||
		(IAMResource{Kind: IAMJob}).PolicyType() != JobPolicyType ||
		(IAMResource{Kind: IAMWorkflowTemplate}).PolicyType() != WorkflowTemplatePolicyType {
		t.Fatal("PolicyType does not follow the collection")
	}
}

// TestWorkflowTemplateIAMDefaultEmpty: get on an existing template returns the
// empty default policy (version 1, default etag, empty bindings).
func TestWorkflowTemplateIAMDefaultEmpty(t *testing.T) {
	p := newProvider(t)
	seedTemplate(t, p)
	pol, err := p.GetIamPolicy(context.Background(), wtIAMName)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if pol.Version != 1 || pol.Etag == "" || len(pol.Bindings) != 0 {
		t.Fatalf("default policy = %+v, want version 1 / default etag / no bindings", pol)
	}
}

// TestWorkflowTemplateIAMConditionRoundTrip: a conditional binding is stored at
// version 3 and reads back with its condition intact.
func TestWorkflowTemplateIAMConditionRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedTemplate(t, p)

	condition := map[string]any{
		"title":       "only-today",
		"description": "time bounded",
		"expression":  "request.time < timestamp(\"2027-01-01T00:00:00Z\")",
	}
	set, err := p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":      "roles/dataproc.worker",
				"members":   []any{"user:a@example.com"},
				"condition": condition,
			}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if set.Version != 3 {
		t.Fatalf("stored version = %d, want 3", set.Version)
	}

	got, err := p.GetIamPolicy(ctx, wtIAMName)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if got.Version != 3 {
		t.Fatalf("read-back version = %d, want 3", got.Version)
	}
	bm, _ := got.Bindings[0].(map[string]any)
	if bm["role"] != "roles/dataproc.worker" {
		t.Fatalf("role = %v", bm["role"])
	}
	cm, _ := bm["condition"].(map[string]any)
	if cm["title"] != condition["title"] || cm["expression"] != condition["expression"] || cm["description"] != condition["description"] {
		t.Fatalf("condition did not round-trip: %v", cm)
	}
}

// TestWorkflowTemplateIAMStaleEtag: etag OCC is enforced on the template policy
// plane (stale etag -> ABORTED/409).
func TestWorkflowTemplateIAMStaleEtag(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedTemplate(t, p)

	first, err := p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}}},
	})
	if err != nil {
		t.Fatalf("initial SetIamPolicy: %v", err)
	}
	_, err = p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{
			"etag":     "stale",
			"bindings": []any{map[string]any{"role": "roles/editor", "members": []any{"user:b@example.com"}}},
		},
	})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Status != "ABORTED" || pe.HTTPStatus != 409 {
		t.Fatalf("stale etag = %v, want ABORTED/409", err)
	}
	if got := p.mustGetPolicy(t, wtIAMName); got.Etag != first.Etag {
		t.Fatal("rejected write changed the stored policy")
	}
}

// mustGetPolicy fetches a stored policy or fails the test.
func (s *Service) mustGetPolicy(t *testing.T, resource string) policy.Policy {
	t.Helper()
	got, err := s.GetIamPolicy(context.Background(), resource)
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	return got
}

// TestWorkflowTemplateIAMMissingTemplate: every verb is NotFound before the
// policy plane is touched, and no policy row is created.
func TestWorkflowTemplateIAMMissingTemplate(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	missing := "projects/proj/regions/us-central1/workflowTemplates/absent"

	if _, err := p.GetIamPolicy(ctx, missing); providerStatus(err) != 404 {
		t.Fatalf("GetIamPolicy(missing) = %v, want 404", err)
	}
	_, err := p.SetIamPolicy(ctx, missing, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}}},
	})
	if providerStatus(err) != 404 {
		t.Fatalf("SetIamPolicy(missing) = %v, want 404", err)
	}
	if _, err := p.TestIamPermissions(ctx, missing, []string{"dataproc.workflowTemplates.get"}); providerStatus(err) != 404 {
		t.Fatalf("TestIamPermissions(missing) = %v, want 404", err)
	}

	// The rejected calls must not have written a policy row for the missing id.
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, WorkflowTemplatePolicyType, "us-central1/absent"); err == nil {
		t.Fatal("a NotFound IAM call created a policy row")
	}
}

// TestWorkflowTemplateIAMWrongShape: a name outside the three supported
// collections is InvalidArgument (the autoscaling-policy and operations
// collections, and locations-scoped names, have no served IAM surface).
func TestWorkflowTemplateIAMWrongShape(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	for _, name := range []string{
		"projects/proj/regions/us-central1/autoscalingPolicies/p1",
		"projects/proj/regions/us-central1/operations/o1",
		"projects/proj/locations/global/workflowTemplates/t1",
		"projects/proj/regions/us-central1/workflowTemplates/t1:getIamPolicy",
	} {
		if _, err := p.GetIamPolicy(ctx, name); providerStatus(err) != 400 {
			t.Fatalf("GetIamPolicy(%q) = %v, want 400", name, err)
		}
		if _, err := p.SetIamPolicy(ctx, name, map[string]any{}); providerStatus(err) != 400 {
			t.Fatalf("SetIamPolicy(%q) = %v, want 400", name, err)
		}
		if _, err := p.TestIamPermissions(ctx, name, nil); providerStatus(err) != 400 {
			t.Fatalf("TestIamPermissions(%q) = %v, want 400", name, err)
		}
	}
}

// TestWorkflowTemplateIAMTestPermissions echoes the requested permissions.
func TestWorkflowTemplateIAMTestPermissions(t *testing.T) {
	p := newProvider(t)
	seedTemplate(t, p)
	perms := []string{"dataproc.workflowTemplates.get", "dataproc.workflowTemplates.update"}
	got, err := p.TestIamPermissions(context.Background(), wtIAMName, perms)
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(got) != len(perms) || got[0] != perms[0] || got[1] != perms[1] {
		t.Fatalf("permissions = %v, want %v", got, perms)
	}
}

// providerStatus returns the HTTP status of a ProviderError, or 0.
func providerStatus(err error) int {
	pe, ok := err.(*model.ProviderError)
	if !ok {
		return 0
	}
	return pe.HTTPStatus
}

// seedCluster creates a cluster every cluster-IAM test targets.
func seedCluster(t *testing.T, p *Service, name string) {
	t.Helper()
	if _, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", name, gceInput()); err != nil {
		t.Fatalf("CreateCluster(%s): %v", name, err)
	}
}

// seedJob submits a job every job-IAM test targets.
func seedJob(t *testing.T, p *Service, id string) {
	t.Helper()
	if _, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", "c1", gceInput()); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := p.SubmitJob(context.Background(), "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": id},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob(%s): %v", id, err)
	}
}

func clusterIAMName(name string) string {
	return "projects/proj/regions/us-central1/clusters/" + name
}

func jobIAMName(id string) string {
	return "projects/proj/regions/us-central1/jobs/" + id
}

// TestClusterIAMRoundTrip: a cluster policy is stored and read back with etag
// OCC and the AIP-149 version rules, exactly like the workflow-template plane.
func TestClusterIAMRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedCluster(t, p, "c1")

	empty, err := p.GetIamPolicy(ctx, clusterIAMName("c1"))
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if empty.Version != 1 || len(empty.Bindings) != 0 {
		t.Fatalf("default policy = %+v, want version 1 / no bindings", empty)
	}

	set, err := p.SetIamPolicy(ctx, clusterIAMName("c1"), map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    "roles/dataproc.worker",
				"members": []any{"user:a@example.com"},
				"condition": map[string]any{
					"title":      "expires-soon",
					"expression": `request.time < timestamp("2027-01-01T00:00:00Z")`,
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if set.Version != 3 {
		t.Fatalf("conditional set version = %d, want 3", set.Version)
	}

	got := p.mustGetPolicy(t, clusterIAMName("c1"))
	if got.Version != 3 || len(got.Bindings) != 1 {
		t.Fatalf("read-back policy = %+v", got)
	}
	bm, _ := got.Bindings[0].(map[string]any)
	if bm["role"] != "roles/dataproc.worker" {
		t.Fatalf("binding role = %v", bm["role"])
	}

	// A stale etag is ABORTED/409 and leaves the stored policy alone.
	_, err = p.SetIamPolicy(ctx, clusterIAMName("c1"), map[string]any{
		"policy": map[string]any{
			"etag":     "stale",
			"bindings": []any{map[string]any{"role": "roles/editor", "members": []any{"user:b@example.com"}}},
		},
	})
	if pe, ok := err.(*model.ProviderError); !ok || pe.Status != "ABORTED" || pe.HTTPStatus != 409 {
		t.Fatalf("stale etag = %v, want ABORTED/409", err)
	}
	if got := p.mustGetPolicy(t, clusterIAMName("c1")); got.Etag != set.Etag {
		t.Fatal("rejected write changed the stored cluster policy")
	}
}

// TestClusterAndJobIAMPoliciesAreSeparate proves the per-collection policy types
// keep a cluster and a job that share an id from overwriting each other.
func TestClusterAndJobIAMPoliciesAreSeparate(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedCluster(t, p, "shared")
	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "shared"},
		"placement":  map[string]any{"clusterName": "shared"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	if _, err := p.SetIamPolicy(ctx, clusterIAMName("shared"), map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dataproc.viewer", "members": []any{"user:cluster@example.com"}}}},
	}); err != nil {
		t.Fatalf("cluster SetIamPolicy: %v", err)
	}
	if _, err := p.SetIamPolicy(ctx, jobIAMName("shared"), map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dataproc.editor", "members": []any{"user:job@example.com"}}}},
	}); err != nil {
		t.Fatalf("job SetIamPolicy: %v", err)
	}

	cluster, err := p.GetIamPolicy(ctx, clusterIAMName("shared"))
	if err != nil {
		t.Fatalf("cluster GetIamPolicy: %v", err)
	}
	job, err := p.GetIamPolicy(ctx, jobIAMName("shared"))
	if err != nil {
		t.Fatalf("job GetIamPolicy: %v", err)
	}
	roleOf := func(pol policy.Policy) string {
		if len(pol.Bindings) != 1 {
			t.Fatalf("policy = %+v", pol)
		}
		bm, _ := pol.Bindings[0].(map[string]any)
		role, _ := bm["role"].(string)
		return role
	}
	if roleOf(cluster) != "roles/dataproc.viewer" {
		t.Fatalf("cluster policy role = %q", roleOf(cluster))
	}
	if roleOf(job) != "roles/dataproc.editor" {
		t.Fatalf("job policy role = %q", roleOf(job))
	}
}

// TestClusterAndJobIAMMissingResource: every verb is NotFound before the policy
// plane is touched, and no policy row is created.
func TestClusterAndJobIAMMissingResource(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedCluster(t, p, "c1")

	cases := []struct {
		name       string
		policyType string
		key        string
	}{
		{clusterIAMName("absent"), ClusterPolicyType, "us-central1/absent"},
		{jobIAMName("absent"), JobPolicyType, "us-central1/absent"},
	}
	for _, tc := range cases {
		if _, err := p.GetIamPolicy(ctx, tc.name); providerStatus(err) != 404 {
			t.Fatalf("GetIamPolicy(%s) = %v, want 404", tc.name, err)
		}
		_, err := p.SetIamPolicy(ctx, tc.name, map[string]any{
			"policy": map[string]any{"bindings": []any{map[string]any{
				"role": "roles/viewer", "members": []any{"user:a@example.com"}}}},
		})
		if providerStatus(err) != 404 {
			t.Fatalf("SetIamPolicy(%s) = %v, want 404", tc.name, err)
		}
		if _, err := p.TestIamPermissions(ctx, tc.name, []string{"dataproc.clusters.get"}); providerStatus(err) != 404 {
			t.Fatalf("TestIamPermissions(%s) = %v, want 404", tc.name, err)
		}
		if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, tc.policyType, tc.key); err == nil {
			t.Fatalf("a NotFound IAM call created a %s policy row", tc.policyType)
		}
	}
}

// TestIAMReadDoesNotAdvanceResourceState: the existence check reads the store,
// not the GetCluster/GetJob read path, so an IAM read never settles a lazy
// CREATING cluster or a PENDING job (real getIamPolicy has no such side effect).
func TestIAMReadDoesNotAdvanceResourceState(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedCluster(t, p, "c1")
	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	if _, err := p.GetIamPolicy(ctx, clusterIAMName("c1")); err != nil {
		t.Fatalf("cluster GetIamPolicy: %v", err)
	}
	if _, err := p.GetIamPolicy(ctx, jobIAMName("j1")); err != nil {
		t.Fatalf("job GetIamPolicy: %v", err)
	}

	c, err := p.store.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.Status.State != "CREATING" {
		t.Fatalf("cluster state after an IAM read = %q, want CREATING", c.Status.State)
	}
	j, err := p.store.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if j.Status.State != "PENDING" {
		t.Fatalf("job state after an IAM read = %q, want PENDING", j.Status.State)
	}
}

// TestClusterDeletePurgesIAMPolicy: the policy is bound to the resource's
// lifetime, so a cluster re-created under the same name starts with the empty
// default instead of inheriting the old grants.
func TestClusterDeletePurgesIAMPolicy(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedCluster(t, p, "c1")

	if _, err := p.SetIamPolicy(ctx, clusterIAMName("c1"), map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dataproc.worker", "members": []any{"user:a@example.com"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if _, err := p.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	// The first read after the delete completes drops the record (and policy).
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); providerStatus(err) != 404 {
		t.Fatalf("GetCluster after delete = %v, want 404", err)
	}
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, ClusterPolicyType, "us-central1/c1"); err == nil {
		t.Fatal("cluster delete left its IAM policy behind")
	}

	seedCluster(t, p, "c1")
	recreated := p.mustGetPolicy(t, clusterIAMName("c1"))
	if len(recreated.Bindings) != 0 {
		t.Fatalf("re-created cluster inherited %d bindings", len(recreated.Bindings))
	}
}

// TestJobDeletePurgesIAMPolicy: deleting a terminal job drops its policy.
func TestJobDeletePurgesIAMPolicy(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedJob(t, p, "j1")

	if _, err := p.SetIamPolicy(ctx, jobIAMName("j1"), map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dataproc.viewer", "members": []any{"user:a@example.com"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}

	// A job must be terminal before it can be deleted; each read advances it.
	for i := 0; i < 16; i++ {
		j, err := p.GetJob(ctx, "proj", "us-central1", "j1")
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if j.Status.State == "DONE" || j.Status.State == "ERROR" || j.Status.State == "CANCELLED" {
			break
		}
	}
	if err := p.DeleteJob(ctx, "proj", "us-central1", "j1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, JobPolicyType, "us-central1/j1"); err == nil {
		t.Fatal("job delete left its IAM policy behind")
	}
}

// TestWorkflowTemplateDeletePurgesIAMPolicy: deleting the template drops its
// policy too.
func TestWorkflowTemplateDeletePurgesIAMPolicy(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	seedTemplate(t, p)

	if _, err := p.SetIamPolicy(ctx, wtIAMName, map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dataproc.worker", "members": []any{"user:a@example.com"}}}},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if err := p.DeleteWorkflowTemplate(ctx, "proj", "us-central1", "t1", 0); err != nil {
		t.Fatalf("DeleteWorkflowTemplate: %v", err)
	}
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, WorkflowTemplatePolicyType, "us-central1/t1"); err == nil {
		t.Fatal("workflow template delete left its IAM policy behind")
	}
}
