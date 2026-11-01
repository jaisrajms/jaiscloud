package dataproc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// ─── version aliases ─────────────────────────────────────────────────────────

// TestResolveGKEEngineVersion covers the documented forms from the
// "Release versions" guide: fully qualified `<major.minor>-dataproc-<n>` releases,
// the `latest`/`<major>`/`<major.minor>` aliases and the `dataproc-<major.minor>`
// image-line alias, all dereferenced to a concrete release at create time.
func TestResolveGKEEngineVersion(t *testing.T) {
	ok := map[string]string{
		"3.5-dataproc-28": "3.5-dataproc-28",
		"3.5-dataproc-17": "3.5-dataproc-17", // deprecated, still pinned by clients
		"3.1-dataproc-14": "3.1-dataproc-14",
		"latest":          "3.5-dataproc-28",
		"3":               "3.5-dataproc-28",
		"3.5":             "3.5-dataproc-28",
		"3.1":             "3.1-dataproc-17",
		"2.4":             "2.4-dataproc-17",
		" 3.5 ":           "3.5-dataproc-28",
		// The only image line real GCP accepts for virtual clusters; it resolves
		// to the same newest release as `latest`.
		"dataproc-2.2": "3.5-dataproc-28",
	}
	for in, want := range ok {
		got, err := resolveGKEEngineVersion(in)
		if err != nil || got != want {
			t.Fatalf("resolveGKEEngineVersion(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	// The image lines real GCP rejects ("not a valid spark version for Virtual
	// clusters") stay InvalidArgument: they are recorded, not inferred.
	bad := []string{
		"", "  ", "9", "3.6", "3.5.1", "3.5-dataproc-99", "spark-3.5", "3.5-dataproc",
		"dataproc-1.4", "dataproc-1.5", "dataproc-2.0", "dataproc-2.1", "dataproc-2.3",
		"dataproc-3.0", "dataproc-4.0", "dataproc-2.2.1", "DATAPROC-2.2",
	}
	for _, in := range bad {
		if got, err := resolveGKEEngineVersion(in); err == nil {
			t.Fatalf("resolveGKEEngineVersion(%q) = %q; want an InvalidArgument", in, got)
		}
	}
}

// ─── software config / staging bucket ────────────────────────────────────────

func TestValidateVirtualClusterConfig_SoftwareConfig(t *testing.T) {
	base := func(software map[string]any) map[string]any {
		kcc := map[string]any{
			"gkeClusterConfig": map[string]any{"gkeClusterTarget": gkeClusterPath},
		}
		if software != nil {
			kcc["kubernetesSoftwareConfig"] = software
		}
		return map[string]any{"kubernetesClusterConfig": kcc}
	}
	cases := []struct {
		name     string
		software map[string]any
		want     string // substring of the error, "" = accepted
	}{
		{
			name: "absent software config is accepted",
		},
		{
			name:     "SPARK resolved from an alias",
			software: map[string]any{"componentVersion": map[string]any{"SPARK": "3.5"}},
		},
		{
			name:     "SPARK resolved from latest",
			software: map[string]any{"componentVersion": map[string]any{"SPARK": "latest"}},
		},
		{
			name:     "componentVersion absent",
			software: map[string]any{"properties": map[string]any{"spark:spark.master": "local"}},
			want:     "requires at least one entry",
		},
		{
			name:     "componentVersion empty",
			software: map[string]any{"componentVersion": map[string]any{}},
			want:     "requires at least one entry",
		},
		{
			name:     "unknown component",
			software: map[string]any{"componentVersion": map[string]any{"HIVE": "3.5"}},
			want:     "unknown component",
		},
		{
			name:     "unknown version alias",
			software: map[string]any{"componentVersion": map[string]any{"SPARK": "9"}},
			want:     "unknown Spark engine version",
		},
		{
			name: "spark and dataproc property prefixes",
			software: map[string]any{
				"componentVersion": map[string]any{"SPARK": "3.5"},
				"properties": map[string]any{
					"spark:spark.executor.memory":                        "4g",
					"dataproc:dataproc.gke.agent.google-service-account": "sa@proj.iam.gserviceaccount.com",
				},
			},
		},
		{
			name: "yarn property prefix is rejected",
			software: map[string]any{
				"componentVersion": map[string]any{"SPARK": "3.5"},
				"properties":       map[string]any{"yarn:yarn.nodemanager.resource.memory-mb": "4096"},
			},
			want: "must use the spark: or dataproc: prefix",
		},
	}
	for _, tc := range cases {
		raw, err := json.Marshal(base(tc.software))
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		v, err := validateVirtualClusterConfig(raw, "demo")
		if tc.want == "" {
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: want error containing %q, got %v", tc.name, tc.want, err)
		}
		if v != nil {
			t.Fatalf("%s: rejected config returned a parse result", tc.name)
		}
	}
}

func TestValidateVirtualClusterConfig_StagingBucketForm(t *testing.T) {
	build := func(bucket any) []byte {
		vcc := map[string]any{
			"kubernetesClusterConfig": map[string]any{
				"gkeClusterConfig": map[string]any{"gkeClusterTarget": gkeClusterPath},
			},
		}
		if bucket != nil {
			vcc["stagingBucket"] = bucket
		}
		raw, err := json.Marshal(vcc)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return raw
	}
	for _, bucket := range []string{"dataproc-staging-proj", "my.bucket-1"} {
		if _, err := validateVirtualClusterConfig(build(bucket), "demo"); err != nil {
			t.Fatalf("bare bucket name %q rejected: %v", bucket, err)
		}
	}
	if _, err := validateVirtualClusterConfig(build(nil), "demo"); err != nil {
		t.Fatalf("absent stagingBucket rejected: %v", err)
	}
	for _, bucket := range []string{"gs://dataproc-staging-proj", "gs://bucket/path", "bucket/path", "bucket name"} {
		_, err := validateVirtualClusterConfig(build(bucket), "demo")
		if err == nil || !strings.Contains(err.Error(), "bare Cloud Storage bucket name") {
			t.Fatalf("stagingBucket %q: want the bare-name error, got %v", bucket, err)
		}
	}
}

// TestValidateVirtualClusterConfig_NamespaceDefault verifies the documented
// default: "If not specified, the name of the Dataproc Cluster is used."
func TestValidateVirtualClusterConfig_NamespaceDefault(t *testing.T) {
	build := func(namespace any) []byte {
		kcc := map[string]any{"gkeClusterConfig": map[string]any{"gkeClusterTarget": gkeClusterPath}}
		if namespace != nil {
			kcc["kubernetesNamespace"] = namespace
		}
		raw, err := json.Marshal(map[string]any{"kubernetesClusterConfig": kcc})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return raw
	}
	v, err := validateVirtualClusterConfig(build(nil), "my-cluster")
	if err != nil {
		t.Fatalf("unset namespace: %v", err)
	}
	if v.namespace != "my-cluster" {
		t.Fatalf("unset namespace = %q, want the cluster name", v.namespace)
	}
	v, err = validateVirtualClusterConfig(build("explicit"), "my-cluster")
	if err != nil {
		t.Fatalf("explicit namespace: %v", err)
	}
	if v.namespace != "explicit" {
		t.Fatalf("explicit namespace = %q, want %q", v.namespace, "explicit")
	}
}

// TestApplyVirtualClusterDefaults verifies the persisted object carries the
// resolved namespace and component version, and that a GCE cluster is untouched.
func TestApplyVirtualClusterDefaults(t *testing.T) {
	raw := []byte(`{"stagingBucket":"bucket","kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"` + gkeClusterPath + `"},"kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"latest"},"properties":{"spark:a":"b"}}}}`)
	v, err := validateVirtualClusterConfig(raw, "demo")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	normalized, err := applyVirtualClusterDefaults(raw, "demo", v)
	if err != nil {
		t.Fatalf("applyVirtualClusterDefaults: %v", err)
	}
	kcc, _ := mustJSONMap(normalized)["kubernetesClusterConfig"].(map[string]any)
	if got := bodyString(kcc, "kubernetesNamespace"); got != "demo" {
		t.Fatalf("namespace = %q, want the cluster name", got)
	}
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	if got := bodyStringMap(software, "componentVersion")["SPARK"]; got != "3.5-dataproc-28" {
		t.Fatalf("SPARK component version = %q, want the resolved release", got)
	}
	if got := bodyStringMap(software, "properties")["spark:a"]; got != "b" {
		t.Fatalf("properties not preserved: %v", software)
	}
	// A GCE cluster has no virtualClusterConfig to normalize.
	if out, err := applyVirtualClusterDefaults(nil, "demo", nil); err != nil || len(out) != 0 {
		t.Fatalf("nil config: %q, %v", out, err)
	}
}

// ─── create path: defaults rendered, namespace ownership ─────────────────────

// TestCreateCluster_GKEDefaultsRendered verifies a create that omits the
// namespace and pins an alias renders the resolved values on read.
func TestCreateCluster_GKEDefaultsRendered(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	vcc := map[string]any{"kubernetesClusterConfig": map[string]any{
		"gkeClusterConfig":         map[string]any{"gkeClusterTarget": gkeClusterPath},
		"kubernetesSoftwareConfig": map[string]any{"componentVersion": map[string]any{"SPARK": "3.5"}},
	}}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-defaults", gkeInput(t, vcc)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-defaults")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	rendered, _ := ClusterJSON(c)["virtualClusterConfig"].(map[string]any)
	kcc, _ := rendered["kubernetesClusterConfig"].(map[string]any)
	if got := bodyString(kcc, "kubernetesNamespace"); got != "gke-defaults" {
		t.Fatalf("rendered namespace = %q, want the cluster name", got)
	}
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	if got := bodyStringMap(software, "componentVersion")["SPARK"]; got != "3.5-dataproc-28" {
		t.Fatalf("rendered SPARK version = %q, want the resolved release", got)
	}
	// The k8s placement namespace stays project-scoped (the emulator's k3d host
	// is shared across projects) rather than the bare cluster name.
	if c.Namespace != "" && c.Namespace == "gke-defaults" {
		t.Fatalf("placement namespace must stay project-scoped, got %q", c.Namespace)
	}
}

// TestCreateCluster_GKENamespaceConflict verifies a namespace already installed
// by another live virtual cluster on the same GKE cluster is FailedPrecondition,
// that a different GKE cluster is not a conflict, and that delete-then-recreate
// of the same name works (the recreate guide's flow).
func TestCreateCluster_GKENamespaceConflict(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	withNamespace := func(target, namespace string) ClusterInput {
		return gkeInput(t, map[string]any{"kubernetesClusterConfig": map[string]any{
			"kubernetesNamespace": namespace,
			"gkeClusterConfig":    map[string]any{"gkeClusterTarget": target},
		}})
	}
	const otherGKE = "projects/proj/locations/us-central1/clusters/gke-2"
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "first", withNamespace(gkeClusterPath, "shared")); err != nil {
		t.Fatalf("CreateCluster first: %v", err)
	}
	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "second", withNamespace(gkeClusterPath, "shared"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 {
		t.Fatalf("duplicate namespace: want 400 FailedPrecondition, got %v", err)
	}
	if !strings.Contains(pe.Message, "already used by Dataproc virtual cluster") {
		t.Fatalf("duplicate namespace message = %q", pe.Message)
	}
	// The same namespace on a different GKE cluster is fine.
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "other-gke", withNamespace(otherGKE, "shared")); err != nil {
		t.Fatalf("namespace on another GKE cluster: %v", err)
	}
	// Deleting the owner frees the namespace, so the recreate flow works.
	if _, err := p.DeleteCluster(ctx, "proj", "us-central1", "first"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "first"); err == nil {
		t.Fatal("deleted cluster still readable")
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "second", withNamespace(gkeClusterPath, "shared")); err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
}

// TestCreateCluster_GKENamespaceConflictIgnoresGCE verifies the ownership check
// only considers GKE-backed clusters: a GCE cluster has no kubernetesNamespace.
func TestCreateCluster_GKENamespaceConflictIgnoresGCE(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gce-1",
		ClusterInput{Config: []byte(`{"gceClusterConfig":{"zoneUri":"us-central1-a"}}`)}); err != nil {
		t.Fatalf("CreateCluster GCE: %v", err)
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gce-1-gke", gkeInput(t, map[string]any{
		"kubernetesClusterConfig": map[string]any{
			"kubernetesNamespace": "gce-1",
			"gkeClusterConfig":    map[string]any{"gkeClusterTarget": gkeClusterPath},
		},
	})); err != nil {
		t.Fatalf("GKE cluster must not conflict with a GCE cluster of the same name: %v", err)
	}
}
