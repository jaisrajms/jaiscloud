package dataproc

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"jaiscloud/internal/gcp/resource"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// This file owns the Dataproc-on-GKE virtualClusterConfig rules that are not
// about node pools: the effective Kubernetes namespace (defaulted to the cluster
// name), the Spark engine release aliases, the KubernetesSoftwareConfig shape
// and the staging-bucket form. Node-pool reconciliation lives in gke.go.

// gkeEngineReleases are the concrete Dataproc-on-GKE Spark engine releases the
// emulator knows, newest first. Source: the "Managed Service for Apache Spark on
// GKE release versions" table (Spark Engine 3.5 / 3.1 / 2.4). Deprecated
// releases stay listed because clients pin them.
var gkeEngineReleases = []string{
	"3.5-dataproc-28",
	"3.5-dataproc-22",
	"3.5-dataproc-17",
	"3.1-dataproc-17",
	"3.1-dataproc-16",
	"3.1-dataproc-15",
	"3.1-dataproc-14",
	"2.4-dataproc-17",
	"2.4-dataproc-16",
	"2.4-dataproc-15",
	"2.4-dataproc-14",
}

// gkeImageVersionAliases are the documented `dataproc-<major.minor>` image-line
// aliases, mapped to the concrete release they dereference to. Recorded from real
// GCP (docs/gcp/dataproc-gke-version-aliases.md): `dataproc-2.2` is the only
// image line accepted as a Spark version for virtual clusters — it is the
// Dataproc image line the GKE engine releases are built on — and it resolves to
// the same newest release as `latest`. Every other image alias (`dataproc-1.4`,
// `-1.5`, `-2.0`, `-2.1`, `-2.3`, `-3.0`, `-4.0`) is rejected by real GCP with
// "'<value>' is not a valid spark version for Virtual clusters.", so it stays
// InvalidArgument here rather than being mapped by inference.
var gkeImageVersionAliases = map[string]string{
	"dataproc-2.2": "3.5-dataproc-28",
}

// k8sComponentSpark is the only KubernetesComponent the GKE release table uses
// ("SPARK"); it is the one entry componentVersion must carry.
const k8sComponentSpark = "SPARK"

// rejectGKEVirtualClusterUpdate enforces the recreate-only lifecycle of a
// Dataproc-on-GKE virtual cluster: real GCP disallows update operations on one
// (release note 2023-03-28), so a patch whose updateMask applies
// virtualClusterConfig or config is InvalidArgument. The mask decides, exactly
// as it does for the persisted fields: a full resource sent under a labels-only
// mask is applied for labels and leaves the placement alone, and an empty mask
// means "everything supplied applies". Labels are therefore the only field a
// virtual cluster can update; GCE clusters are untouched.
func rejectGKEVirtualClusterUpdate(current dpstore.Cluster, in ClusterInput, mask []string) error {
	if !current.IsGKEBacked() {
		return nil
	}
	applies := func(field string, supplied bool) bool {
		return supplied && (len(mask) == 0 || containsMaskField(mask, field))
	}
	if !applies("virtualClusterConfig", in.VirtualClusterConfig != nil) && !applies("config", len(in.Config) > 0) {
		return nil
	}
	return invalidArgument("a Dataproc on GKE virtual cluster is recreate-only: clusters.patch cannot change virtualClusterConfig or config; " +
		"delete the cluster and create it again. Only labels are updatable on a virtual cluster, so a request whose updateMask selects " +
		"neither placement field is applied for labels even when its body still carries them.")
}

// validateGKENamespaceOwnership rejects a create whose effective Kubernetes
// namespace is already installed by another live virtual cluster on the same GKE
// cluster: Dataproc "verifies that another Dataproc VirtualCluster is not
// installed into it". It is FailedPrecondition (the request is well-formed but
// the cluster state forbids it), and it is what makes delete-then-recreate of a
// namespace work without changing the request.
func (s *Service) validateGKENamespaceOwnership(ctx context.Context, project, region, name string, v *gkeVirtualCluster) error {
	if v == nil || v.namespace == "" {
		return nil
	}
	clusters, err := s.store.ListClusters(ctx, project, region)
	if err != nil {
		return mapErr(err)
	}
	target := canonicalGKETarget(v)
	for _, c := range clusters {
		if c.Name == name || !c.IsGKEBacked() || c.Status.State == "DELETING" {
			continue
		}
		if KubernetesNamespaceFromVirtualClusterConfig(c.VirtualClusterConfig) != v.namespace {
			continue
		}
		// Scope the conflict to the GKE cluster when both sides name one; an
		// unspecified target cannot be scoped and is treated as a conflict.
		if other := gkeClusterTargetFromVirtualClusterConfig(c.VirtualClusterConfig); target != "" && other != "" && other != target {
			continue
		}
		return model.NewProviderError("FailedPrecondition", fmt.Sprintf(
			"kubernetesNamespace %q is already used by Dataproc virtual cluster %q", v.namespace, c.Name), 400)
	}
	return nil
}

// canonicalGKETarget renders the parsed target back as its canonical
// projects/{project}/locations/{location}/clusters/{cluster} form ("" when the
// virtual cluster names no target).
func canonicalGKETarget(v *gkeVirtualCluster) string {
	if v == nil || v.target == "" {
		return ""
	}
	return resource.ResourceID(v.targetProject)("container-cluster", v.targetLocation+"/"+v.target)
}

// resolveGKEEngineVersion dereferences a Spark engine version to a concrete
// release. The documented forms are a fully qualified `<major.minor>-dataproc-<n>`,
// the aliases `latest`, `<major>` and `<major.minor>`, and the image-line alias
// `dataproc-<major.minor>`; aliases are dereferenced at cluster creation, so two
// clusters created from the same alias can differ (recorded: gke-versions). An
// unknown alias or release is InvalidArgument — never silently accepted.
func resolveGKEEngineVersion(v string) (string, error) {
	s := strings.TrimSpace(v)
	if s == "" {
		return "", invalidArgument("kubernetesSoftwareConfig.componentVersion SPARK requires a version")
	}
	for _, rel := range gkeEngineReleases {
		if rel == s {
			return rel, nil
		}
	}
	if rel, ok := gkeImageVersionAliases[s]; ok {
		return rel, nil
	}
	if s == "latest" {
		return gkeEngineReleases[0], nil
	}
	if aliasMajor, aliasMinor, ok := parseVersionAlias(s); ok {
		for _, rel := range gkeEngineReleases {
			major, minor := releaseMajorMinor(rel)
			if major != aliasMajor || (aliasMinor != "" && aliasMinor != minor) {
				continue
			}
			return rel, nil
		}
	}
	return "", invalidArgument(fmt.Sprintf(
		"unknown Spark engine version %q: use a release such as %s, or an alias (latest, 3, 3.5, dataproc-2.2)",
		v, gkeEngineReleases[0]))
}

// parseVersionAlias splits a `<major>` or `<major.minor>` alias. The image-line
// form `dataproc-<major.minor>` is not a numeric alias: it is resolved by
// gkeImageVersionAliases, and any image line outside that table is
// InvalidArgument.
func parseVersionAlias(s string) (major, minor string, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return "", "", false
	}
	for _, p := range parts {
		if p == "" {
			return "", "", false
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return "", "", false
			}
		}
	}
	if len(parts) == 2 {
		return parts[0], parts[1], true
	}
	return parts[0], "", true
}

// releaseMajorMinor splits a concrete release (`3.5-dataproc-28`) into its
// engine major and major.minor.
func releaseMajorMinor(rel string) (major, minor string) {
	engine, _, ok := strings.Cut(rel, "-dataproc-")
	if !ok {
		return "", ""
	}
	major, minor, _ = strings.Cut(engine, ".")
	return major, minor
}

// gkeSoftwareConfig is the parsed KubernetesSoftwareConfig: every component
// version already resolved to a concrete release.
type gkeSoftwareConfig struct {
	components map[string]string
	properties map[string]string
}

// parseKubernetesSoftwareConfig validates KubernetesSoftwareConfig. A present
// software config must name at least one component, must carry SPARK, and its
// properties may only use the spark: or dataproc: prefixes (recorded ambiguity
// A4: the REST page says spark: only, the GKE guides use dataproc: too).
func parseKubernetesSoftwareConfig(m map[string]any) (*gkeSoftwareConfig, error) {
	out, errs := collectKubernetesSoftwareConfig(m)
	return out, errs.err()
}

// collectKubernetesSoftwareConfig is parseKubernetesSoftwareConfig with the
// failures exposed, so an unknown component and a bad property prefix are both
// reported. Map keys are visited in sorted order so the joined message is
// deterministic.
func collectKubernetesSoftwareConfig(m map[string]any) (*gkeSoftwareConfig, validationErrors) {
	out := &gkeSoftwareConfig{components: map[string]string{}}
	var errs validationErrors
	raw, ok := m["componentVersion"].(map[string]any)
	if !ok || len(raw) == 0 {
		errs.add(invalidArgument("kubernetesSoftwareConfig.componentVersion requires at least one entry"))
	} else {
		keys := make([]string, 0, len(raw))
		for key := range raw {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if key != k8sComponentSpark {
				errs.add(invalidArgument(fmt.Sprintf(
					"kubernetesSoftwareConfig.componentVersion has an unknown component %q: the only supported component is %s", key, k8sComponentSpark)))
				continue
			}
			version, _ := raw[key].(string)
			resolved, err := resolveGKEEngineVersion(version)
			if err != nil {
				errs.add(err)
				continue
			}
			out.components[key] = resolved
		}
		if _, ok := out.components[k8sComponentSpark]; !ok && errs == nil {
			errs.add(invalidArgument("kubernetesSoftwareConfig.componentVersion must contain a " + k8sComponentSpark + " entry"))
		}
	}
	out.properties = bodyStringMap(m, "properties")
	propKeys := make([]string, 0, len(out.properties))
	for key := range out.properties {
		propKeys = append(propKeys, key)
	}
	sort.Strings(propKeys)
	for _, key := range propKeys {
		if !strings.HasPrefix(key, "spark:") && !strings.HasPrefix(key, "dataproc:") {
			errs.add(invalidArgument(fmt.Sprintf(
				"kubernetesSoftwareConfig.properties key %q must use the spark: or dataproc: prefix", key)))
		}
	}
	return out, errs
}

// validateStagingBucketForm enforces that virtualClusterConfig.stagingBucket is a
// bare bucket name. A "gs://" URI is what clients pass to the GCE
// configBucket/tempBucket fields; the GKE field takes the name only.
func validateStagingBucketForm(bucket string) error {
	s := strings.TrimSpace(bucket)
	if s == "" {
		return nil
	}
	if strings.Contains(s, "://") || strings.Contains(s, "/") {
		return invalidArgument(fmt.Sprintf(
			"virtualClusterConfig.stagingBucket must be a bare Cloud Storage bucket name, got %q", bucket))
	}
	if strings.ContainsAny(s, " \t") {
		return invalidArgument(fmt.Sprintf(
			"virtualClusterConfig.stagingBucket must be a bare Cloud Storage bucket name, got %q", bucket))
	}
	return nil
}

// applyVirtualClusterDefaults returns the virtualClusterConfig that is persisted
// for a create: the caller's object with the effective kubernetesNamespace
// filled in (the API defaults it to the cluster name) and the SPARK
// componentVersion rewritten from an alias to a concrete release. Both are
// output-visible, so a read echoes the resolved values rather than the alias.
func applyVirtualClusterDefaults(raw json.RawMessage, clusterName string, v *gkeVirtualCluster) (json.RawMessage, error) {
	if v == nil || len(raw) == 0 {
		return raw, nil
	}
	vcc := mustJSONMap(raw)
	kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any)
	if !ok {
		return raw, nil // already rejected by validateVirtualClusterConfig
	}
	if bodyString(kcc, "kubernetesNamespace") == "" {
		kcc["kubernetesNamespace"] = clusterName
	}
	if v.software != nil {
		if sw, ok := kcc["kubernetesSoftwareConfig"].(map[string]any); ok {
			versions := make(map[string]any, len(v.software.components))
			for key, value := range v.software.components {
				versions[key] = value
			}
			sw["componentVersion"] = versions
		}
	}
	out, err := json.Marshal(vcc)
	if err != nil {
		return raw, err
	}
	return out, nil
}

// gkeClusterTargetFromVirtualClusterConfig extracts
// kubernetesClusterConfig.gkeClusterConfig.gkeClusterTarget ("" when unset or
// malformed).
func gkeClusterTargetFromVirtualClusterConfig(raw json.RawMessage) string {
	vcc := mustJSONMap(raw)
	if vcc == nil {
		return ""
	}
	kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any)
	if !ok {
		return ""
	}
	gke, ok := kcc["gkeClusterConfig"].(map[string]any)
	if !ok {
		return ""
	}
	return bodyString(gke, "gkeClusterTarget")
}
