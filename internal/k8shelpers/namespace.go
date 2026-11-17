package k8shelpers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Namespace-lifecycle seam: engine-bearing, cluster-shaped resources get one
// Kubernetes namespace each ("a namespace per pod-owning resource"), created
// with the resource and torn down with it. This file owns the naming, the
// create/delete/sweep lifecycle, the ownership labels that keep an adopted
// (pre-existing, not emulator-created) namespace safe from deletion, and the
// per-namespace executor RBAC bootstrap.
//
// The process-wide namespace (cfg.K8sNamespace) remains the default for engines
// that have not been migrated; these helpers are additive.
//
// Executor identity: the RoleBinding created in a managed namespace must bind
// the ServiceAccount the emulator *itself* runs as, which is not necessarily
// DefaultExecutorServiceAccount/DefaultExecutorServiceAccountNamespace (the AWS
// substrate's `jaiscloud`/`jaiscloud`). ResolveOwnServiceAccount resolves it once
// per process with the precedence JAISCLOUD_K8S_SA / JAISCLOUD_K8S_SA_NAMESPACE
// env, then the pod's projected .../serviceaccount/namespace file, then the
// constants (see own_identity.go). Callers that provision a per-resource
// namespace pass the resolved pair to EnsureNamespaceRBAC/ForSubjects; the
// empty-argument form keeps the historical constant default for out-of-cluster
// runs and existing callers.

const (
	// managedByLabel marks a namespace as emulator-created so delete/sweep never
	// removes a namespace the emulator merely adopted. The value is always
	// ManagedByValue.
	managedByLabel = "jaiscloud.io/managed-by"
	// serviceLabel names the owning service (e.g. "dataproc"), so one service's
	// sweep cannot touch another's namespaces.
	serviceLabel = "jaiscloud.io/service"
	// substrateLabel names the substrate ("aws" or "gcp") that created the
	// namespace. Both substrates hold cluster-scoped namespace-admin RBAC, so
	// this label (together with instanceLabel and the ValidatingAdmissionPolicy
	// in deploy/k8s/rbac-admission-policy.yaml) is what proves a namespace is
	// ours: sweep, delete and the admission policy all key off it.
	substrateLabel = "jaiscloud.io/substrate"
	// instanceLabel pins the emulator instance (JAISCLOUD_INSTANCE_ID) that owns
	// the namespace, so parallel instances of the same substrate cannot reap
	// each other's namespaces.
	instanceLabel = "jaiscloud.io/instance-id"
	// ManagedByValue is the value stamped on managedByLabel.
	ManagedByValue = "jaiscloud"

	// NamespacePrefix is the prefix every derived namespace name carries, so a
	// prefix-based sweep is also possible.
	NamespacePrefix = "gcp-"

	// maxLabelValueLength is the Kubernetes label-value limit (63 chars).
	maxLabelValueLength = 63

	// ExecutorClusterRoleName is the ClusterRole (deploy/k8s/rbac.yaml) a
	// per-namespace RoleBinding points at so the executor ServiceAccount can run
	// pods/services/configmaps/jobs in the new namespace.
	ExecutorClusterRoleName = "jaiscloud-executor"
	// ExecutorRoleBindingName is the RoleBinding created in each new namespace.
	ExecutorRoleBindingName = "jaiscloud-executor"
	// DefaultExecutorServiceAccount / ...Namespace are the fallback identity the
	// emulator runs as (deploy/k8s/rbac.yaml) when nothing else identifies it —
	// an out-of-cluster dev binary, or a pod that projects no namespace file. A
	// per-namespace RoleBinding must bind the emulator's own ServiceAccount, not
	// a Spark pod's; resolve it with ResolveOwnServiceAccount (own_identity.go)
	// rather than reading these constants directly.
	DefaultExecutorServiceAccount          = "jaiscloud"
	DefaultExecutorServiceAccountNamespace = "jaiscloud"

	// maxNamespaceLength is the DNS-1123 label limit for a namespace name.
	maxNamespaceLength = 63
	// namespaceHashLen is the length of the deterministic disambiguating hash
	// suffix appended to every derived name.
	namespaceHashLen = 8
	// namespaceSweepCap bounds a sweep so a mislabelled cluster cannot trigger
	// unbounded deletes (mirrors defaultWorkloadSweepCap).
	defaultNamespaceSweepCap = 2000

	// namespaceTerminateWait bounds how long ensureNamespaceRaw waits for a
	// Terminating namespace to disappear before recreating it (see below).
	namespaceTerminateWait = 60 * time.Second
	// namespaceTerminatePoll is the poll interval while waiting.
	namespaceTerminatePoll = 200 * time.Millisecond
)

// Process identity stamped on every namespace the emulator creates. Both
// substrates hold cluster-scoped namespace admin, so without this the sweep and
// delete paths cannot prove a namespace is theirs; SetIdentity is called once at
// startup by each binary (after the instance ID is resolved) and the labels are
// also what the admission policy requires.
var (
	identityMu         sync.RWMutex
	identitySubstrate  string
	identityInstanceID string
)

// SetIdentity records which substrate ("aws" or "gcp") and which instance
// (JAISCLOUD_INSTANCE_ID) this process is. It is safe to call once before any
// namespace operation; SetIdentity("", "") restores the legacy behaviour
// (ownership-only labels, no identity filtering), which tests and out-of-cluster
// callers rely on.
//
// The instance ID is sanitized and truncated to a valid label value: an ID with
// uppercase letters, dots or an over-long value would otherwise make every
// namespace create fail validation.
func SetIdentity(substrate, instanceID string) {
	identityMu.Lock()
	defer identityMu.Unlock()
	identitySubstrate = labelValue(substrate)
	identityInstanceID = labelValue(instanceID)
}

// identity returns the configured substrate and instance ID (both empty when no
// identity is set).
func identity() (substrate, instanceID string) {
	identityMu.RLock()
	defer identityMu.RUnlock()
	return identitySubstrate, identityInstanceID
}

// labelValue sanitizes s into a valid Kubernetes label value: lowercase
// alphanumerics and '-', at most 63 chars, no leading/trailing '-' (a truncation
// can leave one).
func labelValue(s string) string {
	v := sanitizeLabelValue(s)
	if len(v) > maxLabelValueLength {
		v = strings.Trim(v[:maxLabelValueLength], "-")
	}
	return v
}

// OwnerNamespaceLabels returns the ownership labels stamped on a service's
// managed namespaces: managed-by always, service only when non-empty (an empty
// label value is invalid-ish and would make the service selector useless), and
// the substrate/instance identity when SetIdentity configured one.
func OwnerNamespaceLabels(service string) map[string]string {
	labels := map[string]string{managedByLabel: ManagedByValue}
	if service != "" {
		labels[serviceLabel] = service
	}
	substrate, instanceID := identity()
	if substrate != "" {
		labels[substrateLabel] = substrate
	}
	if instanceID != "" {
		labels[instanceLabel] = instanceID
	}
	return labels
}

// NamespaceName derives a deterministic, RFC-1123, <=63-char namespace name for
// a pod-owning resource: "gcp-<service>-<project>-<sanitized-id>-<hash8>". The
// hash is over the *unsanitized* inputs, so two ids that sanitize to the same
// string still get distinct namespaces, and the same inputs always yield the
// same name (restart-safe).
func NamespaceName(service, project, id string) string {
	raw := strings.Join([]string{service, project, id}, "-")
	base := strings.Trim(NamespacePrefix+sanitizeLabelValue(raw), "-")
	sum := sha256.Sum256([]byte(NamespacePrefix + raw))
	hash := hex.EncodeToString(sum[:])[:namespaceHashLen]
	maxBase := maxNamespaceLength - namespaceHashLen - 1 // room for "-<hash>"
	if len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
	}
	if base == "" {
		base = "gcp"
	}
	return base + "-" + hash
}

// sanitizeLabelValue lowercases and replaces every run of characters outside
// [a-z0-9-] with a single '-', trimming leading/trailing dashes. The result is
// a valid DNS-1123 label segment (though possibly longer than 63 chars; callers
// truncate).
func sanitizeLabelValue(s string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

// IsManagedNamespace reports whether ns carries the emulator ownership label
// (and, when service is non-empty, belongs to that service). When a process
// identity is configured it must also match: a namespace without the labels
// cannot be proven to be ours, so sweep/delete leave it alone.
func IsManagedNamespace(ns *corev1.Namespace, service string) bool {
	if ns == nil || ns.Labels == nil {
		return false
	}
	if ns.Labels[managedByLabel] != ManagedByValue {
		return false
	}
	if service != "" && ns.Labels[serviceLabel] != service {
		return false
	}
	substrate, instanceID := identity()
	if substrate != "" && ns.Labels[substrateLabel] != substrate {
		return false
	}
	if instanceID != "" && ns.Labels[instanceLabel] != instanceID {
		return false
	}
	return true
}

// EnsureManagedNamespace creates namespace when it is missing and returns
// whether it created it (true = emulator-owned; false = adopted/pre-existing).
// A Forbidden Get/Create (a namespaced-only ServiceAccount cannot manage
// cluster-scoped namespaces) returns ErrNamespaceForbidden so the caller can
// fall back to the process-wide namespace rather than fail.
func EnsureManagedNamespace(ctx context.Context, client kubernetes.Interface, namespace, service string) (bool, error) {
	return ensureNamespaceRaw(ctx, client, namespace, OwnerNamespaceLabels(service))
}

// ensureNamespaceRaw is the shared create-if-missing primitive. labels==nil
// defaults to the legacy single-namespace label so EnsureNamespace keeps its
// historical behaviour.
func ensureNamespaceRaw(ctx context.Context, client kubernetes.Interface, namespace string, labels map[string]string) (bool, error) {
	if namespace == "" {
		return false, fmt.Errorf("k8shelpers: namespace name is required")
	}
	ns, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	switch {
	case err == nil:
		if ns.DeletionTimestamp == nil {
			// Already present; adopted, not created by us. A namespace an older
			// emulator created before identity labels existed is scoped in place
			// (best-effort) so sweep/delete and the admission policy see it.
			backfillIdentityLabels(ctx, client, ns, labels)
			return false, nil
		}
		// The namespace exists but is Terminating — its prior owner (a Dataproc
		// cluster or Kafka broker with the same derived name) was just deleted,
		// and the namespace teardown is asynchronous. Wait for it to disappear
		// and recreate it: adopting a Terminating namespace makes every later
		// create in it fail ("... is being terminated").
		if waitErr := waitNamespaceGone(ctx, client, namespace); waitErr != nil {
			return false, waitErr
		}
	case k8serrors.IsForbidden(err):
		return false, fmt.Errorf("%w: get %s", ErrNamespaceForbidden, namespace)
	case !k8serrors.IsNotFound(err):
		return false, err
	}
	if labels == nil {
		labels = map[string]string{"managed-by": "jaiscloud"}
	}
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: labels},
	}, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return false, nil
	}
	if k8serrors.IsForbidden(err) {
		return false, fmt.Errorf("%w: create %s", ErrNamespaceForbidden, namespace)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// backfillIdentityLabels stamps the process identity onto an existing namespace
// that an older emulator created before substrate/instance labels existed, so it
// becomes visible to this process's sweep/delete filter (and satisfies the
// admission policy on later updates). It only fills gaps and is deliberately
// conservative:
//
//   - the namespace must already be emulator-owned (managedByLabel) — an adopted
//     or foreign namespace is never relabelled;
//   - a per-service caller's namespace must carry the same service label;
//   - an existing, different substrate/instance label means the namespace
//     belongs to another substrate or instance, so it is left untouched.
//
// It is best-effort: a failure (for example a ServiceAccount that may get but
// not update cluster-scoped namespaces) is logged, not returned, because the
// namespace is still usable and the caller only asked to ensure it exists.
func backfillIdentityLabels(ctx context.Context, client kubernetes.Interface, ns *corev1.Namespace, labels map[string]string) {
	substrate, instanceID := identity()
	if substrate == "" || len(labels) == 0 {
		return
	}
	if ns.Labels[managedByLabel] != ManagedByValue || labels[managedByLabel] != ManagedByValue {
		return
	}
	if want := labels[serviceLabel]; want != "" && ns.Labels[serviceLabel] != want {
		return
	}
	if got := ns.Labels[substrateLabel]; got != "" && got != substrate {
		return
	}
	if got := ns.Labels[instanceLabel]; instanceID != "" && got != "" && got != instanceID {
		return
	}
	missing := make(map[string]string, 2)
	if ns.Labels[substrateLabel] == "" {
		missing[substrateLabel] = substrate
	}
	if instanceID != "" && ns.Labels[instanceLabel] == "" {
		missing[instanceLabel] = instanceID
	}
	if len(missing) == 0 {
		return
	}
	patched := ns.DeepCopy()
	if patched.Labels == nil {
		patched.Labels = make(map[string]string, len(missing))
	}
	for k, v := range missing {
		patched.Labels[k] = v
	}
	if _, err := client.CoreV1().Namespaces().Update(ctx, patched, metav1.UpdateOptions{}); err != nil {
		slog.Warn("k8shelpers: cannot backfill identity labels on managed namespace",
			"namespace", ns.Name, "substrate", substrate, "err", err)
		return
	}
	slog.Info("k8shelpers: backfilled identity labels on managed namespace",
		"namespace", ns.Name, "substrate", substrate, "instance", instanceID)
}

// waitNamespaceGone polls until a Terminating namespace is fully removed, so
// ensureNamespaceRaw can recreate it. It is bounded by namespaceTerminateWait
// (or the caller's earlier deadline) and returns ctx.Err() on timeout.
func waitNamespaceGone(ctx context.Context, client kubernetes.Interface, namespace string) error {
	ctx, cancel := context.WithTimeout(ctx, namespaceTerminateWait)
	defer cancel()
	ticker := time.NewTicker(namespaceTerminatePoll)
	defer ticker.Stop()
	for {
		_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// DeleteManagedNamespace deletes namespace only when the emulator owns it
// (managedByLabel). It returns whether it deleted anything. A pre-existing
// namespace that was merely adopted, or one owned by another service, is left
// untouched. A Forbidden Get/Delete returns ErrNamespaceForbidden.
func DeleteManagedNamespace(ctx context.Context, client kubernetes.Interface, namespace, service string) (bool, error) {
	ns, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return false, nil
	}
	if k8serrors.IsForbidden(err) {
		return false, fmt.Errorf("%w: get %s", ErrNamespaceForbidden, namespace)
	}
	if err != nil {
		return false, err
	}
	if !IsManagedNamespace(ns, service) {
		return false, nil
	}
	return deleteNamespace(ctx, client, namespace)
}

func deleteNamespace(ctx context.Context, client kubernetes.Interface, namespace string) (bool, error) {
	err := client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return false, nil
	}
	if k8serrors.IsForbidden(err) {
		return false, fmt.Errorf("%w: delete %s", ErrNamespaceForbidden, namespace)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// namespaceSelector builds the label selector for a service's managed
// namespaces. An empty service matches every emulator-managed namespace. When a
// process identity is configured the selector also pins the substrate and
// instance, so neither ListManagedNamespaces nor a sweep can see the other
// substrate's namespaces at all.
func namespaceSelector(service string) string {
	sel := managedByLabel + "=" + ManagedByValue
	if service != "" {
		sel += "," + serviceLabel + "=" + service
	}
	substrate, instanceID := identity()
	if substrate != "" {
		sel += "," + substrateLabel + "=" + substrate
	}
	if instanceID != "" {
		sel += "," + instanceLabel + "=" + instanceID
	}
	return sel
}

// ListManagedNamespaces returns every namespace the emulator owns for service
// (all services when service is empty). It backs the startup/reset sweep and
// the namespace-aware ownership patcher.
func ListManagedNamespaces(ctx context.Context, client kubernetes.Interface, service string) ([]string, error) {
	list, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: namespaceSelector(service),
	})
	if k8serrors.IsForbidden(err) {
		return nil, fmt.Errorf("%w: list namespaces", ErrNamespaceForbidden)
	}
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, list.Items[i].Name)
	}
	return out, nil
}

// SweepManagedNamespaces deletes every namespace the emulator owns for service
// and returns how many were deleted. A pre-existing namespace that is not
// emulator-owned cannot appear in the label-selected list. The count is capped
// so a mislabelled cluster cannot trigger unbounded deletes.
func SweepManagedNamespaces(ctx context.Context, client kubernetes.Interface, service string) (int, error) {
	names, err := ListManagedNamespaces(ctx, client, service)
	if err != nil {
		return 0, err
	}
	deleted := 0
	var firstErr error
	for _, name := range names {
		if deleted >= defaultNamespaceSweepCap {
			break
		}
		ok, derr := deleteNamespace(ctx, client, name)
		if derr != nil {
			if firstErr == nil {
				firstErr = derr
			}
			continue
		}
		if ok {
			deleted++
		}
	}
	return deleted, firstErr
}

// RBACSubject is a ServiceAccount granted the executor ClusterRole in a managed
// namespace. The emulator's own ServiceAccount needs to create the driver
// Job/pod, and the Spark driver ServiceAccount needs to create executor pods.
type RBACSubject struct {
	Name      string
	Namespace string
}

// EnsureNamespaceRBAC creates the executor RoleBinding in namespace for the
// emulator's own ServiceAccount (deploy/k8s/rbac.yaml); pass empty
// serviceAccount/serviceAccountNamespace to use the constants. Callers that can
// resolve the running pod's identity should pass ResolveOwnServiceAccount's
// result instead, so a substrate that does not run in `jaiscloud` binds its own
// ServiceAccount. It is idempotent and best-effort against AlreadyExists; a
// Forbidden create returns ErrNamespaceForbidden so the caller can fall back.
func EnsureNamespaceRBAC(ctx context.Context, client kubernetes.Interface, namespace, serviceAccount, serviceAccountNamespace string) error {
	if serviceAccount == "" {
		serviceAccount = DefaultExecutorServiceAccount
	}
	if serviceAccountNamespace == "" {
		serviceAccountNamespace = DefaultExecutorServiceAccountNamespace
	}
	return EnsureNamespaceRBACForSubjects(ctx, client, namespace, RBACSubject{Name: serviceAccount, Namespace: serviceAccountNamespace})
}

// EnsureNamespaceRBACForSubjects creates one executor RoleBinding in namespace
// binding every subject (deduplicated) to the jaiscloud-executor ClusterRole. It
// is idempotent against AlreadyExists; a Forbidden create returns
// ErrNamespaceForbidden so the caller can fall back to the process-wide
// namespace.
func EnsureNamespaceRBACForSubjects(ctx context.Context, client kubernetes.Interface, namespace string, subjects ...RBACSubject) error {
	if namespace == "" {
		return nil
	}
	seen := make(map[RBACSubject]bool, len(subjects))
	rbacSubjects := make([]rbacv1.Subject, 0, len(subjects))
	for _, s := range subjects {
		if s.Name == "" {
			continue
		}
		if s.Namespace == "" {
			s.Namespace = namespace
		}
		if seen[s] {
			continue
		}
		seen[s] = true
		rbacSubjects = append(rbacSubjects, rbacv1.Subject{
			Kind:      "ServiceAccount",
			Name:      s.Name,
			Namespace: s.Namespace,
		})
	}
	if len(rbacSubjects) == 0 {
		return nil
	}
	_, err := client.RbacV1().RoleBindings(namespace).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ExecutorRoleBindingName,
			Namespace: namespace,
			// Same ownership/identity labels as the namespace: the admission
			// policy requires a RoleBinding created by a substrate identity to
			// carry its substrate label.
			Labels: OwnerNamespaceLabels(""),
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     ExecutorClusterRoleName,
		},
		Subjects: rbacSubjects,
	}, metav1.CreateOptions{})
	if err == nil || k8serrors.IsAlreadyExists(err) {
		return nil
	}
	if k8serrors.IsForbidden(err) {
		return fmt.Errorf("%w: rolebinding %s/%s", ErrNamespaceForbidden, namespace, ExecutorRoleBindingName)
	}
	return err
}
