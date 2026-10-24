package k8shelpers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
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

const (
	// managedByLabel marks a namespace as emulator-created so delete/sweep never
	// removes a namespace the emulator merely adopted. The value is always
	// ManagedByValue.
	managedByLabel = "jaiscloud.io/managed-by"
	// serviceLabel names the owning service (e.g. "dataproc"), so one service's
	// sweep cannot touch another's namespaces.
	serviceLabel = "jaiscloud.io/service"
	// ManagedByValue is the value stamped on managedByLabel.
	ManagedByValue = "jaiscloud"

	// NamespacePrefix is the prefix every derived namespace name carries, so a
	// prefix-based sweep is also possible.
	NamespacePrefix = "gcp-"

	// ExecutorClusterRoleName is the ClusterRole (deploy/k8s/rbac.yaml) a
	// per-namespace RoleBinding points at so the executor ServiceAccount can run
	// pods/services/configmaps/jobs in the new namespace.
	ExecutorClusterRoleName = "jaiscloud-executor"
	// ExecutorRoleBindingName is the RoleBinding created in each new namespace.
	ExecutorRoleBindingName = "jaiscloud-executor"
	// DefaultExecutorServiceAccount / ...Namespace identify the ServiceAccount
	// the emulator itself runs as (deploy/k8s/rbac.yaml). A per-namespace
	// RoleBinding must bind this identity, not a Spark pod's service account.
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

// OwnerNamespaceLabels returns the ownership labels stamped on a service's
// managed namespaces.
func OwnerNamespaceLabels(service string) map[string]string {
	return map[string]string{
		managedByLabel: ManagedByValue,
		serviceLabel:   service,
	}
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
// (and, when service is non-empty, belongs to that service).
func IsManagedNamespace(ns *corev1.Namespace, service string) bool {
	if ns == nil || ns.Labels == nil {
		return false
	}
	if ns.Labels[managedByLabel] != ManagedByValue {
		return false
	}
	return service == "" || ns.Labels[serviceLabel] == service
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
			return false, nil // already present; adopted, not created by us
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
// namespaces. An empty service matches every emulator-managed namespace.
func namespaceSelector(service string) string {
	sel := managedByLabel + "=" + ManagedByValue
	if service != "" {
		sel += "," + serviceLabel + "=" + service
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
// serviceAccount/serviceAccountNamespace to use the defaults. It is idempotent
// and best-effort against AlreadyExists; a Forbidden create returns
// ErrNamespaceForbidden so the caller can fall back.
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
		ObjectMeta: metav1.ObjectMeta{Name: ExecutorRoleBindingName, Namespace: namespace},
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
