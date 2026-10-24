package dataproc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

// dataprocService is the ownership label value for Dataproc's per-cluster
// namespaces (k8shelpers.NamespaceName / EnsureManagedNamespace).
const dataprocService = "dataproc"

// namespaceTeardownTimeout bounds the best-effort namespace/workload teardown
// that runs when a cluster is deleted or the store is reset.
const namespaceTeardownTimeout = 30 * time.Second

// defaultNamespace returns the process-wide namespace used by clusters that have
// no per-cluster namespace (mock execution, or the RBAC fallback).
func (s *Service) defaultNamespace() string {
	if s.namespace != "" {
		return s.namespace
	}
	return "jaiscloud"
}

// KubernetesNamespaceFromVirtualClusterConfig extracts the caller-supplied
// kubernetesClusterConfig.kubernetesNamespace from a Dataproc-on-GKE
// VirtualClusterConfig ("" for a GCE-shaped or malformed config). It is
// exported so the console UI can render the requested namespace alongside the
// cluster's effective namespace without duplicating the parse.
func KubernetesNamespaceFromVirtualClusterConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var vcc struct {
		KubernetesClusterConfig struct {
			KubernetesNamespace string `json:"kubernetesNamespace"`
		} `json:"kubernetesClusterConfig"`
	}
	if err := json.Unmarshal(raw, &vcc); err != nil {
		return ""
	}
	return vcc.KubernetesClusterConfig.KubernetesNamespace
}

// resolveClusterNamespace resolves and provisions the effective workload
// namespace for a cluster: the caller's virtualClusterConfig
// kubernetesNamespace when supplied, else a deterministic derived name. It
// returns whether the emulator created the namespace (NamespaceOwned).
//
// Mock execution (no k8s client) returns ("", false): there is no placement to
// record. When the namespace cannot be created/managed (the ServiceAccount
// lacks cluster-scoped namespace RBAC, or another error) it falls back to the
// process-wide namespace with owned=false, so a job never hard-fails on
// namespace plumbing.
func (s *Service) resolveClusterNamespace(ctx context.Context, project, region, name string, in ClusterInput) (string, bool) {
	if s.k8sClient == nil {
		return "", false
	}
	candidate := KubernetesNamespaceFromVirtualClusterConfig(in.VirtualClusterConfig)
	if candidate == "" {
		candidate = k8shelpers.NamespaceName(dataprocService, project, name)
	}
	created, err := k8shelpers.EnsureManagedNamespace(ctx, s.k8sClient, candidate, dataprocService)
	if err != nil {
		slog.Warn("dataproc: cannot provision per-cluster namespace; falling back",
			"cluster", name, "namespace", candidate, "fallback", s.defaultNamespace(), "err", err)
		return s.defaultNamespace(), false
	}
	// Bind the identities that must act in the new namespace: the emulator's own
	// ServiceAccount (creates the driver Job/pod) and the Spark driver's
	// ServiceAccount (creates executor pods; "default" when unset).
	driverSA := s.serviceAccountName
	if driverSA == "" {
		driverSA = "default"
	}
	subjects := []k8shelpers.RBACSubject{
		{Name: k8shelpers.DefaultExecutorServiceAccount, Namespace: k8shelpers.DefaultExecutorServiceAccountNamespace},
		{Name: driverSA, Namespace: candidate},
	}
	if rbacErr := k8shelpers.EnsureNamespaceRBACForSubjects(ctx, s.k8sClient, candidate, subjects...); rbacErr != nil {
		slog.Warn("dataproc: cannot bootstrap executor RBAC in per-cluster namespace; falling back",
			"cluster", name, "namespace", candidate, "fallback", s.defaultNamespace(), "err", rbacErr)
		if created {
			if _, delErr := k8shelpers.DeleteManagedNamespace(ctx, s.k8sClient, candidate, dataprocService); delErr != nil {
				slog.Warn("dataproc: failed to roll back unused namespace", "namespace", candidate, "err", delErr)
			}
		}
		return s.defaultNamespace(), false
	}
	s.registerNamespacePatcher(candidate)
	return candidate, created
}

// rollbackNamespace deletes a namespace the emulator just created when the
// cluster record could not be persisted, so a failed create does not leak it.
func (s *Service) rollbackNamespace(ctx context.Context, namespace string, created bool) {
	if s.k8sClient == nil || !created || namespace == "" {
		return
	}
	if _, err := k8shelpers.DeleteManagedNamespace(ctx, s.k8sClient, namespace, dataprocService); err != nil {
		slog.Warn("dataproc: failed to roll back namespace after create failure", "namespace", namespace, "err", err)
	}
}

// registerNamespacePatcher starts an ownership patcher for one workload
// namespace so executor pods created there are adopted into the driver Job's
// owner references. It is idempotent; a failure is logged and retried for the
// next cluster in that namespace.
func (s *Service) registerNamespacePatcher(namespace string) {
	if s.k8sClient == nil || namespace == "" {
		return
	}
	s.nsPatchersMu.Lock()
	_, exists := s.nsPatchers[namespace]
	s.nsPatchersMu.Unlock()
	if exists {
		return
	}
	stop, err := k8shelpers.StartOwnershipPatcher(s.ctx, s.k8sClient, k8shelpers.PatcherConfig{
		Namespace:     namespace,
		LabelSelector: "spark-role=executor",
		ResolveOwner:  sparkhelpers.MakeExecutorOwnerResolverPerPod(s.k8sClient),
	})
	if err != nil {
		slog.Warn("dataproc: failed to start ownership patcher", "namespace", namespace, "err", err)
		return
	}
	s.nsPatchersMu.Lock()
	if _, raced := s.nsPatchers[namespace]; raced {
		s.nsPatchersMu.Unlock()
		stop()
		return
	}
	s.nsPatchers[namespace] = stop
	s.nsPatchersMu.Unlock()
}

// unregisterNamespacePatcher stops and forgets a namespace's ownership patcher.
func (s *Service) unregisterNamespacePatcher(namespace string) {
	s.nsPatchersMu.Lock()
	stop, ok := s.nsPatchers[namespace]
	if ok {
		delete(s.nsPatchers, namespace)
	}
	s.nsPatchersMu.Unlock()
	if ok {
		stop()
	}
}

// stopNamespacePatchers stops every namespace ownership patcher.
func (s *Service) stopNamespacePatchers() {
	s.nsPatchersMu.Lock()
	stops := make([]func(), 0, len(s.nsPatchers))
	for ns, stop := range s.nsPatchers {
		stops = append(stops, stop)
		delete(s.nsPatchers, ns)
	}
	s.nsPatchersMu.Unlock()
	for _, stop := range stops {
		stop()
	}
}

// dispatchClusterTeardown runs teardownClusterNamespace in the background so a
// cluster read (GetCluster/ListClusters) never blocks on namespace or container
// teardown. The goroutine is tracked by s.wg so Shutdown waits for in-flight
// teardowns.
func (s *Service) dispatchClusterTeardown(c dpstore.Cluster) {
	if s.mockMode() {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.teardownClusterNamespace(context.Background(), c)
	}()
}

// teardownClusterNamespace reaps a deleted cluster's drivers and, when the
// emulator owns the namespace, the namespace itself. It is backend-agnostic for
// the driver reap (k8s Jobs, or Docker containers) and k8s-specific for the
// namespace. A pre-existing adopted namespace is left in place. It is
// best-effort and bounded; the caller (advanceCluster) may run on a request
// context.
func (s *Service) teardownClusterNamespace(ctx context.Context, c dpstore.Cluster) {
	if s.mockMode() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), namespaceTeardownTimeout)
	defer cancel()

	// The executor owns backend-specific driver teardown: Docker sweeps this
	// cluster's containers; the k8s executor is a no-op here because the Jobs
	// are reaped (or the namespace deleted, cascading them) below.
	if s.executor != nil {
		s.executor.ReapCluster(ctx, c.ProjectID, c.Region, c.Name)
	}

	if s.k8sClient == nil || c.Namespace == "" {
		return
	}

	s.unregisterNamespacePatcher(c.Namespace)
	s.deleteClusterJobs(ctx, c)
	if !c.NamespaceOwned {
		slog.Info("dataproc: cluster namespace is not emulator-owned; workloads reaped, namespace left",
			"cluster", c.Name, "namespace", c.Namespace)
		return
	}
	deleted, err := k8shelpers.DeleteManagedNamespace(ctx, s.k8sClient, c.Namespace, dataprocService)
	if err != nil {
		slog.Warn("dataproc: failed to delete cluster namespace", "cluster", c.Name, "namespace", c.Namespace, "err", err)
		return
	}
	if deleted {
		slog.Info("dataproc: deleted cluster namespace", "cluster", c.Name, "namespace", c.Namespace)
	}
}

// deleteClusterJobs deletes this cluster's k8s Jobs; deleting a Job cascades its
// driver pod and executor-template ConfigMap.
func (s *Service) deleteClusterJobs(ctx context.Context, c dpstore.Cluster) {
	sel := "jaiscloud.io/provider=dataproc,jaiscloud.io/cluster-name=" + c.Name
	jobs, err := s.k8sClient.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			slog.Warn("dataproc: failed to list cluster jobs for teardown", "cluster", c.Name, "err", err)
		}
		return
	}
	propagation := metav1.DeletePropagationForeground
	for i := range jobs.Items {
		name := jobs.Items[i].Name
		if err := s.k8sClient.BatchV1().Jobs(c.Namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !k8serrors.IsNotFound(err) {
			slog.Warn("dataproc: failed to delete cluster job", "cluster", c.Name, "job", name, "err", err)
		}
	}
}

// sweepOwnedNamespaces reaps every namespace the emulator owns for Dataproc
// (used by Reset, where the store is wiped wholesale).
func (s *Service) sweepOwnedNamespaces(ctx context.Context) {
	if s.k8sClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), namespaceTeardownTimeout)
	defer cancel()
	if _, err := k8shelpers.SweepManagedNamespaces(ctx, s.k8sClient, dataprocService); err != nil {
		slog.Warn("dataproc: failed to sweep owned namespaces", "err", err)
	}
}
