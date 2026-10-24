package k8shelpers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/clock"
)

const (
	// DefaultWorkloadReadyTimeout bounds how long EnsureWorkload waits for the
	// ClusterIP Service to accept a TCP connection.
	DefaultWorkloadReadyTimeout = 180 * time.Second
	// defaultWorkloadSweepTimeout bounds a SweepWorkloads call.
	defaultWorkloadSweepTimeout = 30 * time.Second
	// workloadListLimit is the page size for a sweep list.
	workloadListLimit = 500
	// defaultWorkloadSweepCap bounds a sweep so a mislabelled namespace cannot
	// trigger unbounded deletes.
	defaultWorkloadSweepCap = 2000
)

// ServicePort describes one ClusterIP Service port for a workload. TargetPort
// defaults to Port when zero.
type ServicePort struct {
	Name       string
	Port       int32
	TargetPort int32
}

// WorkloadSpec describes a single-replica Pod plus a ClusterIP Service that
// fronts it. EnsureWorkload creates both idempotently and waits for the
// in-cluster endpoint to accept connections. It is the spec-agnostic lifecycle
// shared by Managed Kafka brokers and Cloud Run revisions; cloud-specific
// policy (image, env, labels, ports, restart policy) stays with the caller.
type WorkloadSpec struct {
	// Namespace is required; it is created best-effort (a Forbidden Get means
	// the namespace is managed externally and is not an error).
	Namespace string
	// Pod is the caller-built Pod template. ObjectMeta.Name is required. The
	// helper sets Namespace and merges Labels onto the stored pod.
	Pod corev1.Pod
	// ServiceLabels and Selector default to Pod.Labels. Selector should be the
	// minimal stable subset that matches the Pod.
	ServiceLabels map[string]string
	Selector      map[string]string
	// Ports are the ClusterIP Service ports. The first port determines the
	// endpoint returned by EnsureWorkload.
	Ports []ServicePort
	// ReadyTimeout bounds the readiness wait; zero uses DefaultWorkloadReadyTimeout.
	ReadyTimeout time.Duration
	// Probe reports whether addr accepts connections; nil uses a one-second TCP
	// dial. Injected by tests.
	Probe func(addr string) bool
}

// EnsureWorkload creates the namespace (best-effort), the ClusterIP Service and
// the Pod for spec, then waits for the Service's in-cluster TCP endpoint to
// accept a connection. It returns "<pod>.<ns>.svc.cluster.local:<port>". On any
// failure after creation begins, the Pod/Service it created are deleted so a
// failed start cannot leak resources.
func EnsureWorkload(ctx context.Context, client kubernetes.Interface, spec WorkloadSpec) (string, error) {
	name := spec.Pod.Name
	if name == "" {
		return "", fmt.Errorf("k8shelpers: workload pod name is required")
	}
	if spec.Namespace == "" {
		return "", fmt.Errorf("k8shelpers: workload %s namespace is required", name)
	}
	if len(spec.Ports) == 0 {
		return "", fmt.Errorf("k8shelpers: workload %s has no ports", name)
	}
	if err := EnsureNamespace(ctx, client, spec.Namespace, map[string]string{"managed-by": "jaiscloud"}); err != nil {
		return "", fmt.Errorf("k8shelpers: workload %s: ensure namespace: %w", name, err)
	}
	if err := ensureWorkloadService(ctx, client, spec, name); err != nil {
		_ = DeleteWorkload(context.WithoutCancel(ctx), client, spec.Namespace, name)
		return "", fmt.Errorf("k8shelpers: workload %s: ensure service: %w", name, err)
	}
	if err := ensureWorkloadPod(ctx, client, spec, name); err != nil {
		_ = DeleteWorkload(context.WithoutCancel(ctx), client, spec.Namespace, name)
		return "", fmt.Errorf("k8shelpers: workload %s: ensure pod: %w", name, err)
	}

	addr := WorkloadEndpoint(name, spec.Namespace, spec.Ports[0].Port)
	if err := waitWorkloadReady(ctx, spec, addr); err != nil {
		_ = DeleteWorkload(context.WithoutCancel(ctx), client, spec.Namespace, name)
		return "", err
	}
	return addr, nil
}

// WorkloadEndpoint returns the in-cluster DNS address of a workload Service.
func WorkloadEndpoint(name, namespace string, port int32) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, namespace, port)
}

// EnsureNamespace creates namespace when it is missing, tolerating Forbidden (a
// namespaced RBAC ServiceAccount cannot Get cluster-scoped namespaces; the
// namespace is managed externally in every deployment). A genuinely missing
// namespace surfaces on the subsequent Pod/Service create.
//
// It is the legacy single-namespace helper retained for EnsureWorkload and
// engines that have not migrated to per-resource namespaces; it delegates to
// the shared create primitive. New code should use EnsureManagedNamespace.
func EnsureNamespace(ctx context.Context, client kubernetes.Interface, namespace string, labels map[string]string) error {
	_, err := ensureNamespaceRaw(ctx, client, namespace, labels)
	if errors.Is(err, ErrNamespaceForbidden) {
		return nil
	}
	return err
}

func ensureWorkloadService(ctx context.Context, client kubernetes.Interface, spec WorkloadSpec, name string) error {
	if _, err := client.CoreV1().Services(spec.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return err
	}
	labels := spec.ServiceLabels
	if labels == nil {
		labels = spec.Pod.Labels
	}
	selector := spec.Selector
	if selector == nil {
		selector = spec.Pod.Labels
	}
	ports := make([]corev1.ServicePort, 0, len(spec.Ports))
	for _, p := range spec.Ports {
		target := p.TargetPort
		if target == 0 {
			target = p.Port
		}
		ports = append(ports, corev1.ServicePort{
			Name:       p.Name,
			Port:       p.Port,
			TargetPort: intstr.FromInt32(target),
		})
	}
	_, err := client.CoreV1().Services(spec.Namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: spec.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selector,
			Ports:    ports,
		},
	}, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func ensureWorkloadPod(ctx context.Context, client kubernetes.Interface, spec WorkloadSpec, name string) error {
	if _, err := client.CoreV1().Pods(spec.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return err
	}
	pod := spec.Pod.DeepCopy()
	pod.Namespace = spec.Namespace
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	_, err := client.CoreV1().Pods(spec.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func waitWorkloadReady(ctx context.Context, spec WorkloadSpec, addr string) error {
	probe := spec.Probe
	if probe == nil {
		probe = tcpDialProbe
	}
	timeout := spec.ReadyTimeout
	if timeout == 0 {
		timeout = DefaultWorkloadReadyTimeout
	}
	// Startup is real elapsed time, not the simulated clock.
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		if probe(addr) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("k8shelpers: workload %s not ready within %s", addr, timeout)
}

// DeleteWorkload deletes the named Pod and Service, ignoring NotFound. It
// returns the first non-NotFound error.
func DeleteWorkload(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	var firstErr error
	if err := client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		firstErr = fmt.Errorf("k8shelpers: delete pod %s: %w", name, err)
	}
	if err := client.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) && firstErr == nil {
		firstErr = fmt.Errorf("k8shelpers: delete service %s: %w", name, err)
	}
	return firstErr
}

// SweepWorkloads deletes every Pod and Service matching labelSelector in
// namespace and returns how many were deleted. It is best-effort: a list/delete
// failure stops that resource kind and is returned as err, but the other kind
// is still swept. The count is capped so a mislabelled namespace cannot trigger
// unbounded deletes.
func SweepWorkloads(ctx context.Context, client kubernetes.Interface, namespace, labelSelector string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultWorkloadSweepTimeout)
	defer cancel()

	deletedPods, err := sweepPods(ctx, client, namespace, labelSelector)
	deletedSvcs, svcErr := sweepServices(ctx, client, namespace, labelSelector)
	if err == nil {
		err = svcErr
	}
	return deletedPods + deletedSvcs, err
}

func sweepPods(ctx context.Context, client kubernetes.Interface, namespace, labelSelector string) (int, error) {
	deleted := 0
	var cont string
	for {
		list, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
			Limit:         workloadListLimit,
			Continue:      cont,
		})
		if err != nil {
			return deleted, err
		}
		for i := range list.Items {
			name := list.Items[i].Name
			if err := client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
				continue
			}
			deleted++
		}
		cont = list.Continue
		if cont == "" || deleted >= defaultWorkloadSweepCap {
			return deleted, nil
		}
	}
}

func sweepServices(ctx context.Context, client kubernetes.Interface, namespace, labelSelector string) (int, error) {
	deleted := 0
	var cont string
	for {
		list, err := client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
			Limit:         workloadListLimit,
			Continue:      cont,
		})
		if err != nil {
			return deleted, err
		}
		for i := range list.Items {
			name := list.Items[i].Name
			if err := client.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
				continue
			}
			deleted++
		}
		cont = list.Continue
		if cont == "" || deleted >= defaultWorkloadSweepCap {
			return deleted, nil
		}
	}
}

// tcpDialProbe reports whether addr accepts a TCP connection.
func tcpDialProbe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
