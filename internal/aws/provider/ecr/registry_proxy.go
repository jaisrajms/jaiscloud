package ecr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	ecrstore "jaiscloud/internal/aws/store/ecr"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/k8shelpers"
)

const (
	registryImage = "registry:2.8"
	registryPort  = 5000
	registryName  = "registry"
	registryLabel = "jaiscloud-registry"
)

// RegistryProxy manages a real Docker registry:2 Deployment in K8s and
// proxies OCI Distribution v2 requests to it.
type RegistryProxy struct {
	k8sClient  kubernetes.Interface
	namespace  string
	serviceDNS string // registry.<ns>.svc.cluster.local:5000
	httpClient *http.Client
	store      *ecrstore.MemoryECRStore
}

// NewRegistryProxy creates a RegistryProxy. Call Start to provision the K8s resources.
func NewRegistryProxy(k8sClient kubernetes.Interface, namespace string, store *ecrstore.MemoryECRStore) *RegistryProxy {
	return &RegistryProxy{
		k8sClient:  k8sClient,
		namespace:  namespace,
		serviceDNS: fmt.Sprintf("%s.%s.svc.cluster.local:%d", registryName, namespace, registryPort),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		store:      store,
	}
}

// Start provisions the registry:2 Deployment and Service if they don't exist,
// then waits up to 120s for the registry to be ready.
func (p *RegistryProxy) Start(ctx context.Context) error {
	if err := p.ensureNamespace(ctx); err != nil {
		return fmt.Errorf("ecr registry: ensure namespace: %w", err)
	}
	if err := p.ensureDeployment(ctx); err != nil {
		return fmt.Errorf("ecr registry: ensure deployment: %w", err)
	}
	if err := p.ensureService(ctx); err != nil {
		return fmt.Errorf("ecr registry: ensure service: %w", err)
	}
	slog.Info("ecr: waiting for registry:2 to be ready", "dns", p.serviceDNS)
	return p.waitReady(ctx, 120*time.Second)
}

// Proxy forwards an OCI Distribution v2 request to the real registry.
func (p *RegistryProxy) Proxy(w http.ResponseWriter, r *http.Request) {
	targetURL := fmt.Sprintf("http://%s%s", p.serviceDNS, r.URL.Path)
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for k, v := range r.Header {
		if k == "Authorization" {
			continue // strip incoming auth — registry runs with no auth
		}
		proxyReq.Header[k] = v
	}

	resp, err := p.httpClient.Do(proxyReq)
	if err != nil {
		slog.Warn("ecr: registry proxy error", "err", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, `{"errors":[{"code":"UNAVAILABLE","message":"registry unavailable: %s"}]}`, err.Error())
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body) //nolint:errcheck
}

// Healthy returns true if the registry is accepting connections.
func (p *RegistryProxy) Healthy() bool {
	resp, err := p.httpClient.Get(fmt.Sprintf("http://%s/v2/", p.serviceDNS))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized
}

// ResetAllImages deletes all manifests from the real registry for all repos
// in the lite-mode metadata store. Called during /_jaiscloud/reset.
func (p *RegistryProxy) ResetAllImages(ctx context.Context) {
	repos := p.store.ListRepositories("", "")
	for _, repo := range repos {
		for digest := range repo.Images {
			url := fmt.Sprintf("http://%s/v2/%s/manifests/%s", p.serviceDNS, repo.Name, digest)
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
			if err != nil {
				continue
			}
			resp, err := p.httpClient.Do(req)
			if err != nil {
				continue
			}
			resp.Body.Close()
		}
	}
}

// ─── K8s resource management ─────────────────────────────────────────────────

func (p *RegistryProxy) ensureNamespace(ctx context.Context) error {
	_, err := p.k8sClient.CoreV1().Namespaces().Get(ctx, p.namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: p.namespace,
			// The registry proxy creates the process namespace when it is
			// missing; the ownership/identity labels are what the dual-substrate
			// admission policy requires from a namespace-created-by-a-substrate
			// request (and what keeps the sweep from mistaking it for data).
			Labels: k8shelpers.OwnerNamespaceLabels(""),
		},
	}
	_, err = p.k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (p *RegistryProxy) ensureDeployment(ctx context.Context) error {
	_, err := p.k8sClient.AppsV1().Deployments(p.namespace).Get(ctx, registryName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	deploy := p.buildDeployment()
	_, err = p.k8sClient.AppsV1().Deployments(p.namespace).Create(ctx, deploy, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (p *RegistryProxy) ensureService(ctx context.Context) error {
	_, err := p.k8sClient.CoreV1().Services(p.namespace).Get(ctx, registryName, metav1.GetOptions{})
	if err == nil {
		return nil // already exists
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	svc := p.buildService()
	_, err = p.k8sClient.CoreV1().Services(p.namespace).Create(ctx, svc, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (p *RegistryProxy) buildDeployment() *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      registryName,
			Namespace: p.namespace,
			Labels:    map[string]string{"app": registryLabel, "managed-by": "jaiscloud"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": registryLabel},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": registryLabel},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  registryName,
						Image: registryImage,
						Ports: []corev1.ContainerPort{{ContainerPort: registryPort}},
						Env: []corev1.EnvVar{
							{Name: "REGISTRY_STORAGE_DELETE_ENABLED", Value: "true"},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/v2/",
									Port: intstr.FromInt(registryPort),
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       3,
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/registry"},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					}},
				},
			},
		},
	}
}

func (p *RegistryProxy) buildService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      registryName,
			Namespace: p.namespace,
			Labels:    map[string]string{"app": registryLabel, "managed-by": "jaiscloud"},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": registryLabel},
			Ports: []corev1.ServicePort{{
				Port:       registryPort,
				TargetPort: intstr.FromInt(registryPort),
			}},
		},
	}
}

func (p *RegistryProxy) waitReady(ctx context.Context, timeout time.Duration) error {
	// healthy. Must use actual elapsed time, not the simulated clock.
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if p.Healthy() {
			slog.Info("ecr: registry:2 ready", "dns", p.serviceDNS)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return errors.New("ecr: registry:2 not ready within timeout")
}
