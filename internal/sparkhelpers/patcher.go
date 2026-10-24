package sparkhelpers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

// MakeExecutorOwnerResolver returns a k8shelpers.IdentityMutator-compatible callback
// for resolving Spark executor pod ownership back to their parent K8s Job.
//
// In Spark client mode, executor pods are created by the Spark driver (which runs
// inside the batch/v1 Job pod) and have no ownerReference. The resolver:
//  1. Reads the executor pod's spark-app-selector label.
//  2. Lists driver pods matching spark-app-id=<selector>.
//  3. Returns an OwnerRefHint pointing at the driver pod's owning Job.
func MakeExecutorOwnerResolver(k8s kubernetes.Interface, namespace string) func(*corev1.Pod) (*k8shelpers.OwnerRefHint, error) {
	return resolveExecutorOwner(k8s, func(*corev1.Pod) string { return namespace })
}

// MakeExecutorOwnerResolverPerPod is MakeExecutorOwnerResolver for engines that
// own one namespace per resource: it resolves the driver in the *executor pod's
// own namespace* rather than a fixed one, so a single patcher can watch many
// namespaces.
func MakeExecutorOwnerResolverPerPod(k8s kubernetes.Interface) func(*corev1.Pod) (*k8shelpers.OwnerRefHint, error) {
	return resolveExecutorOwner(k8s, func(pod *corev1.Pod) string { return pod.Namespace })
}

func resolveExecutorOwner(k8s kubernetes.Interface, namespaceOf func(*corev1.Pod) string) func(*corev1.Pod) (*k8shelpers.OwnerRefHint, error) {
	return func(pod *corev1.Pod) (*k8shelpers.OwnerRefHint, error) {
		appSelector := pod.Labels["spark-app-selector"]
		if appSelector == "" {
			return nil, nil
		}
		drivers, err := k8s.CoreV1().Pods(namespaceOf(pod)).List(
			context.Background(),
			metav1.ListOptions{LabelSelector: "spark-app-id=" + appSelector},
		)
		if err != nil || len(drivers.Items) == 0 {
			return nil, nil
		}
		for _, ref := range drivers.Items[0].OwnerReferences {
			if ref.Kind == "Job" {
				return &k8shelpers.OwnerRefHint{
					APIVersion: ref.APIVersion,
					Kind:       ref.Kind,
					Name:       ref.Name,
					UID:        ref.UID,
				}, nil
			}
		}
		return nil, nil
	}
}
