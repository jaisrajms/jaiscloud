package sparkgcp

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/k8shelpers"
)

// BuildWorkloadIdentityMutator returns a k8shelpers.IdentityMutator wiring GCP
// Workload Identity:
//  1. Ensures the ServiceAccount exists in the target namespace (creates if missing).
//  2. Sets spec.serviceAccountName on the driver pod template (caller-wins).
//  3. Injects GOOGLE_CLOUD_PROJECT into the container env so the Spark driver
//     authenticates against the emulator project, not the cluster default.
func BuildWorkloadIdentityMutator(namespace, serviceAccount, projectID string) k8shelpers.IdentityMutator {
	return func(ctx context.Context, k8s kubernetes.Interface, tpl *corev1.PodTemplateSpec) error {
		if serviceAccount == "" && projectID == "" {
			return nil
		}

		// Caller-wins: if the pod template already names an SA, trust it.
		if tpl.Spec.ServiceAccountName == "" && serviceAccount != "" {
			tpl.Spec.ServiceAccountName = serviceAccount
		}

		saName := tpl.Spec.ServiceAccountName
		if saName == "" {
			saName = serviceAccount
		}
		if saName == "" {
			return nil
		}

		if err := ensureServiceAccount(ctx, k8s, namespace, saName); err != nil {
			return fmt.Errorf("sparkgcp: Workload Identity SA setup failed: %w", err)
		}
		if projectID != "" && len(tpl.Spec.Containers) > 0 {
			tpl.Spec.Containers[0].Env = append(tpl.Spec.Containers[0].Env, corev1.EnvVar{
				Name:  "GOOGLE_CLOUD_PROJECT",
				Value: projectID,
			})
		}
		return nil
	}
}

// ensureServiceAccount creates the SA if missing. Idempotent — a concurrent
// Create race is retried via Get. GCP Workload Identity binds a k8s SA to a
// Google service account by annotation/email, so no SA-level annotation is
// required here (unlike AWS IRSA); the driver pod only needs the SA to exist
// and the GOOGLE_CLOUD_PROJECT env to be set.
func ensureServiceAccount(ctx context.Context, k8s kubernetes.Interface, namespace, saName string) error {
	sas := k8s.CoreV1().ServiceAccounts(namespace)

	const maxRetries = 3
	for range maxRetries {
		_, err := sas.Get(ctx, saName, metav1.GetOptions{})
		if err == nil {
			return nil
		}
		if !k8serrors.IsNotFound(err) {
			return err
		}
		_, createErr := sas.Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
		}, metav1.CreateOptions{})
		if createErr == nil {
			return nil
		}
		if k8serrors.IsAlreadyExists(createErr) {
			continue // another caller created it concurrently — retry the Get
		}
		return createErr
	}
	return fmt.Errorf("sparkgcp: ensureServiceAccount: too many retries for SA %s/%s", namespace, saName)
}
