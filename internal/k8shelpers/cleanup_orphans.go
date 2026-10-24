package k8shelpers

import (
	"context"
	"log/slog"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CleanupConfig declares what to reap on startup.
type CleanupConfig struct {
	Namespace string

	// Namespaces, when non-empty, are swept in addition to Namespace. It backs
	// engines with one namespace per resource: resolve the owned namespaces with
	// ListManagedNamespaces and pass them here. Duplicates and empty entries are
	// ignored.
	Namespaces []string

	// InstanceID filters Jobs to only those owned by this JaisCloud instance.
	// Jobs labeled with a different InstanceID are skipped.
	InstanceID string

	// OrphanSelectors is a set of additional label selectors. Providers
	// register their framework-pod selectors here (e.g.,
	// "spark-role in (driver,executor)") so the sweep finds pods that
	// don't carry the managed-by label.
	OrphanSelectors []string

	// OnTerminalJob is invoked for each terminal k8s Job found.
	OnTerminalJob func(jobName string, state string, reason string)

	// OnUnownedPod is invoked for each orphan pod found by the extended
	// selectors. Provider decides whether to delete or attempt late ownership
	// patching. Returns true to delete the pod.
	OnUnownedPod func(pod *corev1.Pod) (delete bool)
}

// CleanupOrphans runs a startup sweep across every configured namespace over:
//  1. batchv1.Jobs matching app.kubernetes.io/managed-by=jaiscloud.
//     Terminal Jobs invoke OnTerminalJob and are deleted.
//     Suspended Jobs are unsuspended (re-adopted).
//  2. Each cfg.OrphanSelectors — for pods matching each selector with
//     empty OwnerReferences, OnUnownedPod is called.
func CleanupOrphans(ctx context.Context, k8s kubernetes.Interface, cfg CleanupConfig) error {
	for _, ns := range uniqueNamespaces(cfg.Namespace, cfg.Namespaces) {
		if err := sweepJobs(ctx, k8s, cfg, ns); err != nil {
			slog.Warn("k8shelpers: CleanupOrphans job sweep error", "namespace", ns, "err", err)
		}
		for _, sel := range cfg.OrphanSelectors {
			if err := sweepOrphanPods(ctx, k8s, cfg, ns, sel); err != nil {
				slog.Warn("k8shelpers: CleanupOrphans pod sweep error", "namespace", ns, "selector", sel, "err", err)
			}
		}
	}
	return nil
}

func sweepJobs(ctx context.Context, k8s kubernetes.Interface, cfg CleanupConfig, namespace string) error {
	sel := "app.kubernetes.io/managed-by=jaiscloud"
	if cfg.InstanceID != "" {
		sel += ",jaiscloud.io/instance-id=" + cfg.InstanceID
	}

	var continueToken string
	for {
		list, err := k8s.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: sel,
			Limit:         500,
			Continue:      continueToken,
		})
		if err != nil {
			return err
		}

		for i := range list.Items {
			job := &list.Items[i]

			// Check terminal state.
			if isJobTerminal(job) {
				reason := jobFailureReason(job)
				state := "COMPLETED"
				if job.Status.Failed > 0 {
					state = "FAILED"
				}
				if cfg.OnTerminalJob != nil {
					cfg.OnTerminalJob(job.Name, state, reason)
				}
				propagation := metav1.DeletePropagationForeground
				_ = k8s.BatchV1().Jobs(namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
					PropagationPolicy: &propagation,
				})
				continue
			}

			// Unsuspend suspended jobs.
			if job.Spec.Suspend != nil && *job.Spec.Suspend {
				suspended := false
				patch := job.DeepCopy()
				patch.Spec.Suspend = &suspended
				_, err := k8s.BatchV1().Jobs(namespace).Update(ctx, patch, metav1.UpdateOptions{})
				if err != nil {
					slog.Warn("k8shelpers: failed to unsuspend job", "job", job.Name, "err", err)
				} else {
					slog.Info("k8shelpers: unsuspended adopted job", "job", job.Name)
				}
			}
		}

		continueToken = list.Continue
		if continueToken == "" {
			break
		}
	}
	return nil
}

func sweepOrphanPods(ctx context.Context, k8s kubernetes.Interface, cfg CleanupConfig, namespace, selector string) error {
	pods, err := k8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if len(pod.OwnerReferences) > 0 {
			continue
		}
		shouldDelete := true
		if cfg.OnUnownedPod != nil {
			shouldDelete = cfg.OnUnownedPod(pod)
		}
		if shouldDelete {
			_ = k8s.CoreV1().Pods(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			slog.Info("k8shelpers: deleted orphan pod", "pod", pod.Name, "namespace", namespace, "selector", selector)
		}
	}
	return nil
}

func isJobTerminal(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == "Complete" || c.Type == "Failed") && c.Status == "True" {
			return true
		}
	}
	return false
}

func jobFailureReason(job *batchv1.Job) string {
	for _, c := range job.Status.Conditions {
		if c.Type == "Failed" && c.Status == "True" {
			return c.Message
		}
	}
	return ""
}
