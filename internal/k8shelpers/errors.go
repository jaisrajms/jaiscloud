package k8shelpers

import "errors"

var (
	ErrJobNotFound        = errors.New("k8shelpers: job not found")
	ErrPodNotScheduled    = errors.New("k8shelpers: pod not yet scheduled")
	ErrUnknownOptOutToken = errors.New("k8shelpers: unknown platform-overlay opt-out token")
	// ErrNamespaceForbidden means the ServiceAccount cannot Get/Create/Delete a
	// cluster-scoped Namespace. Callers fall back to the process-wide namespace.
	ErrNamespaceForbidden = errors.New("k8shelpers: namespace operation forbidden")
)
