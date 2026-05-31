package emroneksui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the EMR on EKS UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/virtual-clusters", h.ListVirtualClusters)
	r.Post("/virtual-clusters", h.CreateVirtualCluster)
	r.Get("/virtual-clusters/{id}", h.DescribeVirtualCluster)
	r.Delete("/virtual-clusters/{id}", h.DeleteVirtualCluster)
	r.Get("/virtual-clusters/{vcId}/jobs", h.ListJobRuns)
	r.Post("/virtual-clusters/{vcId}/jobs", h.StartJobRun)
	r.Get("/virtual-clusters/{vcId}/jobs/{jobId}", h.DescribeJobRun)
	r.Delete("/virtual-clusters/{vcId}/jobs/{jobId}", h.CancelJobRun)

	return r
}
