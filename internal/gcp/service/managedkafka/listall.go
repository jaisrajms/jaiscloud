package managedkafka

import (
	"context"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// ListAllClusters lists every cluster in a project across all locations,
// sorted by location then name. It backs the region-optional console list and,
// like ListClusters, resolves each cluster's live broker endpoint without
// starting a broker (a list read never provisions one).
func (s *Service) ListAllClusters(ctx context.Context, project string) ([]mkstore.Cluster, error) {
	clusters, err := s.store.ListClustersByProject(ctx, project)
	if err != nil {
		return nil, err
	}
	for i := range clusters {
		clusters[i].BootstrapAddress = s.brokerEndpoint(project, clusters[i].Location, clusters[i].Name)
	}
	return clusters, nil
}

// ListAllTopics lists every topic in a project across all locations and
// clusters, sorted by location, cluster, then name. It backs the
// region-optional console list.
func (s *Service) ListAllTopics(ctx context.Context, project string) ([]mkstore.Topic, error) {
	return s.store.ListTopicsByProject(ctx, project)
}
