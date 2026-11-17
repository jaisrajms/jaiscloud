package managedkafka

import "context"

// Name identifies the Managed Kafka hook in POST /_jaiscloud/park|resume.
func (s *Service) Name() string { return "managedkafka" }

// clusterParker is implemented by broker managers that can terminate a broker's
// workloads while keeping its namespace (the k8s broker). Managers that only
// know how to stop-and-reap fall back to StopCluster.
type clusterParker interface {
	ParkCluster(ctx context.Context, project, location, cluster string) error
}

// Park terminates every cluster's broker workloads but keeps all cluster and
// topic metadata. Broker topics are lost (the broker has no PVC), exactly like
// a stopped container: the metadata is the source of truth on resume.
func (s *Service) Park(ctx context.Context, project string) (map[string]any, error) {
	clusters, err := s.ListAllClusters(ctx, project)
	if err != nil {
		return nil, err
	}
	stopped := 0
	for _, c := range clusters {
		if s.broker != nil {
			if pb, ok := s.broker.(clusterParker); ok {
				err = pb.ParkCluster(ctx, c.ProjectID, c.Location, c.Name)
			} else {
				err = s.broker.StopCluster(ctx, c.ProjectID, c.Location, c.Name)
			}
			if err != nil {
				return map[string]any{"brokersStopped": stopped}, err
			}
			stopped++
		}
	}
	return map[string]any{"brokersStopped": stopped}, nil
}

// Resume recreates every cluster's broker. It is idempotent: ensureBroker
// reuses a live broker, and a cluster without a broker manager (mock topology)
// stays metadata-only.
func (s *Service) Resume(ctx context.Context, project string) error {
	clusters, err := s.ListAllClusters(ctx, project)
	if err != nil {
		return err
	}
	for _, c := range clusters {
		s.ensureBroker(ctx, c.ProjectID, c.Location, c.Name)
	}
	return nil
}
