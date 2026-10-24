package main

import (
	"context"
	"fmt"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
)

// dataprocScenarios exercises the SDK's gax long-running-operation poller: a
// Dataproc cluster create returns an Operation whose Wait() polls
// operations.get until the LRO reports done.
func dataprocScenarios(r *runner, f *fixtures) {
	region := "us-central1"

	r.run("lro.dataproc_cluster", "OK", func(ctx context.Context) (string, string, error) {
		client, err := dataproc.NewClusterControllerClient(ctx, grpcOptions(r.cfg)...)
		if err != nil {
			return "", "", fmt.Errorf("new dataproc client: %w", err)
		}
		defer client.Close()
		name := rid(r.cfg, "sdk-tour-cluster")
		op, err := client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
			ProjectId: r.cfg.Project,
			Region:    region,
			Cluster: &dataprocpb.Cluster{
				ProjectId:   r.cfg.Project,
				ClusterName: name,
				Config: &dataprocpb.ClusterConfig{
					GceClusterConfig: &dataprocpb.GceClusterConfig{ZoneUri: "us-central1-a"},
					SoftwareConfig:   &dataprocpb.SoftwareConfig{ImageVersion: "2.2"},
				},
			},
		})
		if err != nil {
			return "", "", fmt.Errorf("create cluster: %w", err)
		}
		// The SDK's gax poller: Wait repeatedly polls operations.get.
		cluster, err := op.Wait(ctx)
		if err != nil {
			return "", "", fmt.Errorf("wait for create operation: %w", err)
		}
		state := cluster.GetStatus().GetState().String()
		if state != "RUNNING" {
			return "", "", fmt.Errorf("cluster state = %s, want RUNNING", state)
		}
		if delOp, err := client.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{
			ProjectId: r.cfg.Project, Region: region, ClusterName: name,
		}); err == nil {
			_ = delOp.Wait(ctx)
		}
		return state, "gax op.Wait() settled the LRO to " + state, nil
	})
}
