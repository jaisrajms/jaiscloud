// Package dataprocgke adapts the GKE (container) core to the Dataproc core's
// GKENodePoolProvisioner seam. Dataproc-on-GKE reconciliation has to read the
// target cluster and create/verify node pools through the GKE API, but the
// Dataproc core must not import service/container: main.go builds this adapter
// over the same GKE core instance the REST/gRPC transports serve, exactly as it
// wires the Metastore resolver.
package dataprocgke

import (
	"context"
	"errors"
	"strconv"
	"strings"

	containercore "jaiscloud/internal/gcp/service/container"
	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// Adapter implements dataproccore.GKENodePoolProvisioner over the GKE core.
type Adapter struct {
	core *containercore.Service
}

// New returns an adapter over the GKE core. A nil core yields an adapter whose
// calls fail, which is never wired (main.go only builds it when the GKE service
// is enabled).
func New(core *containercore.Service) *Adapter { return &Adapter{core: core} }

// ResolveGKETarget reads the target GKE cluster and reports the settings
// Dataproc validates it on.
func (a *Adapter) ResolveGKETarget(ctx context.Context, project, location, cluster string) (dataproccore.GKECluster, error) {
	c, err := a.core.GetCluster(ctx, project, location, cluster)
	if err != nil {
		return dataproccore.GKECluster{}, err
	}
	out := dataproccore.GKECluster{Project: project, Location: location, Name: c.Name}
	if c.Autopilot != nil {
		out.Autopilot = c.Autopilot.Enabled
	}
	if c.WorkloadIdentityConfig != nil {
		out.WorkloadIdentityPool = c.WorkloadIdentityConfig.WorkloadPool
	}
	return out, nil
}

// EnsureGKENodePool creates the requested pool when absent, verifies an existing
// one when the caller shaped it, and adopts it otherwise. Pools are never
// deleted: real Dataproc leaves them in place when the virtual cluster goes away.
func (a *Adapter) EnsureGKENodePool(ctx context.Context, cluster dataproccore.GKECluster, spec dataproccore.GKENodePoolSpec) (bool, error) {
	existing, err := a.core.GetNodePool(ctx, cluster.Project, cluster.Location, cluster.Name, spec.Name)
	switch {
	case err == nil:
		if !spec.Verify {
			return false, nil
		}
		if verr := verifyNodePool(spec, existing); verr != nil {
			return false, verr
		}
		return false, nil
	case !isNotFound(err):
		return false, err
	}
	if _, cerr := a.core.CreateNodePool(ctx, cluster.Project, cluster.Location, cluster.Name, nodePoolFromSpec(spec)); cerr != nil {
		return false, cerr
	}
	return true, nil
}

// nodePoolFromSpec materializes a pool in the shape Dataproc asks for, including
// the auto labels. It records exactly the manageable fields: there is no real
// node behind it. Fields the caller left empty take the service's create
// defaults (the documented machine type and the 1/10 autoscaler).
func nodePoolFromSpec(spec dataproccore.GKENodePoolSpec) containerstore.NodePool {
	machineType := spec.MachineType
	if machineType == "" {
		machineType = spec.CreateDefaults.MachineType
	}
	pool := containerstore.NodePool{
		Name:      spec.Name,
		Locations: spec.Locations,
		Config: &containerstore.NodeConfig{
			MachineType:    machineType,
			LocalSsdCount:  spec.LocalSsdCount,
			Preemptible:    spec.Preemptible,
			Spot:           spec.Spot,
			MinCpuPlatform: spec.MinCpuPlatform,
			ServiceAccount: spec.ServiceAccount,
			Labels:         spec.Labels,
		},
	}
	autoscaling := spec.Autoscaling
	if autoscaling == nil {
		autoscaling = spec.CreateDefaults.Autoscaling
	}
	if autoscaling != nil {
		pool.Autoscaling = &containerstore.NodePoolAutoscaling{
			Enabled:      true,
			MinNodeCount: autoscaling.MinNodeCount,
			MaxNodeCount: autoscaling.MaxNodeCount,
		}
		pool.InitialNodeCount = autoscaling.MinNodeCount
	}
	if pool.InitialNodeCount == 0 {
		pool.InitialNodeCount = 1
	}
	return pool
}

// verifyNodePool compares an existing pool with the caller's requested shape,
// field by field, and reports the first difference. Strings and lists are
// compared only when the request set them ("any node pool with the specified
// name is used" covers the rest); the proto3 scalars localSsdCount/preemptible/
// spot have no presence, so the caller's zero value is part of the requested
// shape and is always compared. Autoscaling is compared on enabled + bounds.
func verifyNodePool(spec dataproccore.GKENodePoolSpec, pool containerstore.NodePool) error {
	got := containerstore.NodeConfig{}
	if pool.Config != nil {
		got = *pool.Config
	}
	if spec.MachineType != "" && spec.MachineType != got.MachineType {
		return mismatch(spec, "machineType", spec.MachineType, got.MachineType)
	}
	if spec.LocalSsdCount != got.LocalSsdCount {
		return mismatch(spec, "localSsdCount", itoa(spec.LocalSsdCount), itoa(got.LocalSsdCount))
	}
	if spec.Preemptible != got.Preemptible {
		return mismatch(spec, "preemptible", strconv.FormatBool(spec.Preemptible), strconv.FormatBool(got.Preemptible))
	}
	if spec.Spot != got.Spot {
		return mismatch(spec, "spot", strconv.FormatBool(spec.Spot), strconv.FormatBool(got.Spot))
	}
	if spec.MinCpuPlatform != "" && spec.MinCpuPlatform != got.MinCpuPlatform {
		return mismatch(spec, "minCpuPlatform", spec.MinCpuPlatform, got.MinCpuPlatform)
	}
	if spec.ServiceAccount != "" && spec.ServiceAccount != got.ServiceAccount {
		return mismatch(spec, "serviceAccount", spec.ServiceAccount, got.ServiceAccount)
	}
	if spec.Autoscaling != nil {
		var min, max int32
		enabled := false
		if pool.Autoscaling != nil {
			enabled = pool.Autoscaling.Enabled
			min, max = pool.Autoscaling.MinNodeCount, pool.Autoscaling.MaxNodeCount
		}
		if !enabled || spec.Autoscaling.MinNodeCount != min || spec.Autoscaling.MaxNodeCount != max {
			return mismatch(spec, "autoscaling",
				"enabled "+itoa(spec.Autoscaling.MinNodeCount)+"/"+itoa(spec.Autoscaling.MaxNodeCount),
				"enabled="+strconv.FormatBool(enabled)+" "+itoa(min)+"/"+itoa(max))
		}
	}
	if len(spec.Locations) > 0 && strings.Join(spec.Locations, ",") != strings.Join(pool.Locations, ",") {
		return mismatch(spec, "locations", strings.Join(spec.Locations, ","), strings.Join(pool.Locations, ","))
	}
	return nil
}

func mismatch(spec dataproccore.GKENodePoolSpec, field, want, got string) error {
	return &dataproccore.GKENodePoolMismatchError{Pool: spec.Name, Field: field, Want: want, Got: got}
}

// isNotFound reports whether err is the GKE core's NotFound.
func isNotFound(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == "NotFound"
}

func itoa(v int32) string { return strconv.FormatInt(int64(v), 10) }
