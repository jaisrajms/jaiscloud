// Package container provides the Google Kubernetes Engine (GKE) v1 store.
// Clusters are project+location scoped with canonical names
// projects/{project}/locations/{location}/clusters/{cluster}; operations are the
// google.container.v1.Operation records under
// projects/{project}/locations/{location}/operations/{operation}.
//
// The emulator is metadata-only: a cluster is a stored record, not a schedulable
// Kubernetes control plane, and an operation is created already DONE (there is
// no asynchronous lifecycle to observe). The store persists the record the
// caller last observed so read-back is stable across a Postgres restart.
package container

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoSuchCluster is returned when a cluster name is unknown.
	ErrNoSuchCluster = errors.New("NoSuchCluster")
	// ErrNoSuchNodePool is returned when a node pool name is unknown.
	ErrNoSuchNodePool = errors.New("NoSuchNodePool")
	// ErrNoSuchOperation is returned when an operation name is unknown.
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	// ErrAlreadyExists is returned when creating a cluster/node pool that
	// already exists.
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Cluster status values (google.container.v1.Cluster.Status).
const (
	StatusRunning      = "RUNNING"
	StatusProvisioning = "PROVISIONING"
	StatusError        = "ERROR"
)

// Operation status values (google.container.v1.Operation.Status).
const (
	OperationStatusDone = "DONE"
)

// Operation type values (google.container.v1.Operation.Type). The proto enum
// has a dedicated value for each; setters whose enum value is documented
// "Unused" in the proto (setLabels/setMasterAuth/setNetworkPolicy/
// setMaintenancePolicy and the metadata-only addons/legacyAbac/locations/
// logging/monitoring setters) report UPDATE_CLUSTER, matching real GKE.
const (
	OperationCreateCluster         = "CREATE_CLUSTER"
	OperationDeleteCluster         = "DELETE_CLUSTER"
	OperationCreateNodePool        = "CREATE_NODE_POOL"
	OperationDeleteNodePool        = "DELETE_NODE_POOL"
	OperationUpdateNodePool        = "UPGRADE_NODES"
	OperationSetNodePoolSize       = "SET_NODE_POOL_SIZE"
	OperationSetNodePoolManagement = "SET_NODE_POOL_MANAGEMENT"
	OperationUpdateCluster         = "UPDATE_CLUSTER"
	OperationUpgradeMaster         = "UPGRADE_MASTER"
)

// NodePool is the persisted subset of a GKE node pool. Node pools are stored
// embedded in their cluster record (no separate table), so a node-pool mutation
// is a read-modify-write of the cluster document.
type NodePool struct {
	Name             string                   `json:"name"`
	Status           string                   `json:"status,omitempty"`
	InitialNodeCount int32                    `json:"initialNodeCount,omitempty"`
	NodeCount        int32                    `json:"nodeCount,omitempty"`
	Version          string                   `json:"version,omitempty"`
	Locations        []string                 `json:"locations,omitempty"`
	SelfLink         string                   `json:"selfLink,omitempty"`
	Config           *NodeConfig              `json:"config,omitempty"`
	Autoscaling      *NodePoolAutoscaling     `json:"autoscaling,omitempty"`
	Management       *NodeManagement          `json:"management,omitempty"`
	UpgradeSettings  *NodePoolUpgradeSettings `json:"upgradeSettings,omitempty"`
}

// NodeConfig is the persisted subset of google.container.v1.NodeConfig.
type NodeConfig struct {
	MachineType    string            `json:"machineType,omitempty"`
	DiskSizeGb     int32             `json:"diskSizeGb,omitempty"`
	DiskType       string            `json:"diskType,omitempty"`
	ImageType      string            `json:"imageType,omitempty"`
	OauthScopes    []string          `json:"oauthScopes,omitempty"`
	ServiceAccount string            `json:"serviceAccount,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`
	Tags           []string          `json:"tags,omitempty"`
	MinCpuPlatform string            `json:"minCpuPlatform,omitempty"`
	LocalSsdCount  int32             `json:"localSsdCount,omitempty"`
	Preemptible    bool              `json:"preemptible,omitempty"`
	Spot           bool              `json:"spot,omitempty"`
}

// NodePoolAutoscaling is the persisted subset of
// google.container.v1.NodePoolAutoscaling.
type NodePoolAutoscaling struct {
	Enabled           bool   `json:"enabled,omitempty"`
	MinNodeCount      int32  `json:"minNodeCount,omitempty"`
	MaxNodeCount      int32  `json:"maxNodeCount,omitempty"`
	Autoprovisioned   bool   `json:"autoprovisioned,omitempty"`
	LocationPolicy    string `json:"locationPolicy,omitempty"`
	TotalMinNodeCount int32  `json:"totalMinNodeCount,omitempty"`
	TotalMaxNodeCount int32  `json:"totalMaxNodeCount,omitempty"`
}

// NodeManagement is the persisted subset of google.container.v1.NodeManagement.
type NodeManagement struct {
	AutoUpgrade bool `json:"autoUpgrade,omitempty"`
	AutoRepair  bool `json:"autoRepair,omitempty"`
}

// NodePoolUpgradeSettings is the persisted subset of
// google.container.v1.NodePool.UpgradeSettings.
type NodePoolUpgradeSettings struct {
	MaxSurge       int32  `json:"maxSurge,omitempty"`
	MaxUnavailable int32  `json:"maxUnavailable,omitempty"`
	Strategy       string `json:"strategy,omitempty"`
}

// AddonConfig is the persisted subset of the boolean addon toggles
// (google.container.v1.HttpLoadBalancing and siblings).
type AddonConfig struct {
	Disabled bool `json:"disabled,omitempty"`
}

// AddonsConfig is the persisted subset of google.container.v1.AddonsConfig.
type AddonsConfig struct {
	HttpLoadBalancing        *AddonConfig `json:"httpLoadBalancing,omitempty"`
	HorizontalPodAutoscaling *AddonConfig `json:"horizontalPodAutoscaling,omitempty"`
	NetworkPolicyConfig      *AddonConfig `json:"networkPolicyConfig,omitempty"`
}

// LegacyAbac is the persisted subset of google.container.v1.LegacyAbac.
type LegacyAbac struct {
	Enabled bool `json:"enabled,omitempty"`
}

// NetworkPolicy is the persisted subset of google.container.v1.NetworkPolicy.
type NetworkPolicy struct {
	Provider string `json:"provider,omitempty"`
	Enabled  bool   `json:"enabled,omitempty"`
}

// MaintenancePolicy is the persisted subset of
// google.container.v1.MaintenancePolicy.
type MaintenancePolicy struct {
	ResourceVersion string             `json:"resourceVersion,omitempty"`
	Window          *MaintenanceWindow `json:"window,omitempty"`
}

// MaintenanceWindow is the persisted subset of
// google.container.v1.MaintenanceWindow.
type MaintenanceWindow struct {
	DailyMaintenanceWindow *DailyMaintenanceWindow `json:"dailyMaintenanceWindow,omitempty"`
	RecurringWindow        *RecurringWindow        `json:"recurringWindow,omitempty"`
	MaintenanceExclusions  map[string]*TimeWindow  `json:"maintenanceExclusions,omitempty"`
}

// DailyMaintenanceWindow is the persisted subset of
// google.container.v1.DailyMaintenanceWindow.
type DailyMaintenanceWindow struct {
	StartTime string `json:"startTime,omitempty"`
	Duration  string `json:"duration,omitempty"`
}

// RecurringWindow is the persisted subset of
// google.container.v1.RecurringTimeWindow.
type RecurringWindow struct {
	Window     *TimeWindow `json:"window,omitempty"`
	Recurrence string      `json:"recurrence,omitempty"`
}

// TimeWindow is the persisted subset of google.container.v1.TimeWindow.
type TimeWindow struct {
	StartTime string `json:"startTime,omitempty"`
	EndTime   string `json:"endTime,omitempty"`
}

// Cluster is the persisted subset of google.container.v1.Cluster. Name is the
// short cluster id (the last path segment); the canonical resource name is
// derived from ProjectID/Location/Name.
type Cluster struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Endpoint  string `json:"endpoint,omitempty"`
	// CaCertificate is the masterAuth.clusterCaCertificate value. The mock
	// mints no real CA, so this is the empty string.
	CaCertificate string `json:"caCertificate,omitempty"`

	CurrentMasterVersion  string `json:"currentMasterVersion,omitempty"`
	CurrentNodeVersion    string `json:"currentNodeVersion,omitempty"`
	InitialClusterVersion string `json:"initialClusterVersion,omitempty"`

	Network    string `json:"network,omitempty"`
	Subnetwork string `json:"subnetwork,omitempty"`

	// InitialNodeCount is the request's cluster-level initialNodeCount; the mock
	// materializes it as the default node pool's size.
	InitialNodeCount int32 `json:"initialNodeCount,omitempty"`

	NodePools      []NodePool        `json:"nodePools,omitempty"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`

	// Cluster-level recorded fields the Set*/update setters persist (no real
	// control plane; the values are echoed on read).
	AddonsConfig      *AddonsConfig      `json:"addonsConfig,omitempty"`
	LegacyAbac        *LegacyAbac        `json:"legacyAbac,omitempty"`
	LoggingService    string             `json:"loggingService,omitempty"`
	MonitoringService string             `json:"monitoringService,omitempty"`
	NetworkPolicy     *NetworkPolicy     `json:"networkPolicy,omitempty"`
	MaintenancePolicy *MaintenancePolicy `json:"maintenancePolicy,omitempty"`
	Locations         []string           `json:"locations,omitempty"`
	// AdminUsername is masterAuth.username (SetMasterAuth SET_USERNAME). The
	// admin password is never echoed, matching real GKE.
	AdminUsername string `json:"adminUsername,omitempty"`
	// IPRotationEnabled records StartIPRotation/CompleteIPRotation.
	IPRotationEnabled bool `json:"ipRotationEnabled,omitempty"`

	CreateTime time.Time `json:"createTime,omitempty"`
	SelfLink   string    `json:"selfLink,omitempty"`
}

// Operation is the persisted subset of google.container.v1.Operation. Name is
// the operation id ("operation-<uuid>"); the canonical resource name is derived
// from ProjectID/Location/Name. Note this is GKE's own Operation shape
// (operationType/status), not google.longrunning.Operation (done/response).
type Operation struct {
	ProjectID     string    `json:"projectId"`
	Location      string    `json:"location"`
	Name          string    `json:"name"`
	OperationType string    `json:"operationType"`
	Status        string    `json:"status"`
	TargetLink    string    `json:"targetLink,omitempty"`
	SelfLink      string    `json:"selfLink,omitempty"`
	StartTime     time.Time `json:"startTime,omitempty"`
	EndTime       time.Time `json:"endTime,omitempty"`
}

// Store is the GKE cluster/node-pool/operation store.
type Store interface {
	CreateCluster(ctx context.Context, projectID, location string, c Cluster) error
	GetCluster(ctx context.Context, projectID, location, name string) (Cluster, error)
	DeleteCluster(ctx context.Context, projectID, location, name string) error
	ListClusters(ctx context.Context, projectID, location string) ([]Cluster, error)

	// MutateCluster applies fn to the stored cluster and writes it back as one
	// atomic read-modify-write (memory: under its mutex; Postgres: a
	// SELECT … FOR UPDATE transaction). It returns ErrNoSuchCluster when the
	// cluster is unknown. Node-pool mutations run through it so both backends
	// (and the snapshot) stay consistent and concurrent mutations cannot lose an
	// update.
	MutateCluster(ctx context.Context, projectID, location, cluster string, fn func(*Cluster) error) error

	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, name string) (Operation, error)
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)

	// Reset clears all clusters and operations (/_jaiscloud/reset).
	Reset(ctx context.Context)
}
