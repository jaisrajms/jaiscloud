package metastore

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// IAMLevel identifies which Dataproc Metastore resource an IAM policy is
// attached to. The REST Discovery serves getIamPolicy/setIamPolicy (and, for
// the service and federation levels, testIamPermissions) at each of these;
// gRPC serves all three levels through the shared google.iam.v1.IAMPolicy
// service (the pinned metastore proto defines no IAM RPCs).
type IAMLevel int

const (
	IAMService IAMLevel = iota
	IAMBackup
	IAMDatabase
	IAMTable
	IAMFederation
)

// String returns the level's resource token, used in error messages.
func (l IAMLevel) String() string {
	switch l {
	case IAMService:
		return "service"
	case IAMBackup:
		return "backup"
	case IAMDatabase:
		return "database"
	case IAMTable:
		return "table"
	case IAMFederation:
		return "federation"
	default:
		return "resource"
	}
}

// PolicyType is the shared ResourceStore type that holds the level's IAM policy.
// One type per level keeps policies for a service and its backups/databases
// distinct even when their relative ids could otherwise collide.
func (l IAMLevel) PolicyType() string {
	switch l {
	case IAMService:
		return "gcp_metastore_service_policy"
	case IAMBackup:
		return "gcp_metastore_backup_policy"
	case IAMDatabase:
		return "gcp_metastore_database_policy"
	case IAMTable:
		return "gcp_metastore_table_policy"
	case IAMFederation:
		return "gcp_metastore_federation_policy"
	default:
		return "gcp_metastore_policy"
	}
}

// IAMResource is a parsed metastore IAM target: the level and the parsed
// components needed both to build the policy store id and to check the owning
// resource's existence.
type IAMResource struct {
	Level      IAMLevel
	Project    string
	Location   string
	Service    string
	Backup     string
	Database   string
	Table      string
	Federation string
}

// Key is the ResourceStore id (relative to the project) under which the level's
// policy is stored. It mirrors the Eventarc pattern (the location-prefixed
// relative name) so the shared policy store is keyed consistently.
func (r IAMResource) Key() string {
	switch r.Level {
	case IAMService:
		return r.Location + "/" + r.Service
	case IAMBackup:
		return r.Location + "/" + r.Service + "/backups/" + r.Backup
	case IAMDatabase:
		return r.Location + "/" + r.Service + "/databases/" + r.Database
	case IAMTable:
		return r.Location + "/" + r.Service + "/databases/" + r.Database + "/tables/" + r.Table
	case IAMFederation:
		return r.Location + "/" + r.Federation
	default:
		return r.Location
	}
}

// ParseIAMResource parses a metastore resource name into its IAM target. ok is
// false when the name is not a metastore resource that carries an IAM surface.
// The most specific collection wins (a table name also parses a service and
// database; the table level is returned).
func ParseIAMResource(name string) (IAMResource, bool) {
	rn := ParseName(name)
	switch {
	case rn.Federation != "":
		return IAMResource{Level: IAMFederation, Project: rn.Project, Location: rn.Location, Federation: rn.Federation}, true
	case rn.Table != "":
		return IAMResource{Level: IAMTable, Project: rn.Project, Location: rn.Location, Service: rn.Service, Database: rn.Database, Table: rn.Table}, true
	case rn.Database != "":
		return IAMResource{Level: IAMDatabase, Project: rn.Project, Location: rn.Location, Service: rn.Service, Database: rn.Database}, true
	case rn.Backup != "":
		return IAMResource{Level: IAMBackup, Project: rn.Project, Location: rn.Location, Service: rn.Service, Backup: rn.Backup}, true
	case rn.Service != "":
		return IAMResource{Level: IAMService, Project: rn.Project, Location: rn.Location, Service: rn.Service}, true
	}
	return IAMResource{}, false
}

// InvalidIAMResource builds the canonical InvalidArgument error reported when a
// name is not a metastore resource with an IAM surface. It is exported so both
// transports report the same code.
func InvalidIAMResource() error {
	return model.NewProviderError("InvalidArgument", "invalid metastore IAM resource name", 400)
}

// WithResources wires the shared ResourceStore that backs the metadata-only IAM
// policy plane. Without it, IAM calls return InvalidArgument.
func WithResources(resources store.ResourceStore) Option {
	return func(s *Service) { s.resources = resources }
}

// requireIAMTarget verifies that the resource an IAM request names exists, for
// the levels the control plane models (service, backup, federation). Databases
// and tables are not modelled here — they live in the separate Hive Metastore
// Thrift plane (internal/gcp/hms) — so their IAM is metadata-only and keyed by
// name without an existence check (documented divergence; the emulator serves
// no per-service catalog).
func (s *Service) requireIAMTarget(ctx context.Context, r IAMResource) error {
	switch r.Level {
	case IAMService:
		if _, err := s.store.GetService(ctx, r.Project, r.Location, r.Service); err != nil {
			return mapErr(err)
		}
	case IAMBackup:
		if _, err := s.store.GetBackup(ctx, r.Project, r.Location, r.Service, r.Backup); err != nil {
			return mapErr(err)
		}
	case IAMFederation:
		if _, err := s.store.GetFederation(ctx, r.Project, r.Location, r.Federation); err != nil {
			return mapErr(err)
		}
	}
	return nil
}

// GetIamPolicy returns the stored IAM policy for a metastore resource name.
func (s *Service) GetIamPolicy(ctx context.Context, resource string) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, ok := ParseIAMResource(resource)
	if !ok {
		return policy.Policy{}, InvalidIAMResource()
	}
	if err := s.requireIAMTarget(ctx, r); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, r.Project, r.Level.PolicyType(), r.Key()), nil
}

// SetIamPolicy stores an IAM policy for a metastore resource name (etag OCC).
func (s *Service) SetIamPolicy(ctx context.Context, resource string, body map[string]any) (policy.Policy, error) {
	if s.resources == nil {
		return policy.Policy{}, InvalidIAMResource()
	}
	r, ok := ParseIAMResource(resource)
	if !ok {
		return policy.Policy{}, InvalidIAMResource()
	}
	if err := s.requireIAMTarget(ctx, r); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, r.Project, r.Level.PolicyType(), r.Key(), body)
}

// TestIamPermissions reports the requested permissions the caller holds. The
// emulator does not enforce IAM, so every permission is granted.
func (s *Service) TestIamPermissions(ctx context.Context, resource string, perms []string) ([]string, error) {
	if s.resources == nil {
		return nil, InvalidIAMResource()
	}
	r, ok := ParseIAMResource(resource)
	if !ok {
		return nil, InvalidIAMResource()
	}
	if err := s.requireIAMTarget(ctx, r); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}
