// Package metastore is the transport-neutral core of the Dataproc Metastore v1
// control plane (metastore.googleapis.com): the Glue Data Catalog analogue.
// It owns all service/backup/metadata-import and long-running-operation
// business logic over internal/gcp/store/metastore.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/metastore) and the REST
// transport (internal/gcp/transport/rest/metastore) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// Create/Update/Delete return a done google.longrunning.Operation inline by
// default. The LRO timing is opt-in via WithLROMode: when enabled, operations
// are stored done=false and settle lazily on read (see settle); the default
// remains synchronous.
//
// This is control-plane only: the emulator never stands up a Hive Thrift /
// Iceberg metadata endpoint per service (the single global Hive Metastore
// Thrift plane in internal/gcp/hms is separate). Deferred operations
// (ExportMetadata, RestoreService, QueryMetadata, MoveTableToDatabase,
// AlterMetadataResourceLocation) are not modelled here; each transport reports
// them Unimplemented. The pinned proto defines no getIamPolicy/setIamPolicy/
// testIamPermissions rpcs; the REST Discovery documents those verbs but the
// emulator models no metastore IAM plane, so they fall through to an
// unsupported-operation 404 rather than being served.
package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	"jaiscloud/internal/gcp/paging"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

// Service is the transport-neutral Dataproc Metastore v1 service.
type Service struct {
	store metastorestore.Store
	// lroMode controls operation timing. The zero value is synchronous: every
	// operation is stored done=true inline, matching the v1.1.0 contract. An
	// enabled mode stores operations done=false and settles them lazily on read.
	lroMode lro.Mode
}

// Option configures Service.
type Option func(*Service)

// WithLROMode sets the long-running-operation timing mode. The zero value is
// synchronous; Mode{Enabled: true, Delay: d} stores create/update/delete
// operations done=false and settles them on read once d has elapsed.
func WithLROMode(m lro.Mode) Option {
	return func(s *Service) { s.lroMode = m }
}

// NewService returns a Dataproc Metastore core backed by the given store.
func NewService(s metastorestore.Store, opts ...Option) *Service {
	svc := &Service{store: s}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// Reset wipes the store.
func (s *Service) Reset(ctx context.Context) { s.store.Reset(ctx) }

// pageParams builds the shared cursor-pagination parameter map.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{}
	if pageSize > 0 {
		params["pageSize"] = pageSize
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// --- config decoding helpers ---

// decodeConfig unmarshals a stored/request JSON body into a map, returning nil
// (not an error) for an absent or non-object body.
func decodeConfig(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	return m
}

// labelsFromConfig extracts the top-level "labels" object, or nil when unset.
func labelsFromConfig(raw json.RawMessage) map[string]string {
	m := decodeConfig(raw)
	lm, _ := m["labels"].(map[string]any)
	if lm == nil {
		return nil
	}
	out := make(map[string]string, len(lm))
	for k, v := range lm {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// descriptionFromConfig extracts the top-level "description" field.
func descriptionFromConfig(raw json.RawMessage) string {
	m := decodeConfig(raw)
	s, _ := m["description"].(string)
	return s
}

// endpointProtocolFromConfig extracts hiveMetastoreConfig.endpointProtocol
// (camelCase on the wire) from a service body, or "" when unset.
func endpointProtocolFromConfig(raw json.RawMessage) string {
	m := decodeConfig(raw)
	hmsCfg, _ := m["hiveMetastoreConfig"].(map[string]any)
	if hmsCfg == nil {
		return ""
	}
	s, _ := hmsCfg["endpointProtocol"].(string)
	return s
}

// validateEndpointProtocol enforces the endpoint_protocol contract (D4/F7):
// THRIFT (or absent, the proto default) is accepted; GRPC is rejected with
// InvalidArgument (the per-service gRPC serving plane is deferred), and any
// other value is rejected as invalid rather than stored verbatim.
func validateEndpointProtocol(raw json.RawMessage) error {
	proto := strings.ToUpper(strings.TrimSpace(endpointProtocolFromConfig(raw)))
	switch proto {
	case "", "THRIFT":
		return nil
	case "GRPC":
		return model.NewProviderError("InvalidArgument", "endpointProtocol GRPC is not supported by the emulator (gRPC serving plane is deferred)", 400)
	default:
		return model.NewProviderError("InvalidArgument", "invalid endpointProtocol "+proto+": must be THRIFT or GRPC", 400)
	}
}

// --- Services ---

// CreateService creates a service and returns it with the done create operation.
func (s *Service) CreateService(ctx context.Context, project, location, serviceID string, cfg json.RawMessage) (metastorestore.Service, metastorestore.Operation, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Service{}, metastorestore.Operation{}, invalidArgument("missing location or serviceId")
	}
	// D4/F7: parse and validate hiveMetastoreConfig.endpointProtocol. THRIFT is
	// the default and the only served protocol; the per-service GRPC serving
	// plane is deferred, so it fails loud instead of advertising an endpoint
	// nothing listens on.
	if err := validateEndpointProtocol(cfg); err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, err
	}
	now := clock.Now().UTC()
	svc := metastorestore.Service{
		Location:     location,
		Name:         serviceID,
		Labels:       labelsFromConfig(cfg),
		State:        "ACTIVE",
		StateHistory: []metastorestore.ServiceState{{State: "CREATING", StateStartTime: now}},
		CreateTime:   now,
		UpdateTime:   now,
	}
	if len(cfg) > 0 {
		svc.Config = cfg
	}
	if err := s.store.CreateService(ctx, project, location, svc); err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, mapErr(err)
	}
	target := ServiceName(project, location, serviceID)
	op, err := s.storeOperation(ctx, project, location, "create", target, operationResponse(serviceTypeURL, ServiceJSON(svc, project)))
	if err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, mapErr(err)
	}
	return svc, op, nil
}

// GetService returns one service.
func (s *Service) GetService(ctx context.Context, project, location, serviceID string) (metastorestore.Service, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Service{}, invalidArgument("missing location or serviceId")
	}
	svc, err := s.store.GetService(ctx, project, location, serviceID)
	if err != nil {
		return metastorestore.Service{}, mapErr(err)
	}
	return svc, nil
}

// ListServices returns a cursor page of the services in a location.
func (s *Service) ListServices(ctx context.Context, project, location string, pageSize int, pageToken string) ([]metastorestore.Service, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	services, err := s.store.ListServices(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(services, func(svc metastorestore.Service) string { return svc.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateService merges the caller's fields into the stored service and returns
// it with the done update operation. The merge honors updateMask (comma-
// separated; empty means apply every field in the body).
func (s *Service) UpdateService(ctx context.Context, project, location, serviceID string, cfg json.RawMessage, mask string) (metastorestore.Service, metastorestore.Operation, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Service{}, metastorestore.Operation{}, invalidArgument("missing location or serviceId")
	}
	// The endpoint_protocol guard (D4/F7) applies to updates too: a service
	// created as THRIFT must not be patched to GRPC (or arbitrary junk).
	if err := validateEndpointProtocol(cfg); err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, err
	}
	updated, err := s.store.UpdateServiceAtomic(ctx, project, location, serviceID, func(current metastorestore.Service) (metastorestore.Service, error) {
		apply := func(field string) bool {
			return mask == "" || containsMaskField(mask, field)
		}
		if apply("labels") {
			if labels := labelsFromConfig(cfg); labels != nil {
				current.Labels = labels
			}
		}
		// Echo the remaining body fields into the stored config verbatim
		// (top-level keys overwrite), honoring the updateMask's named paths.
		if len(cfg) > 0 {
			stored := map[string]any{}
			if len(current.Config) > 0 {
				_ = json.Unmarshal(current.Config, &stored)
			}
			incoming := map[string]any{}
			_ = json.Unmarshal(cfg, &incoming)
			if mask == "" {
				for k, v := range incoming {
					stored[k] = v
				}
			} else {
				for _, field := range splitMask(mask) {
					root, ok := maskJSONRoot(field)
					if !ok {
						return metastorestore.Service{}, invalidArgument("unsupported updateMask field " + field)
					}
					if v, present := incoming[root]; present {
						stored[root] = v
					}
				}
			}
			if data, err := json.Marshal(stored); err == nil {
				current.Config = data
			}
		}
		current.UpdateTime = clock.Now().UTC()
		return current, nil
	})
	if err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, mapErr(err)
	}
	target := ServiceName(project, location, serviceID)
	op, err := s.storeOperation(ctx, project, location, "update", target, operationResponse(serviceTypeURL, ServiceJSON(updated, project)))
	if err != nil {
		return metastorestore.Service{}, metastorestore.Operation{}, err
	}
	return updated, op, nil
}

// DeleteService deletes a service (with its backups and imports) and returns
// the done delete operation.
func (s *Service) DeleteService(ctx context.Context, project, location, serviceID string) (metastorestore.Operation, error) {
	if location == "" || serviceID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location or serviceId")
	}
	if err := s.store.DeleteService(ctx, project, location, serviceID); err != nil {
		return metastorestore.Operation{}, mapErr(err)
	}
	target := ServiceName(project, location, serviceID)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}

// --- Backups ---

// CreateBackup creates a backup under an existing service.
func (s *Service) CreateBackup(ctx context.Context, project, location, serviceID, backupID string, cfg json.RawMessage) (metastorestore.Backup, metastorestore.Operation, error) {
	if location == "" || serviceID == "" || backupID == "" {
		return metastorestore.Backup{}, metastorestore.Operation{}, invalidArgument("missing location, serviceId, or backupId")
	}
	if _, err := s.store.GetService(ctx, project, location, serviceID); err != nil {
		return metastorestore.Backup{}, metastorestore.Operation{}, mapErr(err)
	}
	now := clock.Now().UTC()
	b := metastorestore.Backup{
		Location:    location,
		ServiceName: serviceID,
		Name:        backupID,
		Description: descriptionFromConfig(cfg),
		State:       "ACTIVE",
		CreateTime:  now,
		EndTime:     now,
	}
	if len(cfg) > 0 {
		b.Config = cfg
	}
	if err := s.store.CreateBackup(ctx, project, location, serviceID, b); err != nil {
		return metastorestore.Backup{}, metastorestore.Operation{}, mapErr(err)
	}
	target := BackupName(project, location, serviceID, backupID)
	op, err := s.storeOperation(ctx, project, location, "create", target, operationResponse(backupTypeURL, BackupJSON(b, project)))
	if err != nil {
		return metastorestore.Backup{}, metastorestore.Operation{}, err
	}
	return b, op, nil
}

// GetBackup returns one backup.
func (s *Service) GetBackup(ctx context.Context, project, location, serviceID, backupID string) (metastorestore.Backup, error) {
	if location == "" || serviceID == "" || backupID == "" {
		return metastorestore.Backup{}, invalidArgument("missing location, serviceId, or backupId")
	}
	b, err := s.store.GetBackup(ctx, project, location, serviceID, backupID)
	if err != nil {
		return metastorestore.Backup{}, mapErr(err)
	}
	return b, nil
}

// ListBackups returns a cursor page of the backups in a service.
func (s *Service) ListBackups(ctx context.Context, project, location, serviceID string, pageSize int, pageToken string) ([]metastorestore.Backup, string, error) {
	if location == "" || serviceID == "" {
		return nil, "", invalidArgument("missing location or serviceId")
	}
	if _, err := s.store.GetService(ctx, project, location, serviceID); err != nil {
		return nil, "", mapErr(err)
	}
	backups, err := s.store.ListBackups(ctx, project, location, serviceID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(backups, func(b metastorestore.Backup) string { return b.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// DeleteBackup deletes a backup and returns the done delete operation.
func (s *Service) DeleteBackup(ctx context.Context, project, location, serviceID, backupID string) (metastorestore.Operation, error) {
	if location == "" || serviceID == "" || backupID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location, serviceId, or backupId")
	}
	if err := s.store.DeleteBackup(ctx, project, location, serviceID, backupID); err != nil {
		return metastorestore.Operation{}, mapErr(err)
	}
	target := BackupName(project, location, serviceID, backupID)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}

// --- Metadata imports ---

// CreateMetadataImport creates a metadata import under an existing service.
func (s *Service) CreateMetadataImport(ctx context.Context, project, location, serviceID, importID string, cfg json.RawMessage) (metastorestore.MetadataImport, metastorestore.Operation, error) {
	if location == "" || serviceID == "" || importID == "" {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, invalidArgument("missing location, serviceId, or metadataImportId")
	}
	if _, err := s.store.GetService(ctx, project, location, serviceID); err != nil {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, mapErr(err)
	}
	now := clock.Now().UTC()
	mi := metastorestore.MetadataImport{
		Location:    location,
		ServiceName: serviceID,
		Name:        importID,
		Description: descriptionFromConfig(cfg),
		State:       "SUCCEEDED",
		CreateTime:  now,
		UpdateTime:  now,
		EndTime:     now,
	}
	if len(cfg) > 0 {
		mi.Config = cfg
	}
	if err := s.store.CreateMetadataImport(ctx, project, location, serviceID, mi); err != nil {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, mapErr(err)
	}
	target := MetadataImportName(project, location, serviceID, importID)
	op, err := s.storeOperation(ctx, project, location, "create", target, operationResponse(metadataImportTypeURL, MetadataImportJSON(mi, project)))
	if err != nil {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, err
	}
	return mi, op, nil
}

// GetMetadataImport returns one metadata import.
func (s *Service) GetMetadataImport(ctx context.Context, project, location, serviceID, importID string) (metastorestore.MetadataImport, error) {
	if location == "" || serviceID == "" || importID == "" {
		return metastorestore.MetadataImport{}, invalidArgument("missing location, serviceId, or metadataImportId")
	}
	mi, err := s.store.GetMetadataImport(ctx, project, location, serviceID, importID)
	if err != nil {
		return metastorestore.MetadataImport{}, mapErr(err)
	}
	return mi, nil
}

// ListMetadataImports returns a cursor page of the imports in a service.
func (s *Service) ListMetadataImports(ctx context.Context, project, location, serviceID string, pageSize int, pageToken string) ([]metastorestore.MetadataImport, string, error) {
	if location == "" || serviceID == "" {
		return nil, "", invalidArgument("missing location or serviceId")
	}
	if _, err := s.store.GetService(ctx, project, location, serviceID); err != nil {
		return nil, "", mapErr(err)
	}
	imports, err := s.store.ListMetadataImports(ctx, project, location, serviceID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(imports, func(mi metastorestore.MetadataImport) string { return mi.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateMetadataImport updates the description (only that field is mutable) and
// returns the import with the done update operation.
func (s *Service) UpdateMetadataImport(ctx context.Context, project, location, serviceID, importID string, cfg json.RawMessage, mask string) (metastorestore.MetadataImport, metastorestore.Operation, error) {
	if location == "" || serviceID == "" || importID == "" {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, invalidArgument("missing location, serviceId, or metadataImportId")
	}
	updated, err := s.store.UpdateMetadataImportAtomic(ctx, project, location, serviceID, importID, func(current metastorestore.MetadataImport) (metastorestore.MetadataImport, error) {
		if mask == "" || containsMaskField(mask, "description") {
			if desc, ok := decodeConfig(cfg)["description"].(string); ok {
				current.Description = desc
			}
		}
		// endTime records when the import finished and must not move on a
		// description-only patch; only updateTime is refreshed.
		current.UpdateTime = clock.Now().UTC()
		return current, nil
	})
	if err != nil {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, mapErr(err)
	}
	target := MetadataImportName(project, location, serviceID, importID)
	op, err := s.storeOperation(ctx, project, location, "update", target, operationResponse(metadataImportTypeURL, MetadataImportJSON(updated, project)))
	if err != nil {
		return metastorestore.MetadataImport{}, metastorestore.Operation{}, err
	}
	return updated, op, nil
}

// --- Operations ---

// GetOperation returns a persisted long-running operation.
func (s *Service) GetOperation(ctx context.Context, project, location, opID string) (metastorestore.Operation, error) {
	if location == "" || opID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location or operationId")
	}
	op, err := s.store.GetOperation(ctx, project, location, opID)
	if err != nil {
		return metastorestore.Operation{}, mapErr(err)
	}
	return s.settle(op), nil
}

// ListOperations returns a cursor page of the persisted operations for a
// location.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]metastorestore.Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(ops, func(op metastorestore.Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	for i := range page {
		page[i] = s.settle(page[i])
	}
	return page, next, nil
}

// IsNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation), so a transport's
// ResolveOperation can decline an unknown name instead of surfacing an error.
func IsNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}

// --- errors ---

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapErr maps a metastore store error onto a canonical provider error.
func mapErr(err error) error {
	switch {
	case errors.Is(err, metastorestore.ErrNoSuchService):
		return model.NewProviderError("NotFound", "service not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchBackup):
		return model.NewProviderError("NotFound", "backup not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchMetadataImport):
		return model.NewProviderError("NotFound", "metadata import not found", 404)
	case errors.Is(err, metastorestore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, metastorestore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	default:
		return err
	}
}

// containsMaskField reports whether a comma-separated updateMask contains the
// given top-level field (or a sub-path of it).
func containsMaskField(mask, field string) bool {
	for _, part := range splitMask(mask) {
		if part == field || strings.HasPrefix(part, field+".") {
			return true
		}
	}
	return false
}

func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// metastoreNestedBlocks maps the proto snake_case nested config block roots to
// their camelCase JSON wire keys. An updateMask deeper than top-level whose
// root is absent here (e.g. a misspelled snake_case field) fails loud with
// InvalidArgument rather than silently no-op-ing.
var metastoreNestedBlocks = map[string]string{
	"hive_metastore_config":        "hiveMetastoreConfig",
	"network_config":               "networkConfig",
	"encryption_config":            "encryptionConfig",
	"telemetry_config":             "telemetryConfig",
	"scaling_config":               "scalingConfig",
	"maintenance_window":           "maintenanceWindow",
	"metadata_management_activity": "metadataManagementActivity",
}

// maskJSONRoot translates an updateMask field path to the top-level JSON key in
// the stored body. Single-word top-level fields (labels, tier, network, port)
// and already-camelCase roots pass through unchanged; snake_case nested config
// blocks are translated via metastoreNestedBlocks. A snake_case root that is
// not a known nested block returns ok=false so the caller fails loud.
func maskJSONRoot(path string) (root string, ok bool) {
	root = path
	if i := strings.IndexByte(path, '.'); i >= 0 {
		root = path[:i]
	}
	if !strings.Contains(root, "_") {
		return root, true
	}
	camel, known := metastoreNestedBlocks[root]
	if !known {
		return "", false
	}
	return camel, true
}
