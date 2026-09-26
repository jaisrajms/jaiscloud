package managedkafka

import (
	"context"
	"encoding/json"
	"time"

	"jaiscloud/internal/clock"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// operationMetadataType is the @type of the Managed Kafka v1 OperationMetadata.
const operationMetadataType = "type.googleapis.com/google.cloud.managedkafka.v1.OperationMetadata"

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// ClusterJSON renders a stored Cluster as the Discovery cluster shape.
// capacityConfig, gcpConfig, rebalanceConfig, and tlsConfig are echoed from the
// stored config verbatim; the remaining fields are derived.
func ClusterJSON(c mkstore.Cluster, project string) map[string]any {
	out := map[string]any{
		"name":             ClusterName(project, c.Location, c.Name),
		"state":            "ACTIVE",
		"bootstrapAddress": BootstrapAddress(project, c.Location, c.Name),
		"createTime":       formatTimestamp(c.CreateTime),
		"updateTime":       formatTimestamp(c.UpdateTime),
	}
	var cfg map[string]any
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &cfg)
	}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := cfg[k].(map[string]any); ok {
			out[k] = v
		}
	}
	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// TopicJSON renders a stored Topic as the Discovery topic shape. configs is
// echoed from the stored config verbatim when present.
func TopicJSON(t mkstore.Topic, project string) map[string]any {
	out := map[string]any{
		"name":              TopicName(project, t.Location, t.ClusterName, t.Name),
		"partitionCount":    t.PartitionCount,
		"replicationFactor": t.ReplicationFactor,
	}
	var cfg map[string]any
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &cfg)
	}
	if v, ok := cfg["configs"].(map[string]any); ok {
		out["configs"] = v
	}
	return out
}

// AclEntryJSON renders a stored ACL entry.
func AclEntryJSON(e mkstore.AclEntry) map[string]any {
	return map[string]any{
		"principal":      e.Principal,
		"permissionType": e.PermissionType,
		"operation":      e.Operation,
		"host":           e.Host,
	}
}

// AclJSON renders a stored ACL as the Discovery acl shape.
func AclJSON(a mkstore.Acl, project string) map[string]any {
	entries := make([]any, 0, len(a.AclEntries))
	for _, e := range a.AclEntries {
		entries = append(entries, AclEntryJSON(e))
	}
	return map[string]any{
		"name":         AclName(project, a.Location, a.ClusterName, a.Name),
		"aclEntries":   entries,
		"etag":         a.Etag,
		"resourceType": a.ResourceType,
		"resourceName": a.ResourceName,
		"patternType":  a.PatternType,
	}
}

// OperationMetadataJSON renders the managedkafka.v1.OperationMetadata for an
// operation. The emulator completes every cluster mutation synchronously, so
// createTime and endTime are the same instant.
func OperationMetadataJSON(target, verb string, start time.Time) map[string]any {
	return map[string]any{
		"@type":      operationMetadataType,
		"createTime": formatTimestamp(start),
		"endTime":    formatTimestamp(start),
		"target":     target,
		"verb":       verb,
		"apiVersion": "v1",
	}
}

// OperationJSON renders a stored Operation as a google.longrunning.Operation.
// Metadata/Response are the canonical wire JSON persisted alongside it.
func OperationJSON(op mkstore.Operation, project string) map[string]any {
	name := OperationName(project, op.Location, op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// storeOperation persists a done google.longrunning.Operation and returns it.
// Marshalling metadata/response cannot fail for the plain maps this package
// builds, so a marshal error is ignored and surfaces as an empty JSON object on
// read-back.
func (s *Service) storeOperation(ctx context.Context, project, location, verb, target string, response map[string]any) (mkstore.Operation, error) {
	now := clock.Now().UTC()
	metaJSON, _ := json.Marshal(OperationMetadataJSON(target, verb, now))
	respJSON, _ := json.Marshal(response)
	op := mkstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Location:   location,
		Done:       true,
		Metadata:   string(metaJSON),
		Response:   string(respJSON),
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return mkstore.Operation{}, err
	}
	return op, nil
}
