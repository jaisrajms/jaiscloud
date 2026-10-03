package managedkafka

import (
	"context"
	"encoding/json"
	"strconv"
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
// stored config verbatim; the remaining fields are derived. bootstrapAddress is
// the live broker endpoint when one is running (c.BootstrapAddress, set by the
// core from the broker manager), otherwise the synthesized cloud.goog name of
// the mock topology.
func ClusterJSON(c mkstore.Cluster, project string) map[string]any {
	addr := c.BootstrapAddress
	if addr == "" {
		addr = BootstrapAddress(project, c.Location, c.Name)
	}
	out := map[string]any{
		"name":             ClusterName(project, c.Location, c.Name),
		"state":            "ACTIVE",
		"bootstrapAddress": addr,
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

// ConsumerGroupJSON renders a consumer group as the Discovery consumerGroup
// shape: the resource name plus a topics map keyed by the full topic resource
// name, each holding a partitions map keyed by partition index. A group with no
// committed offsets omits the topics field.
func ConsumerGroupJSON(g ConsumerGroup, project string) map[string]any {
	out := map[string]any{"name": ConsumerGroupName(project, g.Location, g.Cluster, g.Name)}
	if len(g.Topics) == 0 {
		return out
	}
	topics := make(map[string]any, len(g.Topics))
	for topic, t := range g.Topics {
		partitions := make(map[string]any, len(t.Partitions))
		for p, meta := range t.Partitions {
			// ConsumerPartitionMetadata.offset is an int64 rendered as a JSON
			// string (Discovery: type string, format int64), and the official
			// REST client decodes it with the ,string struct tag.
			entry := map[string]any{"offset": strconv.FormatInt(meta.Offset, 10)}
			if meta.Metadata != "" {
				entry["metadata"] = meta.Metadata
			}
			partitions[strconv.Itoa(int(p))] = entry
		}
		topics[topic] = map[string]any{"partitions": partitions}
	}
	out["topics"] = topics
	return out
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
// Metadata/Response are the canonical wire JSON persisted alongside it. An
// in-flight operation omits both the response and metadata.endTime; a done
// operation keeps the original shape exactly.
func OperationJSON(op mkstore.Operation, project string) map[string]any {
	name := OperationName(project, op.Location, op.ID)
	metadata := map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done {
		// An async operation is persisted without an endTime; surface the
		// settled endTime here. A synchronously completed operation already
		// carries it, so it is left untouched and the output is unchanged.
		if _, ok := metadata["endTime"]; !ok {
			metadata["endTime"] = formatTimestamp(op.EndTime)
		}
		if op.Response != "" {
			var response any = map[string]any{}
			if json.Unmarshal([]byte(op.Response), &response) == nil {
				out["response"] = response
			}
		}
	} else {
		// Real GCP does not publish endTime until the operation completes.
		delete(metadata, "endTime")
	}
	return out
}

// storeOperation persists a google.longrunning.Operation and returns it. In the
// default synchronous mode it is stored done=true with EndTime=now, exactly as
// before. In async mode it is stored done=false with a zero EndTime and no
// metadata.endTime; the settle helper derives completion on read.
// Marshalling metadata/response cannot fail for the plain maps this package
// builds, so a marshal error is ignored and surfaces as an empty JSON object on
// read-back.
func (s *Service) storeOperation(ctx context.Context, project, location, verb, target string, response map[string]any) (mkstore.Operation, error) {
	now := clock.Now().UTC()
	meta := OperationMetadataJSON(target, verb, now)
	respJSON, _ := json.Marshal(response)
	op := mkstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Location:   location,
		Response:   string(respJSON),
		Verb:       verb,
		Target:     target,
		CreateTime: now,
	}
	if s.lroMode.Async() {
		delete(meta, "endTime")
	} else {
		op.Done = true
		op.EndTime = now
	}
	metaJSON, _ := json.Marshal(meta)
	op.Metadata = string(metaJSON)
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return mkstore.Operation{}, err
	}
	return op, nil
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation (done=false)
// becomes done once the delay has elapsed, with a deterministic EndTime of
// createTime+delay (a zero delay yields createTime).
//
// The flip is derived on read rather than written back: the managedkafka store
// has no UpdateOperation (adding one would be a store migration for no
// behavioral gain), and the persisted flag is an input while the settled state
// is a pure function of it and the clock. A store that later needs the settled
// flag visible to other readers can add the write without changing this
// contract.
func (s *Service) settle(op mkstore.Operation) mkstore.Operation {
	if op.Done || s.lroMode.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lroMode.Delay)
	return op
}
