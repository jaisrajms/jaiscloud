package managedkafkaui

import (
	"encoding/json"
	"testing"
)

// The console bodies are mapped onto the core inputs here; these conversions are
// the parity-critical seam (e.g. the core reads topic overrides from the
// verbatim body's "configs" object, so the UI must re-wrap them).

func TestTopicInput_WrapsConfigs(t *testing.T) {
	in := topicInput(TopicWriteInput{
		PartitionCount:    3,
		ReplicationFactor: 2,
		Configs:           map[string]string{"retention.ms": "600000"},
	})
	if in.PartitionCount != 3 || in.ReplicationFactor != 2 {
		t.Fatalf("counts not carried: %+v", in)
	}
	var body struct {
		Configs map[string]string `json:"configs"`
	}
	if err := json.Unmarshal(in.Config, &body); err != nil {
		t.Fatalf("config not valid JSON: %v", err)
	}
	if body.Configs["retention.ms"] != "600000" {
		t.Fatalf("configs not wrapped in the verbatim body: %s", in.Config)
	}
}

func TestTopicInput_EmptyConfigsIsNil(t *testing.T) {
	if in := topicInput(TopicWriteInput{PartitionCount: 1}); in.Config != nil {
		t.Fatalf("empty configs must yield a nil body, got %s", in.Config)
	}
}

func TestClusterInput_CarriesLabelsAndConfigVerbatim(t *testing.T) {
	raw := json.RawMessage(`{"capacityConfig":{"vcpuCount":3}}`)
	in := clusterInput(ClusterWriteInput{Labels: map[string]string{"env": "dev"}, Config: raw})
	if in.Labels["env"] != "dev" {
		t.Fatalf("labels not carried: %+v", in)
	}
	if string(in.Config) != string(raw) {
		t.Fatalf("config not verbatim: %s", in.Config)
	}
}

func TestAclInput_MapsEtagAndEntries(t *testing.T) {
	in := aclInput(AclWriteInput{
		Etag: "etag-1",
		Entries: []AclEntry{
			{Principal: "User:alice", PermissionType: "ALLOW", Operation: "READ", Host: "*"},
		},
	})
	if in.Etag != "etag-1" || len(in.AclEntries) != 1 {
		t.Fatalf("acl input not mapped: %+v", in)
	}
	e := in.AclEntries[0]
	if e.Principal != "User:alice" || e.PermissionType != "ALLOW" || e.Operation != "READ" || e.Host != "*" {
		t.Fatalf("entry not mapped: %+v", e)
	}
}
