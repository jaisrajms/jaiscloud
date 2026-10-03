package main

import (
	"encoding/json"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestAssignProbeRecords(t *testing.T) {
	tests := []struct {
		name       string
		records    int
		partitions int
		want       map[int32]int
	}{
		{"even spread", 12, 3, map[int32]int{0: 4, 1: 4, 2: 4}},
		{"one partition", 5, 1, map[int32]int{0: 5}},
		{"uneven spread", 5, 2, map[int32]int{0: 3, 1: 2}},
		{"one record", 1, 3, map[int32]int{0: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := assignProbeRecords(tt.records, tt.partitions)
			if len(got) != len(tt.want) {
				t.Fatalf("assignProbeRecords(%d, %d) = %v, want %v", tt.records, tt.partitions, got, tt.want)
			}
			total := 0
			for p, n := range tt.want {
				if got[p] != n {
					t.Errorf("partition %d count = %d, want %d", p, got[p], n)
				}
				total += n
			}
			if total != tt.records {
				t.Errorf("assigned %d records, want %d", total, tt.records)
			}
		})
	}
}

func TestSummarizeProbeOffsets(t *testing.T) {
	produced := map[int32]int{0: 4, 1: 4, 2: 4}
	end := kadm.ListedOffsets{
		"orders": {
			0: {Topic: "orders", Partition: 0, Offset: 4},
			1: {Topic: "orders", Partition: 1, Offset: 4},
			2: {Topic: "orders", Partition: 2, Offset: 4},
		},
	}
	committed := kadm.OffsetResponses{
		"orders": {
			0: {Offset: kadm.Offset{Topic: "orders", Partition: 0, At: 4}},
			1: {Offset: kadm.Offset{Topic: "orders", Partition: 1, At: 4}},
			2: {Offset: kadm.Offset{Topic: "orders", Partition: 2, At: 4}},
		},
	}

	gotCommitted, gotLag, err := summarizeProbeOffsets("orders", produced, end, committed)
	if err != nil {
		t.Fatalf("summarizeProbeOffsets: %v", err)
	}
	for partition := range produced {
		if gotCommitted[partition] != 4 {
			t.Errorf("partition %d committed = %d, want 4", partition, gotCommitted[partition])
		}
		if gotLag[partition] != 0 {
			t.Errorf("partition %d lag = %d, want 0", partition, gotLag[partition])
		}
	}
}

func TestSummarizeProbeOffsetsNoCommitIsZeroLagWhenEmpty(t *testing.T) {
	produced := map[int32]int{0: 2}
	end := kadm.ListedOffsets{"orders": {0: {Topic: "orders", Partition: 0, Offset: 2}}}
	// kadm reports -1 for a partition with no committed offset.
	committed := kadm.OffsetResponses{"orders": {0: {Offset: kadm.Offset{Topic: "orders", Partition: 0, At: -1}}}}

	gotCommitted, gotLag, err := summarizeProbeOffsets("orders", produced, end, committed)
	if err != nil {
		t.Fatalf("summarizeProbeOffsets: %v", err)
	}
	if gotCommitted[0] != 0 {
		t.Errorf("committed = %d, want 0 for an uncommitted partition", gotCommitted[0])
	}
	if gotLag[0] != 2 {
		t.Errorf("lag = %d, want 2", gotLag[0])
	}
}

func TestKafkaProbeSummaryJSONPartitionKeys(t *testing.T) {
	raw, err := json.Marshal(kafkaProbeSummary{
		OK:         true,
		Topic:      "orders",
		Group:      "g",
		Produced:   12,
		Consumed:   12,
		Partitions: map[int32]int{0: 4, 1: 4, 2: 4},
		Committed:  map[int32]int64{0: 4, 1: 4, 2: 4},
		Lag:        map[int32]int64{0: 0, 1: 0, 2: 0},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		ProbeOK    bool             `json:"probe_ok"`
		Partitions map[string]int   `json:"partitions"`
		Committed  map[string]int64 `json:"committed"`
		Lag        map[string]int64 `json:"lag"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !decoded.ProbeOK {
		t.Errorf("probe_ok = false, want true")
	}
	if decoded.Partitions["0"] != 4 || decoded.Committed["2"] != 4 || decoded.Lag["1"] != 0 {
		t.Errorf("decoded summary = %s, want partition keys rendered as strings", raw)
	}
}

func TestTopicOverridesFiltersDefaultsAndSynonyms(t *testing.T) {
	str := func(s string) *string { return &s }
	rc := kadm.ResourceConfig{Name: "orders", Configs: []kadm.Config{
		{Key: "cleanup.policy", Value: str("compact"), Source: kmsg.ConfigSourceDynamicTopicConfig},
		{Key: "retention.ms", Value: str("604800000"), Source: kmsg.ConfigSourceDefaultConfig},
		{Key: "segment.ms", Value: nil, Source: kmsg.ConfigSourceDynamicTopicConfig},
	}}
	got := topicOverrides(rc)
	if len(got) != 2 {
		t.Fatalf("topicOverrides = %v, want the two dynamic-topic entries", got)
	}
	if got["cleanup.policy"] != "compact" {
		t.Errorf("cleanup.policy = %q, want compact", got["cleanup.policy"])
	}
	if v, ok := got["segment.ms"]; !ok || v != "" {
		t.Errorf("segment.ms = %q (present %v), want empty present", v, ok)
	}
	if _, ok := got["retention.ms"]; ok {
		t.Errorf("default retention.ms leaked into overrides: %v", got)
	}

	if nilOverrides := topicOverrides(kadm.ResourceConfig{Name: "empty"}); nilOverrides != nil {
		t.Errorf("no dynamic configs = %v, want nil", nilOverrides)
	}
}
