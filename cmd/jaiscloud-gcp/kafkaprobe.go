package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kafkaProbeTimeout bounds the whole probe (connect + produce + consume +
// offset read) so a stuck broker cannot hang the k3d gate.
const kafkaProbeTimeout = 3 * time.Minute

// kafkaProbeSummary is the single JSON line the kafka-probe command prints on
// stdout. The real-Kafka k3d e2e gate
// (tests/persistent_mode/gcp/managedkafka-broker) parses it to assert that a
// real Kafka client produced and consumed across partitions and left committed
// offsets with zero lag. Partitions/Committed/Lag are keyed by partition index
// (JSON renders the integer keys as strings).
type kafkaProbeSummary struct {
	OK         bool            `json:"probe_ok"`
	Topic      string          `json:"topic"`
	Group      string          `json:"group"`
	Produced   int             `json:"produced"`
	Consumed   int             `json:"consumed"`
	Partitions map[int32]int   `json:"partitions"`
	Committed  map[int32]int64 `json:"committed"`
	Lag        map[int32]int64 `json:"lag"`
	// Configs are the topic's explicitly-overridden properties as the broker
	// reports them (Source=DYNAMIC_TOPIC_CONFIG). The k3d gate uses this to
	// prove the API's Topic.configs were applied to the real broker.
	Configs map[string]string `json:"configs,omitempty"`
}

// kafkaProbeConfig is the resolved kafka-probe invocation.
type kafkaProbeConfig struct {
	Brokers    string
	Topic      string
	Group      string
	Partitions int
	Records    int
}

// kafkaProbeCmd is a hidden test/debug tool: it drives a live managedkafka
// broker over the real Kafka wire protocol from inside the cluster (where the
// advertised <svc>.<ns>.svc.cluster.local listener resolves). It is not part of
// the emulator's user-facing surface.
func kafkaProbeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "kafka-probe",
		Short:  "Produce/consume against a live managedkafka broker (test tooling)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var cfg kafkaProbeConfig
			cfg.Brokers, _ = cmd.Flags().GetString("brokers")
			cfg.Topic, _ = cmd.Flags().GetString("topic")
			cfg.Group, _ = cmd.Flags().GetString("group")
			cfg.Partitions, _ = cmd.Flags().GetInt("partitions")
			cfg.Records, _ = cmd.Flags().GetInt("records")
			if cfg.Brokers == "" || cfg.Topic == "" || cfg.Group == "" {
				return errors.New("kafka-probe: --brokers, --topic and --group are required")
			}
			if cfg.Partitions < 1 || cfg.Records < 1 {
				return errors.New("kafka-probe: --partitions and --records must be >= 1")
			}
			ctx, cancel := context.WithTimeout(context.Background(), kafkaProbeTimeout)
			defer cancel()
			return runKafkaProbe(ctx, cfg)
		},
	}
	cmd.Flags().String("brokers", "", "Kafka bootstrap address (host:port)")
	cmd.Flags().String("topic", "", "Topic to produce to and consume from")
	cmd.Flags().String("group", "", "Consumer group id to join and commit under")
	cmd.Flags().Int("partitions", 1, "Number of partitions the topic has")
	cmd.Flags().Int("records", 1, "Number of records to produce and consume")
	return cmd
}

// runKafkaProbe produces cfg.Records records spread across cfg.Partitions,
// consumes them under cfg.Group from the earliest offset, commits, and reports
// the per-partition committed offsets and lag. It returns a non-nil error when
// any invariant (all records consumed, zero lag on every partition) does not
// hold, after printing the summary line, so a failing probe fails the k8s Job.
func runKafkaProbe(ctx context.Context, cfg kafkaProbeConfig) error {
	produced := assignProbeRecords(cfg.Records, cfg.Partitions)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers),
		kgo.ClientID("jaiscloud-kafka-probe"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("kafka-probe: producer client: %w", err)
	}
	defer producer.Close()

	recs := make([]*kgo.Record, 0, cfg.Records)
	for partition := 0; partition < cfg.Partitions; partition++ {
		for i := 0; i < produced[int32(partition)]; i++ {
			recs = append(recs, &kgo.Record{
				Topic:     cfg.Topic,
				Partition: int32(partition),
				Value:     []byte(fmt.Sprintf("probe-%d-%d", partition, i)),
			})
		}
	}
	if err := producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka-probe: produce %d records: %w", cfg.Records, err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers),
		kgo.ClientID("jaiscloud-kafka-probe"),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("kafka-probe: consumer client: %w", err)
	}
	consumed := 0
	for consumed < cfg.Records {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			consumer.Close()
			return fmt.Errorf("kafka-probe: consume: %w", err)
		}
		fetches.EachRecord(func(*kgo.Record) { consumed++ })
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		consumer.Close()
		return fmt.Errorf("kafka-probe: commit offsets: %w", err)
	}
	// Closing leaves the group; the committed offsets outlive the member.
	consumer.Close()

	admin := kadm.NewClient(producer)
	endOffsets, err := admin.ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		return fmt.Errorf("kafka-probe: list end offsets: %w", err)
	}
	committed, err := admin.FetchOffsets(ctx, cfg.Group)
	if err != nil {
		return fmt.Errorf("kafka-probe: fetch committed offsets: %w", err)
	}
	committedByPart, lag, err := summarizeProbeOffsets(cfg.Topic, produced, endOffsets, committed)
	if err != nil {
		return err
	}
	configs, err := describeProbeConfigs(ctx, admin, cfg.Topic)
	if err != nil {
		return fmt.Errorf("kafka-probe: describe topic configs: %w", err)
	}

	summary := kafkaProbeSummary{
		OK:         true,
		Topic:      cfg.Topic,
		Group:      cfg.Group,
		Produced:   cfg.Records,
		Consumed:   consumed,
		Partitions: produced,
		Committed:  committedByPart,
		Lag:        lag,
		Configs:    configs,
	}

	var failures []string
	if consumed != cfg.Records {
		failures = append(failures, fmt.Sprintf("consumed %d of %d records", consumed, cfg.Records))
	}
	for partition, l := range lag {
		if l != 0 {
			failures = append(failures, fmt.Sprintf("partition %d lag=%d (want 0)", partition, l))
		}
	}
	summary.OK = len(failures) == 0

	raw, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("kafka-probe: marshal summary: %w", err)
	}
	fmt.Println(string(raw))

	if len(failures) > 0 {
		return fmt.Errorf("kafka-probe: %s", strings.Join(failures, "; "))
	}
	return nil
}

// assignProbeRecords distributes records round-robin across partitions and
// returns the per-partition record count. It is the probe's deterministic
// partition plan, asserted by both the probe's own summary and the e2e gate.
func assignProbeRecords(records, partitions int) map[int32]int {
	out := make(map[int32]int, partitions)
	for i := 0; i < records; i++ {
		out[int32(i%partitions)]++
	}
	return out
}

// summarizeProbeOffsets maps the broker's end and committed offsets onto the
// probe's produced partitions, deriving each partition's lag (end - committed).
// kadm reports -1 for "no committed offset"; a partition the probe never
// produced to has no lag entry.
func summarizeProbeOffsets(topic string, produced map[int32]int, end kadm.ListedOffsets, committed kadm.OffsetResponses) (map[int32]int64, map[int32]int64, error) {
	committedByPart := make(map[int32]int64, len(produced))
	lag := make(map[int32]int64, len(produced))
	for partition := range produced {
		endOffset := int64(0)
		if e, ok := end[topic][partition]; ok {
			if e.Err != nil {
				return nil, nil, fmt.Errorf("kafka-probe: end offset topic %q partition %d: %w", topic, partition, e.Err)
			}
			endOffset = e.Offset
		}
		committedOffset := int64(0)
		if c, ok := committed[topic][partition]; ok {
			if c.Err != nil {
				return nil, nil, fmt.Errorf("kafka-probe: committed offset topic %q partition %d: %w", topic, partition, c.Err)
			}
			if c.At >= 0 {
				committedOffset = c.At
			}
		}
		committedByPart[partition] = committedOffset
		lag[partition] = endOffset - committedOffset
	}
	return committedByPart, lag, nil
}

// describeProbeConfigs reads topic's configs from the broker and returns only
// the explicitly-overridden properties.
func describeProbeConfigs(ctx context.Context, admin *kadm.Client, topic string) (map[string]string, error) {
	rcs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, err
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return nil, err
	}
	if rc.Err != nil {
		return nil, rc.Err
	}
	return topicOverrides(rc), nil
}

// topicOverrides extracts the topic properties a caller explicitly set (the
// broker reports them with Source=DYNAMIC_TOPIC_CONFIG), dropping the defaults
// and synonyms the describe response also carries.
func topicOverrides(rc kadm.ResourceConfig) map[string]string {
	out := map[string]string{}
	for _, c := range rc.Configs {
		if c.Source == kmsg.ConfigSourceDynamicTopicConfig {
			out[c.Key] = c.MaybeValue()
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
