//go:build managedkafka_broker_e2e

// This file is the local-Docker data-plane gate for the Managed Kafka broker
// (W1.1/MKD1). Unlike the k3d gates it needs no cluster: it drives an emulator
// started with JAISCLOUD_KAFKA_BROKER_MODE=docker on the local Docker daemon
// and proves a host-side franz-go client can produce and consume records at the
// broker's advertised 127.0.0.1:<port>, then that deleting the cluster reaps
// the container (the published port stops accepting).
//
// Run with:
//
//	make test-managedkafka-broker-docker
//
// It skips unless MANAGEDKAFKA_DOCKER_E2E=1 (which the Makefile target sets) and
// the emulator is reachable, so `make test-managedkafka-broker-k8s` (which also
// compiles this file) is unaffected.
package managedkafkabroker_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dockerCreateClient allows the synchronous broker startup (container create +
// Kafka-listener readiness wait) to complete within one create request.
var dockerCreateClient = &http.Client{Timeout: 10 * time.Minute}

// emulatorBase returns the reachable emulator base URL (JAISCLOUD_HOST, else
// http://localhost:8080), skipping when it is not up.
func emulatorBase(t *testing.T) string {
	t.Helper()
	base := strings.TrimRight(os.Getenv("JAISCLOUD_HOST"), "/")
	if base == "" {
		base = "http://localhost:8080"
	}
	resp, err := http.Get(base + "/_jaiscloud/health")
	if err != nil {
		t.Skipf("emulator not reachable at %s: %v", base, err)
	}
	resp.Body.Close()
	return base
}

func TestManagedKafkaBrokerDocker(t *testing.T) {
	if os.Getenv("MANAGEDKAFKA_DOCKER_E2E") != "1" {
		t.Skip("set MANAGEDKAFKA_DOCKER_E2E=1 (run via make test-managedkafka-broker-docker)")
	}
	base := emulatorBase(t)

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	cluster := "mk-docker-" + run
	t.Cleanup(func() { deleteCluster(t, base, cluster) })
	// Delete any broker left by a prior run before asserting reaping.
	deleteCluster(t, base, cluster)

	code, body := api(t, dockerCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{"labels": map[string]string{"smoke": "managedkafka-docker-broker"}})
	if code < 200 || code >= 300 {
		t.Fatalf("create cluster: HTTP %d: %v", code, body)
	}

	code, cl := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get cluster: HTTP %d: %v", code, cl)
	}
	addr := strField(cl, "bootstrapAddress")
	if addr == "" {
		t.Fatalf("cluster has no bootstrapAddress: %v", cl)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("bootstrapAddress %q is not the docker loopback endpoint — is the emulator running JAISCLOUD_KAFKA_BROKER_MODE=docker?", addr)
	}

	// The advertised address must accept a connection (the container is live).
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		t.Fatalf("dial broker %s: %v", addr, err)
	}
	conn.Close()

	// Create the topic through the Managed Kafka API (mirrored onto the broker),
	// then produce and consume over the real Kafka wire.
	const topic = "orders"
	code, topicBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/topics?topicId="+topic,
		map[string]any{"partitionCount": 1, "replicationFactor": 1})
	if code < 200 || code >= 300 {
		t.Fatalf("create topic: HTTP %d: %v", code, topicBody)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	produceConsume(t, ctx, addr, topic)

	// TD10 p2/p5 over the real broker: parallel producer sessions, parallel
	// reader sessions over the wire, and a committed consumer group the control
	// plane then serves (TD10 p4's consumer-group half, which the hermetic mock
	// topology cannot exercise). A second, multi-partition topic keeps the
	// record counts exact (the first topic already carries the smoke records).
	const (
		concurrentTopic = "orders-concurrent"
		group           = "mk-depth-group"
	)
	code, concurrentBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/topics?topicId="+concurrentTopic,
		map[string]any{"partitionCount": 3, "replicationFactor": 1})
	if code < 200 || code >= 300 {
		t.Fatalf("create concurrent topic: HTTP %d: %v", code, concurrentBody)
	}
	produceConsumeConcurrent(t, ctx, addr, concurrentTopic, group)
	assertConsumerGroupVisible(t, base, cluster, concurrentTopic, group)

	// Delete the cluster and assert the container is reaped: the advertised
	// loopback port stops accepting connections.
	deleteCluster(t, base, cluster)
	waitPortClosed(t, addr, 60*time.Second)
}

// produceConsumeConcurrent is the Managed Kafka data-plane p2/p5 gate. It
// drives the real broker with several producer clients at once (no write is
// lost), then several independent reader sessions at once (each sees the whole
// stream), then commits a consumer group's offsets so the group is durable and
// readable through the API.
func produceConsumeConcurrent(t *testing.T, ctx context.Context, addr, topic, group string) {
	t.Helper()
	const (
		producers   = 4
		perProducer = 10
		readers     = 3
		total       = producers * perProducer
	)
	payload := func(p, i int) string { return fmt.Sprintf("p%d-m%d", p, i) }

	// (1) Parallel producers to one topic.
	var wg sync.WaitGroup
	errs := make([]error, producers)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			producer, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
				kgo.DefaultProduceTopic(topic),
				kgo.ClientID(fmt.Sprintf("mk-depth-producer-%d", p)),
			)
			if err != nil {
				errs[p] = err
				return
			}
			defer producer.Close()
			for i := 0; i < perProducer; i++ {
				if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte(payload(p, i))}).FirstErr(); err != nil {
					errs[p] = err
					return
				}
			}
		}(p)
	}
	wg.Wait()
	for p, err := range errs {
		if err != nil {
			t.Fatalf("producer %d: %v", p, err)
		}
	}

	// (2) Parallel reader sessions: each independent client must see every
	// record, so concurrent streamed reads do not interleave or truncate.
	readErrs := make([]error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			reader, err := kgo.NewClient(
				kgo.SeedBrokers(addr),
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.ClientID(fmt.Sprintf("mk-depth-reader-%d", r)),
			)
			if err != nil {
				readErrs[r] = err
				return
			}
			defer reader.Close()
			seen := map[string]int{}
			for len(seen) < total {
				if ctx.Err() != nil {
					readErrs[r] = fmt.Errorf("reader %d saw %d/%d records: %w", r, len(seen), total, ctx.Err())
					return
				}
				fetches := reader.PollFetches(ctx)
				if errs := fetches.Errors(); len(errs) > 0 {
					readErrs[r] = fmt.Errorf("reader %d poll: %v", r, errs)
					return
				}
				fetches.EachRecord(func(rec *kgo.Record) { seen[string(rec.Value)]++ })
			}
			for p := 0; p < producers; p++ {
				for i := 0; i < perProducer; i++ {
					if seen[payload(p, i)] == 0 {
						readErrs[r] = fmt.Errorf("reader %d never saw %s", r, payload(p, i))
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
	for r, err := range readErrs {
		if err != nil {
			t.Fatalf("reader %d: %v", r, err)
		}
	}

	// (3) Commit a consumer group so the group and its offsets are durable for
	// the API read.
	member, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ClientID("mk-depth-group-member"),
	)
	if err != nil {
		t.Fatalf("consumer group client: %v", err)
	}
	defer member.Close()
	var committed []*kgo.Record
	for len(committed) < total {
		if ctx.Err() != nil {
			t.Fatalf("group member committed %d/%d records: %v", len(committed), total, ctx.Err())
		}
		fetches := member.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("group member poll: %v", errs)
		}
		fetches.EachRecord(func(rec *kgo.Record) { committed = append(committed, rec) })
	}
	if err := member.CommitRecords(ctx, committed...); err != nil {
		t.Fatalf("commit group offsets: %v", err)
	}
}

// assertConsumerGroupVisible reads the committed group back through the
// Managed Kafka consumer-group API, paging with pageSize=1 to compose the
// documented pageToken with the group listing.
func assertConsumerGroupVisible(t *testing.T, base, cluster, topic, group string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		found, groupTopics, err := findConsumerGroup(t, base, cluster, group)
		if err != nil {
			t.Fatalf("list consumer groups: %v", err)
		}
		if found {
			suffix := "/topics/" + topic
			for name := range groupTopics {
				if strings.HasSuffix(name, suffix) {
					return
				}
			}
			last = fmt.Sprintf("group %q listed but has no committed offset for %q (topics=%v)", group, topic, groupTopics)
		} else {
			last = fmt.Sprintf("group %q not listed yet", group)
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("consumer group never became visible: %s", last)
}

// findConsumerGroup pages the cluster's consumer groups (pageSize=1) looking for
// group, returning whether it was found and its committed topics map.
func findConsumerGroup(t *testing.T, base, cluster, group string) (bool, map[string]any, error) {
	t.Helper()
	token := ""
	for page := 0; page < 100; page++ {
		u := base + clusterPath(cluster) + "/consumerGroups?pageSize=1"
		if token != "" {
			u += "&pageToken=" + url.QueryEscape(token)
		}
		code, body := api(t, httpClient, http.MethodGet, u, nil)
		if code < 200 || code >= 300 {
			return false, nil, fmt.Errorf("HTTP %d: %v", code, body)
		}
		groups, _ := body["consumerGroups"].([]any)
		if len(groups) > 1 {
			return false, nil, fmt.Errorf("pageSize=1 returned %d groups", len(groups))
		}
		for _, g := range groups {
			m, ok := g.(map[string]any)
			if !ok {
				continue
			}
			if strings.HasSuffix(strField(m, "name"), "/"+group) {
				topics, _ := m["topics"].(map[string]any)
				return true, topics, nil
			}
		}
		token = strField(body, "nextPageToken")
		if token == "" {
			return false, nil, nil
		}
	}
	return false, nil, fmt.Errorf("consumer group pagination did not terminate")
}

// produceConsume writes a handful of records and reads them back over a real
// franz-go client at addr.
func produceConsume(t *testing.T, ctx context.Context, addr, topic string) {
	t.Helper()
	const records = 10

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.DefaultProduceTopic(topic),
		kgo.ClientID("jaiscloud-mk-docker-e2e"),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer producer.Close()
	for i := 0; i < records; i++ {
		if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("m-%d", i))}).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("jaiscloud-mk-docker-e2e"),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consumer.Close()

	got := 0
	deadline := time.Now().Add(60 * time.Second)
	for got < records && time.Now().Before(deadline) {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs)
		}
		fetches.EachRecord(func(*kgo.Record) { got++ })
	}
	if got < records {
		t.Fatalf("consumed %d records, want %d", got, records)
	}
}

// waitPortClosed waits until addr stops accepting TCP connections.
func waitPortClosed(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("broker port %s still accepting connections after cluster delete", addr)
}
