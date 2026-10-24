package gcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	pubsub "cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	scheduler "cloud.google.com/go/scheduler/apiv1"
	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"jaiscloud/internal/clock"
)

// This file is the dedicated emulator-clock / time-control suite (TD2). It
// freezes, advances and reads the business clock over the admin endpoint
// (POST/GET /_jaiscloud/clock) and asserts, deterministically, the contract Raj
// committed in f3db99ae ("Time freeze, offset feature and test implementation")
// — the three UTC modes, the admin endpoint validation, reset restoring real
// time, no leak between tests — plus the clock-driven service paths:
// scheduler due-time dispatch, Pub/Sub retention expiry, KMS scheduled-destroy
// and async-LRO deadline settlement.
//
// It runs as part of `go test -race ./tests/integration/gcp/` (no extra wiring).
// The async-LRO case is skipped unless JAISCLOUD_LRO_ASYNC=1 and is picked up by
// `make test-lro-async-gcp` (whose `-run TestLROAsync` prefix matches it).
//
// Two hard rules from the contract this file must obey:
//   - advancing a *frozen* clock does NOT wake the wall-time tickers (scheduler,
//     tasks, monitoring, EventBridge): a deterministic test calls the synchronous
//     `/_jaiscloud/<engine>-tick` endpoint instead of sleeping;
//   - test-side uniqueness uses clock.RealNow(), never clock.Now(), so a frozen
//     business clock cannot make generated resource names collide.

// ─── clock helpers (mirroring tests/integration/helpers_test.go for AWS) ──────

// tcProjectID is the project the gRPC-backed cases operate in.
func tcProjectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

// tcGRPCAddr is the emulator's gRPC listener. CI starts it on :8081.
func tcGRPCAddr() string {
	if h := os.Getenv("JAISCLOUD_GCP_GRPC"); h != "" {
		return h
	}
	return "localhost:8081"
}

// tcSuffix returns a per-run suffix from the REAL wall clock, so a frozen
// business clock can never make two runs (or two resources) collide.
func tcSuffix() string { return fmt.Sprintf("%d", clock.RealNow().UnixNano()) }

// tcLRODelay bounds the async-LRO in-flight window. The canonical
// `make test-lro-async-gcp` target starts the server with JAISCLOUD_LRO_DELAY=2s
// but does not export it to the test process, so default to that; a direct run
// that exports it (or a server on the 250ms default) is still covered because
// the advance is generous.
func tcLRODelay() time.Duration {
	if v := os.Getenv("JAISCLOUD_LRO_DELAY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 2 * time.Second
}

// tcJSONHeaders is the content type every KMS/scheduler REST write needs.
func tcJSONHeaders() map[string]string {
	return map[string]string{"Content-Type": "application/json"}
}

// tcPostClock posts a clock payload and asserts the status code.
func tcPostClock(t *testing.T, body map[string]any, want int) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	resp, respBody := do(t, http.MethodPost, "/_jaiscloud/clock", raw, tcJSONHeaders())
	require.Equalf(t, want, resp.StatusCode, "POST /_jaiscloud/clock %v: %s", body, respBody)
}

// tcFreezeAt freezes the business clock at ts for the duration of the test,
// restoring real time on cleanup.
func tcFreezeAt(t *testing.T, ts time.Time) {
	t.Helper()
	tcPostClock(t, map[string]any{"mode": "fixed", "time": ts}, http.StatusNoContent)
	t.Cleanup(func() { tcResetClockBestEffort(t) })
}

// tcAdvanceTo moves a frozen clock to a new instant (fixed mode re-position).
func tcAdvanceTo(t *testing.T, ts time.Time) {
	t.Helper()
	tcPostClock(t, map[string]any{"mode": "fixed", "time": ts}, http.StatusNoContent)
}

// tcClockState reads mode + time from GET /_jaiscloud/clock.
func tcClockState(t *testing.T) (string, time.Time) {
	t.Helper()
	resp, body := do(t, http.MethodGet, "/_jaiscloud/clock", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /_jaiscloud/clock: %s", body)
	m := jsonMap(t, body)
	mode, _ := m["mode"].(string)
	raw, _ := m["time"].(string)
	ts, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err, "clock time %q", raw)
	return mode, ts
}

func tcClockMode(t *testing.T) string {
	t.Helper()
	mode, _ := tcClockState(t)
	return mode
}

func tcClockNow(t *testing.T) time.Time {
	t.Helper()
	_, ts := tcClockState(t)
	return ts
}

// tcResetClockBestEffort restores wall time and never fails the test — it runs
// in cleanup, where a hard failure would mask the real one.
func tcResetClockBestEffort(t *testing.T) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"mode": "real"})
	resp, err := http.Post(gcpBase()+"/_jaiscloud/clock", "application/json", bytes.NewReader(b))
	if err == nil {
		resp.Body.Close()
	}
}

// ─── 1. the clock contract ────────────────────────────────────────────────────

// TestClock_FixedMode_TimeDoesNotAdvance: in fixed mode clock.Now() returns the
// same instant on every call regardless of real wall time passing.
func TestClock_FixedMode_TimeDoesNotAdvance(t *testing.T) {
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	require.Equal(t, "fixed", tcClockMode(t))
	require.True(t, T0.Equal(tcClockNow(t)), "GET clock should reflect the frozen %v", T0)

	// Real time passing must not advance the frozen business clock.
	time.Sleep(250 * time.Millisecond)
	require.True(t, T0.Equal(tcClockNow(t)), "frozen clock must not advance with real time")
}

// TestClock_OffsetMode_TimeAdvancesFromBase: in offset mode the clock is
// anchored to a base and advances at real wall-clock speed.
func TestClock_OffsetMode_TimeAdvancesFromBase(t *testing.T) {
	resetState(t)

	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tcPostClock(t, map[string]any{"mode": "offset", "time": base}, http.StatusNoContent)
	t.Cleanup(func() { tcResetClockBestEffort(t) })

	c1 := tcClockNow(t)
	require.False(t, c1.Before(base), "clock should be at or after the base %v", base)

	time.Sleep(300 * time.Millisecond)

	c2 := tcClockNow(t)
	require.True(t, c2.After(c1), "offset clock should advance with real time")
	elapsed := c2.Sub(c1)
	require.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "elapsed %v", elapsed)
	require.Less(t, elapsed, 2*time.Second, "elapsed %v (must not be frozen)", elapsed)
}

// TestClock_AdminEndpointContract: mode transitions, validation, and that
// POST /_jaiscloud/reset restores wall time.
func TestClock_AdminEndpointContract(t *testing.T) {
	resetState(t)
	require.Equal(t, "real", tcClockMode(t), "reset restores real mode")

	T0 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)
	require.Equal(t, "fixed", tcClockMode(t))
	require.True(t, T0.Equal(tcClockNow(t)), "expected frozen %v", T0)

	tcPostClock(t, map[string]any{"mode": "real"}, http.StatusNoContent)
	require.Equal(t, "real", tcClockMode(t))

	// Validation: fixed/offset with no (or zero) time → 400; unknown mode → 400.
	tcPostClock(t, map[string]any{"mode": "fixed"}, http.StatusBadRequest)
	tcPostClock(t, map[string]any{"mode": "fixed", "time": time.Time{}}, http.StatusBadRequest)
	tcPostClock(t, map[string]any{"mode": "offset"}, http.StatusBadRequest)
	tcPostClock(t, map[string]any{"mode": "bogus"}, http.StatusBadRequest)

	// reset returns the clock to real.
	tcFreezeAt(t, T0)
	resetState(t)
	require.Equal(t, "real", tcClockMode(t), "reset must restore the wall clock")
}

// TestClock_NoLeakBetweenTests: a freeze from one sub-test must not leak into
// the next (cleanup restores real time).
func TestClock_NoLeakBetweenTests(t *testing.T) {
	t.Run("A_freeze", func(t *testing.T) {
		resetState(t)
		tcFreezeAt(t, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Equal(t, "fixed", tcClockMode(t))
	})

	t.Run("B_no_leak", func(t *testing.T) {
		resetState(t)
		require.Equal(t, "real", tcClockMode(t), "A's freeze must not leak into B")
		require.WithinDuration(t, clock.RealNow(), tcClockNow(t), time.Minute,
			"clock should be near real wall time, not 2020")
	})
}

// TestClock_ServiceTimestampFromFrozenClock: a resource's wire timestamp is
// stamped from the frozen business clock, not wall time — the GCP analogue of
// the AWS suite's STS-token Expiration assertion. Cross-checked REST + gRPC.
func TestClock_ServiceTimestampFromFrozenClock(t *testing.T) {
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	parent := "projects/" + tcProjectID() + "/locations/global"
	ringID := "clock-ring-" + tcSuffix()

	resp, body := do(t, http.MethodPost, "/v1/"+parent+"/keyRings?keyRingId="+ringID,
		[]byte("{}"), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create key ring: %s", body)

	restRaw, _ := jsonMap(t, body)["createTime"].(string)
	require.NotEmpty(t, restRaw, "keyRing createTime must be present: %s", body)
	restTS, err := time.Parse(time.RFC3339Nano, restRaw)
	require.NoError(t, err)
	require.Truef(t, T0.Equal(restTS),
		"REST keyRing.createTime = %v, want the frozen instant %v", restTS, T0)

	ctx := context.Background()
	kc, err := kms.NewKeyManagementClient(ctx,
		option.WithEndpoint(tcGRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	require.NoError(t, err, "dial kms gRPC")
	t.Cleanup(func() { _ = kc.Close() })

	ring, err := kc.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: parent + "/keyRings/" + ringID})
	require.NoError(t, err, "gRPC GetKeyRing")
	require.Truef(t, T0.Equal(ring.GetCreateTime().AsTime()),
		"gRPC keyRing.createTime = %v, want %v", ring.GetCreateTime().AsTime(), T0)
}

// ─── 2. scheduler — due-time dispatch needs an explicit tick ──────────────────

// tcSchedulerHook records the deliveries and X-CloudScheduler-ScheduleTime the
// emulator's scheduler makes to a local httpTarget.
type tcSchedulerHook struct {
	mu    sync.Mutex
	n     int
	slots []string
}

func (h *tcSchedulerHook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.n++
	h.slots = append(h.slots, r.Header.Get("X-CloudScheduler-ScheduleTime"))
	h.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (h *tcSchedulerHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func (h *tcSchedulerHook) firstSlot() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.slots) == 0 {
		return ""
	}
	return h.slots[0]
}

// TestClock_SchedulerDueTimeDeterministic freezes the clock, proves a tick
// before the cron slot fires nothing, advances to the slot and fires it with an
// explicit /_jaiscloud/scheduler-tick. The scheduleTime is cross-checked REST +
// gRPC, and the delivery carries the same slot in X-CloudScheduler-ScheduleTime.
func TestClock_SchedulerDueTimeDeterministic(t *testing.T) {
	resetState(t)

	// Half a second past the minute so the next "* * * * *" slot is strictly future.
	T0 := time.Date(2024, 3, 15, 10, 0, 0, 500_000_000, time.UTC)
	tcFreezeAt(t, T0)

	hook := &tcSchedulerHook{}
	srv := httptest.NewServer(hook)
	defer srv.Close()

	proj, loc := tcProjectID(), "us-central1"
	jobID := "clock-job-" + tcSuffix()
	jobName := "projects/" + proj + "/locations/" + loc + "/jobs/" + jobID
	createBody, err := json.Marshal(map[string]any{
		"name":       jobName,
		"schedule":   "* * * * *",
		"timeZone":   "UTC",
		"httpTarget": map[string]any{"uri": srv.URL, "httpMethod": "GET"},
	})
	require.NoError(t, err)
	resp, body := do(t, http.MethodPost, "/v1/projects/"+proj+"/locations/"+loc+"/jobs",
		createBody, tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create job: %s", body)

	scheduleRaw, _ := jsonMap(t, body)["scheduleTime"].(string)
	require.NotEmpty(t, scheduleRaw, "job scheduleTime must be present: %s", body)
	slot, err := time.Parse(time.RFC3339Nano, scheduleRaw)
	require.NoError(t, err)
	require.Truef(t, slot.After(T0), "cron slot %v must be after the frozen now %v", slot, T0)

	// REST↔gRPC cross-check: the scheduleTime is the same over both transports.
	ctx := context.Background()
	sc, err := scheduler.NewCloudSchedulerClient(ctx,
		option.WithEndpoint(tcGRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	require.NoError(t, err, "dial scheduler gRPC")
	t.Cleanup(func() { _ = sc.Close() })

	job, err := sc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: jobName})
	require.NoError(t, err, "gRPC GetJob")
	require.Truef(t, slot.Equal(job.GetScheduleTime().AsTime()),
		"gRPC scheduleTime = %v, want %v", job.GetScheduleTime().AsTime(), slot)

	// A tick before the slot fires nothing (the job is not yet due).
	resp, body = do(t, http.MethodPost, "/_jaiscloud/scheduler-tick", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "tick: %s", body)
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, hook.count(), "no delivery before the cron slot")

	// Advance the frozen clock to the slot, then tick. The engine's wall-time
	// ticker does not wake from a clock advance, so the explicit tick is what
	// makes this deterministic.
	tcAdvanceTo(t, slot)
	resp, body = do(t, http.MethodPost, "/_jaiscloud/scheduler-tick", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "tick: %s", body)
	require.Eventually(t, func() bool { return hook.count() >= 1 }, 5*time.Second, 20*time.Millisecond,
		"the due job must fire on the tick")
	require.Equalf(t, slot.UTC().Format(time.RFC3339), hook.firstSlot(),
		"X-CloudScheduler-ScheduleTime must carry the cron slot")
}

// ─── 3. Pub/Sub retention expiry ──────────────────────────────────────────────

// tcPullImmediate issues one immediate Pull (no long-poll) and returns the
// received messages.
func tcPullImmediate(t *testing.T, c *pubsub.Client, ctx context.Context, sub string) []*pubsubpb.ReceivedMessage {
	t.Helper()
	resp, err := c.SubscriptionAdminClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: sub, MaxMessages: 10, ReturnImmediately: true,
	})
	require.NoError(t, err, "Pull(%s)", sub)
	return resp.GetReceivedMessages()
}

// TestClock_PubSubRetentionExpiry: a message is pullable while the frozen clock
// is behind the topic's retention window and expired once the clock crosses it.
func TestClock_PubSubRetentionExpiry(t *testing.T) {
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Setenv("PUBSUB_EMULATOR_HOST", tcGRPCAddr())
	}
	ctx := context.Background()
	c, err := pubsub.NewClient(ctx, tcProjectID())
	require.NoError(t, err, "pubsub.NewClient")
	t.Cleanup(func() { _ = c.Close() })

	sfx := tcSuffix()
	topic := "projects/" + tcProjectID() + "/topics/clock-retention-" + sfx
	sub := "projects/" + tcProjectID() + "/subscriptions/clock-retention-" + sfx
	const retention = 600 * time.Second

	_, err = c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topic, MessageRetentionDuration: durationpb.New(retention),
	})
	require.NoError(t, err, "CreateTopic")
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.TopicAdminClient.DeleteTopic(dctx, &pubsubpb.DeleteTopicRequest{Topic: topic})
	})

	_, err = c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 10,
	})
	require.NoError(t, err, "CreateSubscription")
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.SubscriptionAdminClient.DeleteSubscription(dctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: sub})
	})

	pub, err := c.TopicAdminClient.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("retained")}},
	})
	require.NoError(t, err, "Publish")
	require.Len(t, pub.GetMessageIds(), 1)

	// Seeded negative: behind the retention window the message is still readable.
	msgs := tcPullImmediate(t, c, ctx, sub)
	require.Len(t, msgs, 1, "message must be pullable while behind its retention window")
	require.Equal(t, "retained", string(msgs[0].GetMessage().GetData()))

	// Cross the window on the frozen clock: the message is expired.
	tcAdvanceTo(t, T0.Add(retention+time.Second))
	require.Empty(t, tcPullImmediate(t, c, ctx, sub),
		"message must be expired once the clock passes the retention window")

	// REST cross-check: the topic reports the retention that was set (not the default).
	resp, body := do(t, http.MethodGet, "/v1/projects/"+tcProjectID()+"/topics/clock-retention-"+sfx, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "REST get topic: %s", body)
	require.Equal(t, "600s", jsonMap(t, body)["messageRetentionDuration"],
		"topic retention must round-trip over REST: %s", body)
}

// ─── 4. KMS scheduled-destroy (lazy-on-read promotion) ────────────────────────

// TestClock_KMSScheduledDestroy: destroy schedules a future destroy_time and the
// version is promoted to DESTROYED only when the frozen clock crosses it (the
// emulator promotes lazily on read, so no advance-alone is needed like the
// ticker-backed engines).
func TestClock_KMSScheduledDestroy(t *testing.T) {
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	parent := "projects/" + tcProjectID() + "/locations/global"
	sfx := tcSuffix()
	ringID, keyID := "clock-destroy-ring-"+sfx, "clock-destroy-key-"+sfx

	resp, body := do(t, http.MethodPost, "/v1/"+parent+"/keyRings?keyRingId="+ringID, []byte("{}"), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create key ring: %s", body)
	keyName := parent + "/keyRings/" + ringID + "/cryptoKeys/" + keyID
	resp, body = do(t, http.MethodPost, "/v1/"+parent+"/keyRings/"+ringID+"/cryptoKeys?cryptoKeyId="+keyID,
		[]byte(`{"purpose":"ENCRYPT_DECRYPT"}`), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create crypto key: %s", body)

	// Explicitly create the version to destroy.
	resp, body = do(t, http.MethodPost, "/v1/"+keyName+"/cryptoKeyVersions", []byte("{}"), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create version: %s", body)
	verName, _ := jsonMap(t, body)["name"].(string)
	require.NotEmpty(t, verName, "version name: %s", body)

	// Destroy schedules a destroy_time 30 days out.
	resp, body = do(t, http.MethodPost, "/v1/"+verName+":destroy", []byte("{}"), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "destroy version: %s", body)
	dm := jsonMap(t, body)
	require.Equal(t, "DESTROY_SCHEDULED", dm["state"], "destroy must schedule, not delete: %s", body)
	destroyRaw, _ := dm["destroyTime"].(string)
	require.NotEmpty(t, destroyRaw, "destroyTime: %s", body)
	destroyAt, err := time.Parse(time.RFC3339Nano, destroyRaw)
	require.NoError(t, err)
	require.Truef(t, destroyAt.After(T0), "destroyTime %v must be after the frozen now %v", destroyAt, T0)

	// Seeded negative: before the window it stays scheduled.
	resp, body = do(t, http.MethodGet, "/v1/"+verName, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get version: %s", body)
	require.Equal(t, "DESTROY_SCHEDULED", jsonMap(t, body)["state"],
		"version must not be destroyed while behind the destroy window")

	// Cross the window: a read promotes it to DESTROYED.
	tcAdvanceTo(t, destroyAt.Add(time.Second))
	resp, body = do(t, http.MethodGet, "/v1/"+verName, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get version: %s", body)
	require.Equal(t, "DESTROYED", jsonMap(t, body)["state"],
		"lazy read must promote the version after the destroy window: %s", body)

	// gRPC cross-check.
	ctx := context.Background()
	kc, err := kms.NewKeyManagementClient(ctx,
		option.WithEndpoint(tcGRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	require.NoError(t, err, "dial kms gRPC")
	t.Cleanup(func() { _ = kc.Close() })

	ver, err := kc.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: verName})
	require.NoError(t, err, "gRPC GetCryptoKeyVersion")
	require.Equal(t, kmspb.CryptoKeyVersion_DESTROYED, ver.GetState(),
		"gRPC must report the promoted DESTROYED state")
}

// TestClock_KMSExpiredImportJobHidesWrappingKey: the ImportJob wrapping public
// key is documented "Only returned if state is ACTIVE". The emulator derives the
// state lazily on read, so once the frozen clock crosses the 72h import window
// the job reads back EXPIRED with public_key unset on get and list, over REST and
// gRPC — a client can no longer wrap key material against a job that cannot
// accept it. The publicKeyFormat stays, as it echoes the requested format.
func TestClock_KMSExpiredImportJobHidesWrappingKey(t *testing.T) {
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	parent := "projects/" + tcProjectID() + "/locations/global"
	sfx := tcSuffix()
	ringID, jobID := "clock-import-ring-"+sfx, "clock-import-job-"+sfx

	resp, body := do(t, http.MethodPost, "/v1/"+parent+"/keyRings?keyRingId="+ringID, []byte("{}"), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create key ring: %s", body)

	jobPath := parent + "/keyRings/" + ringID + "/importJobs"
	jobName := jobPath + "/" + jobID
	resp, body = do(t, http.MethodPost, "/v1/"+jobPath+"?importJobId="+jobID,
		[]byte(`{"importMethod":"RSA_OAEP_3072_SHA256"}`), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create import job: %s", body)
	created := jsonMap(t, body)
	require.Equal(t, "ACTIVE", created["state"], "new import job: %s", body)
	pub, _ := created["publicKey"].(map[string]any)
	require.NotEmpty(t, pub["pem"], "an in-window job must publish its wrapping key: %s", body)
	expireRaw, _ := created["expireTime"].(string)
	expireAt, err := time.Parse(time.RFC3339Nano, expireRaw)
	require.NoError(t, err, "expireTime %q", expireRaw)

	ctx := context.Background()
	kc, err := kms.NewKeyManagementClient(ctx,
		option.WithEndpoint(tcGRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	require.NoError(t, err, "dial kms gRPC")
	t.Cleanup(func() { _ = kc.Close() })

	// Seeded negative: inside the window both transports publish the key.
	got, err := kc.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName})
	require.NoError(t, err, "gRPC GetImportJob in-window")
	require.NotNil(t, got.GetPublicKey(), "an ACTIVE job must publish its wrapping key")

	// Cross the 72h window: a read derives EXPIRED and withholds the key.
	tcAdvanceTo(t, expireAt.Add(time.Second))
	resp, body = do(t, http.MethodGet, "/v1/"+jobName, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get import job: %s", body)
	expired := jsonMap(t, body)
	require.Equal(t, "EXPIRED", expired["state"], "past expire_time: %s", body)
	_, hasKey := expired["publicKey"]
	require.False(t, hasKey, "an EXPIRED import job must not return its wrapping public key: %s", body)
	require.Equal(t, "PEM", expired["publicKeyFormat"], "the format echoes the request: %s", body)

	// The list renders the same shape as the get.
	resp, body = do(t, http.MethodGet, "/v1/"+jobPath, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "list import jobs: %s", body)
	items, _ := jsonMap(t, body)["importJobs"].([]any)
	require.Len(t, items, 1, "importJobs.list: %s", body)
	item, _ := items[0].(map[string]any)
	require.Equal(t, "EXPIRED", item["state"], "listed state: %s", body)
	_, hasKey = item["publicKey"]
	require.False(t, hasKey, "importJobs.list must not return an EXPIRED job's wrapping key: %s", body)

	// gRPC agrees on the get and the list.
	got, err = kc.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName})
	require.NoError(t, err, "gRPC GetImportJob past expire_time")
	require.Equal(t, kmspb.ImportJob_EXPIRED, got.GetState(), "gRPC must derive the EXPIRED state")
	require.Nil(t, got.GetPublicKey(), "gRPC EXPIRED import job must not return its wrapping key")
	require.Equal(t, kmspb.PublicKey_PEM, got.GetPublicKeyFormat())
	listed, err := kc.ListImportJobs(ctx, &kmspb.ListImportJobsRequest{Parent: parent + "/keyRings/" + ringID}).Next()
	require.NoError(t, err, "gRPC ListImportJobs")
	require.NotNil(t, listed, "gRPC ListImportJobs must return the job")
	require.Nil(t, listed.GetPublicKey(),
		"gRPC list must not return an EXPIRED job's wrapping key")
}

// ─── 5. async LRO deadline settlement ─────────────────────────────────────────

// TestLROAsyncClockDeadline: in the opt-in async LRO mode an in-flight operation
// settles once the frozen clock crosses JAISCLOUD_LRO_DELAY. Named with the
// TestLROAsync prefix so `make test-lro-async-gcp` (`-run TestLROAsync`) picks
// it up; skipped otherwise.
func TestLROAsyncClockDeadline(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	resetState(t)

	T0 := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	tcFreezeAt(t, T0)

	proj, loc := tcProjectID(), "us-central1"
	wfID := "clock-lro-wf-" + tcSuffix()
	resp, body := do(t, http.MethodPost,
		"/v1/projects/"+proj+"/locations/"+loc+"/workflows?workflowId="+wfID,
		[]byte(`{"sourceContents":"main:\n  steps:\n    - done:\n        return: ok"}`), tcJSONHeaders())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create workflow: %s", body)

	op := jsonMap(t, body)
	opName, _ := op["name"].(string)
	require.NotEmpty(t, opName, "operation name: %s", body)
	done, ok := op["done"].(bool)
	require.True(t, ok, "create operation must carry a boolean done: %s", body)
	require.False(t, done, "async create must be in flight at the frozen now: %s", body)

	pollPath := "/v1/" + opName
	_, body = do(t, http.MethodGet, pollPath, nil, nil)
	inflight := jsonMap(t, body)
	done, ok = inflight["done"].(bool)
	require.True(t, ok, "operation must carry a boolean done: %s", body)
	require.False(t, done, "operation must stay in flight while the clock is behind the deadline")

	// Cross the delay window on the frozen clock; the poll settles it.
	tcAdvanceTo(t, T0.Add(tcLRODelay()+time.Second))
	_, body = do(t, http.MethodGet, pollPath, nil, nil)
	settled := jsonMap(t, body)
	done, ok = settled["done"].(bool)
	require.True(t, ok, "settled operation must carry a boolean done: %s", body)
	require.True(t, done, "operation must settle once the clock crosses the deadline: %s", body)
	require.NotEmpty(t, settled["response"], "settled operation must carry a response: %s", body)
}
