//go:build gcp_differential

package gcpdifferential

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestScenariosValid exercises the curated list contract: every scenario is
// addressable (non-empty Service/Op/Method/Path), every Service has a real-GCP
// origin in serviceBaseURL, and (Service, Op) is unique so goldens can be
// matched to scenarios by key instead of by position.
func TestScenariosValid(t *testing.T) {
	const project = "proj"
	const suffix = "abc123"
	scenarios := Scenarios(project, suffix)
	if len(scenarios) == 0 {
		t.Fatal("Scenarios returned no scenarios")
	}

	seen := map[string]bool{}
	have := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Path == "" {
			t.Errorf("scenario %+v has an empty required field", sc)
		}
		if _, ok := serviceBaseURL[sc.Service]; !ok {
			t.Errorf("scenario %s/%s: service %q has no serviceBaseURL entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate (Service, Op): %s/%s", sc.Service, sc.Op)
		}
		seen[k] = true
		have[sc.Service] = true
	}
	// The original twelve plus the AUD6 breadth services must each stay
	// represented, so a dropped scenario block is caught offline.
	for _, svc := range []string{
		"dns", "workflows", "iam", "firestore",
		"container", "dataproc", "monitoring", "logging", "datastore",
		"workflowexecutions", "serviceusage",
		"resourcemanager", "eventarc", "tasks", "run", "functions",
		"metastore", "managedkafka",
	} {
		if !have[svc] {
			t.Errorf("expected at least one %s scenario", svc)
		}
	}
}

// TestNormalizerCoversResources guards that every run-suffixed resource name is
// folded to its placeholder, so committed goldens never carry run-specific
// identifiers.
func TestNormalizerCoversResources(t *testing.T) {
	const project = "proj"
	const suffix = "abc123"
	names := Names(suffix)
	norm := NewNormalizer(project, "123456789", suffix, names)

	cases := []struct{ in, want string }{
		{names.Bucket, "<bucket>"},
		{names.Topic, "<topic>"},
		{names.Sub, "<subscription>"},
		{names.Secret, "<secret>"},
		{names.DS, "<dataset>"},
		{names.Table, "<table>"},
		{names.DNSRRSet, "<rrset>"},
		{names.DNSName, "<dnsName>"},
		{names.DNSZone, "<dnsZone>"},
		{names.Workflow, "<workflow>"},
		{names.FSCollection, "<fsCollection>"},
		{names.FSDoc, "<fsDoc>"},
		{names.ComputeInstance, "<computeInstance>"},
		{names.SQLInstance, "<sqlInstance>"},
		{names.RedisInstance, "<redisInstance>"},
		{names.ContainerCluster, "<containerCluster>"},
		{names.DataprocCluster, "<dataprocCluster>"},
		{names.LogName, "<logName>"},
		{names.MetricType, "<metricType>"},
		{names.DSKind, "<dsKind>"},
		{names.DSEntity, "<dsEntity>"},
		{names.MsgTopic, "<topic>"},
		{names.MsgSub, "<subscription>"},
		{names.OrderTopic, "<topic>"},
		{names.OrderSub, "<subscription>"},
		{names.ServiceAccount + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
		{names.ServiceAccount, "<serviceAccountId>"},
		{"missing-" + suffix + "@" + project + ".iam.gserviceaccount.com", "<serviceAccount>"},
		{"missing-" + suffix, "<missing>"},
		{project, "<project>"},
	}
	for _, tc := range cases {
		if got := norm.substitute(tc.in); got != tc.want {
			t.Errorf("substitute(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestNormalizerFoldsAckIDs guards that a server-generated Pub/Sub ack id never
// lands in a committed golden, in a request (Acknowledge/ModifyAckDeadline) or a
// response (Pull). The tokens are delivery-scoped and differ every run.
func TestNormalizerFoldsAckIDs(t *testing.T) {
	names := Names("abc123")
	norm := NewNormalizer("differential-proj", "", "abc123", names)

	req := string(norm.RequestBytes([]byte(`{"subscription":"projects/differential-proj/subscriptions/s","ackIds":["UAYWLF1GSFE3GQhoUQ-opaque"]}`)))
	if !strings.Contains(req, `"ackIds":["<ackId>"]`) {
		t.Errorf("request ackIds were not folded: %s", req)
	}
	if strings.Contains(req, "opaque") {
		t.Errorf("request still carries the raw ack id: %s", req)
	}
	resp := string(norm.Bytes([]byte(`{"receivedMessages":[{"ackId":"UAYWLF1GSFE3GQhoUQ-opaque"}]}`)))
	if !strings.Contains(resp, `"ackId":"<ackId>"`) {
		t.Errorf("response ackId was not folded: %s", resp)
	}
}

// TestNormalizerFoldsOpaqueIDs verifies the directional id folding: server
// responses fold generated decimal/hex ids, while harness-authored request
// bodies keep client id values verbatim (e.g. BigQuery's row "id":"1").
func TestNormalizerFoldsOpaqueIDs(t *testing.T) {
	// Use a project id that is not a substring of "projects" (real project ids
	// and the emulator default are both longer/distinct), so the operation-name
	// shape survives textual substitution.
	names := Names("abc123")
	norm := NewNormalizer("differential-proj", "998877665544", "abc123", names)

	resp := string(norm.Bytes([]byte(`{"id":"1234567890"}`)))
	if !strings.Contains(resp, `"<id>"`) {
		t.Errorf("response id was not folded: %s", resp)
	}
	req := string(norm.RequestBytes([]byte(`{"id":"1234567890"}`)))
	if strings.Contains(req, "<id>") {
		t.Errorf("request id must be preserved verbatim: %s", req)
	}

	op := string(norm.Bytes([]byte(`{"name":"projects/differential-proj/locations/us-central1/operations/abcdef123456"}`)))
	if !strings.Contains(op, "/operations/<operation>") {
		t.Errorf("operation name was not folded: %s", op)
	}

	// Dataproc publishes region-scoped operation names; the region segment must
	// survive while the id folds (AUD6-4).
	dpop := string(norm.Bytes([]byte(`{"name":"projects/differential-proj/regions/us-central1/operations/12345678-abcd"}`)))
	if !strings.Contains(dpop, "regions/us-central1/operations/<operation>") {
		t.Errorf("Dataproc region-scoped operation name was not folded: %s", dpop)
	}
}

// TestNormalizerFoldsOperationPath verifies an LRO poll path's volatile
// operation id is folded, so an operation-poll golden is stable across runs.
func TestNormalizerFoldsOperationPath(t *testing.T) {
	names := Names("abc123")
	norm := NewNormalizer("differential-proj", "998877665544", "abc123", names)
	got := norm.Path("/v1/projects/differential-proj/locations/us-central1/operations/operation-123-abc")
	want := "/v1/projects/<project>/locations/us-central1/operations/<operation>"
	if got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
}

// TestMatchScenariosToGoldens pins the (Service, Op) matcher contract that lets
// TestReplay run with unrecorded scenarios in the tree: a scenario without a
// golden is pending (not fatal), while a golden without a scenario is an orphan
// (fatal), and duplicate keys are reported.
func TestMatchScenariosToGoldens(t *testing.T) {
	scenarios := []Scenario{
		{Service: "dns", Op: "zone_get", Method: "GET", Path: "/a"},
		{Service: "workflows", Op: "workflow_get", Method: "GET", Path: "/b"},
	}
	goldens := []Exchange{{Service: "dns", Op: "zone_get"}}

	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, goldens)
	if len(matched) != 1 || matched[0].Scenario.Op != "zone_get" {
		t.Fatalf("matched = %+v, want one dns/zone_get pair", matched)
	}
	if len(pending) != 1 || pending[0] != "workflows/workflow_get" {
		t.Fatalf("pending = %v, want [workflows/workflow_get]", pending)
	}
	if len(orphans) != 0 || len(duplicates) != 0 {
		t.Fatalf("orphans = %v, duplicates = %v, want none", orphans, duplicates)
	}

	// A golden with no scenario is an orphan.
	_, _, orphans, _ = matchScenariosToGoldens(scenarios, append(goldens, Exchange{Service: "x", Op: "y"}))
	if len(orphans) != 1 || orphans[0] != "x/y" {
		t.Fatalf("orphans = %v, want [x/y]", orphans)
	}

	// A duplicated scenario key is reported.
	_, _, _, duplicates = matchScenariosToGoldens(
		append(scenarios, Scenario{Service: "dns", Op: "zone_get"}),
		goldens,
	)
	if len(duplicates) != 1 || duplicates[0] != "dns/zone_get" {
		t.Fatalf("duplicates = %v, want [dns/zone_get]", duplicates)
	}
}

// TestNoAuthOmitsAuthorization verifies a NoAuth scenario is sent without a
// bearer token (the authz probe) while an ordinary scenario carries it.
func TestNoAuthOmitsAuthorization(t *testing.T) {
	var auth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = append(auth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	tr := &Target{
		Name:    "test",
		Project: "proj",
		Token:   "secret-token",
		HTTP:    srv.Client(),
		URLFor:  func(_, path string) string { return srv.URL + path },
	}
	_, err := tr.Run([]Scenario{
		{Op: "with_auth", Service: "storage", Method: http.MethodGet, Path: "/a"},
		{Op: "no_auth", Service: "storage", Method: http.MethodGet, Path: "/b", NoAuth: true},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(auth) != 2 {
		t.Fatalf("got %d requests, want 2", len(auth))
	}
	if auth[0] != "Bearer secret-token" {
		t.Errorf("authenticated scenario Authorization = %q, want bearer token", auth[0])
	}
	if auth[1] != "" {
		t.Errorf("NoAuth scenario Authorization = %q, want empty", auth[1])
	}
}

// TestWaitForPollsUntilDone verifies a Wait scenario keeps polling until the
// configured field is truthy, then records the terminal response.
func TestWaitForPollsUntilDone(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) < 3 {
			_, _ = io.WriteString(w, `{"done":false}`)
			return
		}
		_, _ = io.WriteString(w, `{"done":true}`)
	}))
	defer srv.Close()

	tr := &Target{
		Name:    "test",
		Project: "proj",
		HTTP:    srv.Client(),
		URLFor:  func(_, path string) string { return srv.URL + path },
	}
	exs, err := tr.Run([]Scenario{{
		Op: "wait", Service: "workflows", Method: http.MethodGet, Path: "/op",
		Wait: &WaitSpec{Field: "done", Interval: time.Millisecond, Timeout: 2 * time.Second},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(exs) != 1 || exs[0].Status != 200 {
		t.Fatalf("exchanges = %+v", exs)
	}
	if !strings.Contains(string(exs[0].Response), `"done":true`) {
		t.Fatalf("terminal response not recorded: %s", exs[0].Response)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Fatalf("polled %d times, want >= 3", got)
	}
}

// TestWaitForContains verifies a Wait scenario can poll an eventually-consistent
// list until it contains a specific resource.
func TestWaitForContains(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) < 3 {
			_, _ = io.WriteString(w, `{"accounts":[{"email":"other@example.com"}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"accounts":[{"email":"target@example.com"}]}`)
	}))
	defer srv.Close()

	tr := &Target{
		Name:    "test",
		Project: "proj",
		HTTP:    srv.Client(),
		URLFor:  func(_, path string) string { return srv.URL + path },
	}
	exs, err := tr.Run([]Scenario{{
		Op: "list", Service: "iam", Method: http.MethodGet, Path: "/sa",
		Wait: &WaitSpec{Field: "accounts", Contains: "target@example.com", Interval: time.Millisecond, Timeout: 2 * time.Second},
	}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exs[0].Status != 200 {
		t.Fatalf("status = %d, want 200", exs[0].Status)
	}
	// It must poll until the target appears, then stop (three calls, not more).
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("polled %d times, want 3", got)
	}
}
