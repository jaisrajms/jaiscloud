package main

// Error / retry / idempotency tour (SDK_TOUR_MODE=errors).
//
// The happy-path tour (runAll) asserts the official clients can drive every
// surface. This leg asserts the *failure* surfaces a green tour cannot see:
// error-code mapping, retry classification, backoff / Retry-After honoring,
// idempotency and resumable-upload rewind. It reuses the same clients and the
// same recorder, so its rows land in the cross-language matrix beside the
// tour's.
//
// Throttle injection is armed through the emulator's runtime control plane
// (POST /_jaiscloud/throttle) per scenario rather than through process env, so
// one emulator process covers every phase; the env path
// (JAISCLOUD_GCP_THROTTLE / _FAIL_FIRST / _STATUS / _RETRY_DELAY) reaches the
// same injector and is exercised by the retry scenarios' first case.
//
// Attempt counts are read back from the emulator's Prometheus counters
// (emulator started with --metrics), which record refused (429/503) and served
// requests alike, so the retry scenarios assert the *observable* attempt count
// rather than "it felt like it retried".

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runErrors is the entry point for SDK_TOUR_MODE=errors.
func runErrors(r *runner) {
	f := &fixtures{
		cfg:       r.cfg,
		bucket:    rid(r.cfg, "errors-bucket"),
		topic:     rid(r.cfg, "errors-topic"),
		sub:       rid(r.cfg, "errors-sub"),
		bigBucket: rid(r.cfg, "errors-bq"),
	}
	storageErrorScenarios(r, f)
	throttleScenarios(r, f)
	idempotencyScenarios(r, f)
	pubsubErrorScenarios(r, f)
}

// ─── runtime throttle control ────────────────────────────────────────────────

// armThrottle posts a control document to the running injector.
func armThrottle(rest, body string) error {
	resp, err := http.Post(rest+"/_jaiscloud/throttle", "application/json", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("arm throttle: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("arm throttle: status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func clearThrottle(rest string) error {
	return armThrottle(rest, `{"mode":"off"}`)
}

// ─── metrics-backed attempt counting ─────────────────────────────────────────

// requestsTotal sums the emulator's jaiscloud_requests_total counter over all
// series whose service label matches want (empty = every cloud service,
// excluding the unnamed admin/metrics series). The value counts the requests
// the emulator *received* for that surface, so the delta across an operation is
// its attempt count.
func requestsTotal(rest, want string) (int, error) {
	resp, err := http.Get(rest + "/metrics")
	if err != nil {
		return 0, fmt.Errorf("scrape metrics: %w", err)
	}
	defer resp.Body.Close()

	total := 0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "jaiscloud_requests_total{") {
			continue
		}
		open := strings.IndexByte(line, '}')
		if open < 0 {
			continue
		}
		labels := line[len("jaiscloud_requests_total{"):open]
		fields := strings.Fields(line[open+1:])
		if len(fields) != 1 {
			continue
		}
		if want == "" {
			if strings.Contains(labels, `service="unknown"`) {
				continue
			}
		} else if !strings.Contains(labels, `service="`+want+`"`) {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		total += int(v)
	}
	return total, sc.Err()
}

// countAttempts runs fn and returns how many requests the emulator received for
// service during it.
func countAttempts(rest, service string, fn func() error) (int, error) {
	before, err := requestsTotal(rest, service)
	if err != nil {
		return 0, err
	}
	if err := fn(); err != nil {
		return 0, err
	}
	after, err := requestsTotal(rest, service)
	if err != nil {
		return 0, err
	}
	return after - before, nil
}

// ─── error classification helpers ────────────────────────────────────────────

// httpStatus extracts the HTTP status an official client surfaced, covering the
// apiary (*googleapi.Error) and gax (*apierror.APIError) shapes.
func httpStatus(err error) (int, bool) {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code, true
	}
	var ae *apierror.APIError
	if errors.As(err, &ae) && ae.HTTPCode() > 0 {
		return ae.HTTPCode(), true
	}
	return 0, false
}

// requireStatus returns nil when err is an HTTP status error with want.
func requireStatus(err error, want int) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d, got success", want)
	}
	got, ok := httpStatus(err)
	if !ok {
		return fmt.Errorf("expected HTTP %d, got non-HTTP error: %v", want, err)
	}
	if got != want {
		return fmt.Errorf("expected HTTP %d, got %d: %v", want, got, err)
	}
	return nil
}

// requireGRPCCode returns nil when err is a gRPC status error with want.
func requireGRPCCode(err error, want codes.Code) error {
	if err == nil {
		return fmt.Errorf("expected gRPC %s, got success", want)
	}
	if st, ok := status.FromError(err); ok && st.Code() == want {
		return nil
	}
	var ae *apierror.APIError
	if errors.As(err, &ae) && ae.GRPCStatus() != nil && ae.GRPCStatus().Code() == want {
		return nil
	}
	return fmt.Errorf("expected gRPC %s, got: %v", want, err)
}

// rawGET issues a no-retry HTTP GET and returns status, headers and body, used
// to inspect the exact injected error envelope.
func rawGET(url string) (int, http.Header, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, b, nil
}

// ─── storage error mapping ───────────────────────────────────────────────────

func storageErrorScenarios(r *runner, f *fixtures) {
	cfg := r.cfg
	var client *storage.Client
	get := func(ctx context.Context) (*storage.Client, error) {
		if client == nil {
			c, err := newStorageClient(ctx)
			if err != nil {
				return nil, err
			}
			client = c
		}
		return client, nil
	}
	ensureBucket := func(ctx context.Context) error {
		c, err := get(ctx)
		if err != nil {
			return err
		}
		if err := c.Bucket(f.bucket).Create(ctx, cfg.Project, &storage.BucketAttrs{Location: "US"}); err != nil && !isConflict(err) {
			return fmt.Errorf("create bucket: %w", err)
		}
		return nil
	}

	r.run("errors.storage_already_exists", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		err := c.Bucket(f.bucket).Create(ctx, cfg.Project, &storage.BucketAttrs{Location: "US"})
		if err := requireStatus(err, http.StatusConflict); err != nil {
			return "", "", err
		}
		return "already_exists=yes", "second bucket create surfaced 409 ALREADY_EXISTS", nil
	})

	r.run("errors.storage_not_found", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		_, err := c.Bucket(f.bucket).Object("no/such/object").Attrs(ctx)
		if err := requireStatus(err, http.StatusNotFound); err != nil {
			return "", "", err
		}
		return "not_found=yes", "missing object surfaced 404 NOT_FOUND", nil
	})

	// A 404 must not be retried: the emulator must see exactly one request.
	r.run("errors.no_retry_on_4xx", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		n, err := countAttempts(cfg.REST, "storage", func() error {
			_, err := c.Bucket(f.bucket).Object("no/such/object").Attrs(ctx)
			return requireStatus(err, http.StatusNotFound)
		})
		if err != nil {
			return "", "", err
		}
		if n != 1 {
			return "", "", fmt.Errorf("4xx must not be retried: emulator saw %d attempts, want 1", n)
		}
		return "no_retry_attempts=1", "404 failed after exactly one attempt", nil
	})

	// IAM optimistic concurrency: a stale policy etag must be rejected.
	r.run("errors.iam_failed_precondition", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		h := c.Bucket(f.bucket).IAM()
		pol, err := h.Policy(ctx)
		if err != nil {
			return "", "", fmt.Errorf("get iam policy: %w", err)
		}
		if pol.InternalProto == nil {
			pol.InternalProto = &iampb.Policy{}
		}
		pol.InternalProto.Etag = []byte("stale-etag")
		if err := h.SetPolicy(ctx, pol); err == nil {
			return "", "", fmt.Errorf("stale IAM etag was accepted (want 409 FAILED_PRECONDITION)")
		} else if serr := requireStatus(err, http.StatusConflict); serr != nil {
			return "", "", serr
		}
		return "failed_precondition=yes", "stale IAM etag surfaced 409", nil
	})
}

// ─── throttled retry scenarios ───────────────────────────────────────────────

func throttleScenarios(r *runner, f *fixtures) {
	cfg := r.cfg
	var client *storage.Client
	get := func(ctx context.Context) (*storage.Client, error) {
		if client == nil {
			c, err := newStorageClient(ctx)
			if err != nil {
				return nil, err
			}
			client = c
		}
		return client, nil
	}
	ensureBucket := func(ctx context.Context) error {
		c, err := get(ctx)
		if err != nil {
			return err
		}
		if err := c.Bucket(f.bucket).Create(ctx, cfg.Project, &storage.BucketAttrs{Location: "US"}); err != nil && !isConflict(err) {
			return fmt.Errorf("create bucket: %w", err)
		}
		return nil
	}

	for _, tc := range []struct {
		name   string
		status int
		obs    string
	}{
		{"errors.retry_429_storage_get", http.StatusTooManyRequests, "retry_429_attempts=2"},
		{"errors.retry_503_storage_get", http.StatusServiceUnavailable, "retry_503_attempts=2"},
	} {
		tc := tc
		r.run(tc.name, "OK", func(ctx context.Context) (string, string, error) {
			if err := ensureBucket(ctx); err != nil {
				return "", "", err
			}
			c, _ := get(ctx)
			if err := armThrottle(cfg.REST, fmt.Sprintf(
				`{"mode":"fault","failFirst":1,"services":["storage"],"status":%d,"retryDelay":"1s"}`, tc.status)); err != nil {
				return "", "", err
			}
			defer clearThrottle(cfg.REST)
			n, err := countAttempts(cfg.REST, "storage", func() error {
				if _, err := c.Bucket(f.bucket).Attrs(ctx); err != nil {
					return fmt.Errorf("get bucket attrs after injected %d: %w", tc.status, err)
				}
				return nil
			})
			if err != nil {
				return "", "", err
			}
			if n != 2 {
				return "", "", fmt.Errorf("client made %d attempts, want 2 (one retry)", n)
			}
			return tc.obs, fmt.Sprintf("injected %d refused the first attempt; the client retried once and succeeded", tc.status), nil
		})
	}

	// Retry-info shape: the raw 429 envelope must carry the Retry-After header
	// and a google.rpc.RetryInfo detail, as real GCP does.
	r.run("errors.retry_info_shape", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		if err := armThrottle(cfg.REST, `{"mode":"fault","failFirst":1,"services":["storage"],"status":429,"retryDelay":"1s"}`); err != nil {
			return "", "", err
		}
		defer clearThrottle(cfg.REST)
		code, hdr, body, err := rawGET(cfg.REST + "/storage/v1/b/" + f.bucket)
		if err != nil {
			return "", "", err
		}
		if code != http.StatusTooManyRequests {
			return "", "", fmt.Errorf("want 429, got %d: %s", code, body)
		}
		var env map[string]any
		if err := json.Unmarshal(body, &env); err != nil {
			return "", "", fmt.Errorf("error body is not JSON: %w (%s)", err, body)
		}
		delay, hasRetryInfo := retryInfoDelay(env)
		if !hasRetryInfo {
			return "", "", fmt.Errorf("no google.rpc.RetryInfo detail in %s", body)
		}
		retryAfter := hdr.Get("Retry-After")
		if retryAfter == "" {
			return "", "", fmt.Errorf("no Retry-After header (headers=%v)", hdr)
		}
		return fmt.Sprintf("RetryInfo:%s:Retry-After=%s", delay, retryAfter),
			"injected 429 carried Retry-After + google.rpc.RetryInfo", nil
	})

	// Pagination stability: page tokens must survive a throttled retry.
	r.run("errors.pagination_stability", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		const total = 5
		for i := 0; i < total; i++ {
			w := c.Bucket(f.bucket).Object(fmt.Sprintf("errors/page/%02d.txt", i)).NewWriter(ctx)
			if _, err := w.Write([]byte(fmt.Sprintf("page-%d", i))); err != nil {
				return "", "", err
			}
			if err := w.Close(); err != nil {
				return "", "", err
			}
		}
		if err := armThrottle(cfg.REST, `{"mode":"fault","failFirst":1,"services":["storage"],"status":429,"retryDelay":"1s"}`); err != nil {
			return "", "", err
		}
		defer clearThrottle(cfg.REST)
		it := c.Bucket(f.bucket).Objects(ctx, &storage.Query{Prefix: "errors/page/"})
		pager := iterator.NewPager(it, 2, "")
		seen := 0
		for {
			var page []*storage.ObjectAttrs
			next, err := pager.NextPage(&page)
			if err != nil {
				return "", "", fmt.Errorf("paginate across a throttled retry: %w", err)
			}
			seen += len(page)
			if next == "" {
				break
			}
		}
		if seen != total {
			return "", "", fmt.Errorf("listed %d objects across pages, want %d", seen, total)
		}
		return fmt.Sprintf("pagination_total=%d", total), "page tokens survived a throttled retry", nil
	})

	// Resumable rewind: a refused chunk must be re-sent and still checksum.
	r.run("errors.resumable_rewind", "OK", func(ctx context.Context) (string, string, error) {
		if err := ensureBucket(ctx); err != nil {
			return "", "", err
		}
		c, _ := get(ctx)
		// Scope the injected fault to the chunk PUT alone so the resumable
		// session start is unaffected: exactly a mid-upload failure.
		if err := armThrottle(cfg.REST, `{"mode":"fault","failFirst":1,"services":["storage/objectsinsertresumable"],"status":429,"retryDelay":"1s"}`); err != nil {
			return "", "", err
		}
		defer clearThrottle(cfg.REST)
		payload := resumablePayload()
		w := c.Bucket(f.bucket).Object("errors/rewind.bin").NewWriter(ctx)
		w.ChunkSize = 256 * 1024
		w.ContentType = "application/octet-stream"
		if _, err := w.Write(payload); err != nil {
			return "", "", fmt.Errorf("resumable write after mid-upload 429: %w", err)
		}
		if err := w.Close(); err != nil {
			return "", "", fmt.Errorf("resumable close after mid-upload 429: %w", err)
		}
		attrs, err := c.Bucket(f.bucket).Object("errors/rewind.bin").Attrs(ctx)
		if err != nil {
			return "", "", err
		}
		if attrs.Size != int64(len(payload)) {
			return "", "", fmt.Errorf("rewound object size %d, want %d", attrs.Size, len(payload))
		}
		rd, err := c.Bucket(f.bucket).Object("errors/rewind.bin").NewReader(ctx)
		if err != nil {
			return "", "", err
		}
		defer rd.Close()
		body, err := io.ReadAll(rd)
		if err != nil {
			return "", "", err
		}
		got := sha256Hex(payload)
		if sum := sha256Hex(body); sum != got {
			return "", "", fmt.Errorf("rewound checksum %s, want %s", sum, got)
		}
		return "resumable_sha256=" + got, "a refused chunk was re-sent and the object checksum matches", nil
	})
}

// retryInfoDelay finds the google.rpc.RetryInfo detail in a Google JSON error
// envelope and returns its retryDelay.
func retryInfoDelay(env map[string]any) (string, bool) {
	errObj, _ := env["error"].(map[string]any)
	if errObj == nil {
		return "", false
	}
	details, _ := errObj["details"].([]any)
	for _, d := range details {
		dm, _ := d.(map[string]any)
		if dm == nil {
			continue
		}
		if t, _ := dm["@type"].(string); t == "type.googleapis.com/google.rpc.RetryInfo" {
			if s, _ := dm["retryDelay"].(string); s != "" {
				return s, true
			}
			return "0s", true
		}
	}
	return "", false
}

// ─── idempotency ─────────────────────────────────────────────────────────────

func idempotencyScenarios(r *runner, f *fixtures) {
	project := r.cfg.Project
	dsID := strings.ReplaceAll(rid(r.cfg, "errors_ds"), "-", "_")
	tblID := strings.ReplaceAll(rid(r.cfg, "errors_tbl"), "-", "_")

	// A malformed request must surface INVALID_ARGUMENT synchronously.
	r.run("errors.invalid_argument", "OK", func(ctx context.Context) (string, string, error) {
		svc, err := newBigQueryService(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		_, err = svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        "SELECT * FROM",
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		if err := requireStatus(err, http.StatusBadRequest); err != nil {
			return "", "", err
		}
		return "invalid_argument=yes", "malformed SQL surfaced 400 INVALID_ARGUMENT", nil
	})

	// insertId is the API's client-supplied idempotency key: replaying a row
	// with the same insertId must not create a second row.
	r.run("errors.idempotent_insertall", "OK", func(ctx context.Context) (string, string, error) {
		svc, err := newBigQueryService(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		if _, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: dsID},
			FriendlyName:     "sdk-tour-errors",
		}).Do(); err != nil && !isConflict(err) {
			return "", "", fmt.Errorf("create dataset: %w", err)
		}
		if _, err := svc.Tables.Insert(project, dsID, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: dsID, TableId: tblID},
			Schema:         bqSchema(),
		}).Do(); err != nil && !isConflict(err) {
			return "", "", fmt.Errorf("create table: %w", err)
		}
		ins := func() ([]string, error) {
			resp, err := svc.Tabledata.InsertAll(project, dsID, tblID, &bigquery.TableDataInsertAllRequest{
				Rows: []*bigquery.TableDataInsertAllRequestRows{
					{InsertId: "idem-1", Json: map[string]bigquery.JsonValue{"id": 1, "name": "a"}},
				},
			}).Do()
			if err != nil {
				return nil, fmt.Errorf("insertAll: %w", err)
			}
			var reasons []string
			for _, ie := range resp.InsertErrors {
				for _, e := range ie.Errors {
					reasons = append(reasons, e.Reason)
				}
			}
			return reasons, nil
		}
		if reasons, err := ins(); err != nil {
			return "", "", err
		} else if len(reasons) != 0 {
			return "", "", fmt.Errorf("first insertAll unexpectedly errored: %v", reasons)
		}
		reasons, err := ins()
		if err != nil {
			return "", "", err
		}
		// Real GCP's insertAll dedup is best-effort: it may silently accept the
		// replayed row (empty insertErrors) or report it as a duplicate. Either
		// is fine; what must hold is that no second row exists.
		for _, r := range reasons {
			if r != "duplicate" {
				return "", "", fmt.Errorf("replayed insertAll returned unexpected error %q (want duplicate): %v", r, reasons)
			}
		}
		data, err := svc.Tabledata.List(project, dsID, tblID).Do()
		if err != nil {
			return "", "", fmt.Errorf("list rows: %w", err)
		}
		if len(data.Rows) != 1 {
			return "", "", fmt.Errorf("idempotent insert produced %d rows, want 1", len(data.Rows))
		}
		return "idempotent_rows=1", "replayed insertId was de-duplicated (one stored row)", nil
	})
}

// ─── gRPC error mapping ──────────────────────────────────────────────────────

func pubsubErrorScenarios(r *runner, f *fixtures) {
	r.run("errors.pubsub_topic_already_exists", "OK", func(ctx context.Context) (string, string, error) {
		client, err := newPubSubClient(ctx, r.cfg)
		if err != nil {
			return "", "", err
		}
		defer client.Close()
		if _, err := client.CreateTopic(ctx, f.topic); err != nil {
			return "", "", fmt.Errorf("create topic: %w", err)
		}
		_, err = client.CreateTopic(ctx, f.topic)
		if err := requireGRPCCode(err, codes.AlreadyExists); err != nil {
			return "", "", err
		}
		return "grpc_already_exists=yes", "duplicate topic create surfaced gRPC ALREADY_EXISTS", nil
	})
}
