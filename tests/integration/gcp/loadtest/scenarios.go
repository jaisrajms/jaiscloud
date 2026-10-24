package loadtest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// The three hot paths share one project id ("proj"); GCS buckets, Pub/Sub
// resources and Firestore documents all live under it. Names are fixed so a
// rerun against a live emulator overwrites rather than accumulates.
const (
	project          = "proj"
	gcsBucket        = "load-gcs"
	pubTopic         = "load-topic"
	pubSubscriptions = 3
	firestoreDB      = "projects/" + project + "/databases/(default)"
)

// api is a thin JSON/bytes client over the emulator's REST surface.
type api struct {
	base   string
	client *http.Client
}

func pubSubName(i int) string { return fmt.Sprintf("load-sub-%d", i) }

// do issues one request and returns the HTTP status and body. A nil body sends
// no body; a non-nil body defaults Content-Type to application/json (overridable
// via hdr, e.g. text/plain for a media upload).
func (a *api) do(ctx context.Context, method, path string, body []byte, hdr map[string]string) (int, []byte, error) {
	var r *bytes.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	var req *http.Request
	var err error
	if r != nil {
		req, err = http.NewRequestWithContext(ctx, method, a.base+path, r)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, a.base+path, nil)
	}
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, buf.Bytes(), nil
}

// seed creates the bucket, topic and fan-out subscriptions every scenario
// needs, so seeding is not part of the measured window.
func seed(ctx context.Context, a *api) error {
	st, body, err := a.do(ctx, http.MethodPost,
		"/storage/v1/b?project="+project, []byte(`{"name":"`+gcsBucket+`"}`), nil)
	if err != nil {
		return err
	}
	if st != http.StatusOK {
		return fmt.Errorf("create bucket: status %d: %s", st, body)
	}
	st, body, err = a.do(ctx, http.MethodPut, "/v1/projects/"+project+"/topics/"+pubTopic, []byte(`{}`), nil)
	if err != nil {
		return err
	}
	if st != http.StatusOK {
		return fmt.Errorf("create topic: status %d: %s", st, body)
	}
	for i := 0; i < pubSubscriptions; i++ {
		sub := pubSubName(i)
		reqBody := fmt.Sprintf(`{"topic":"projects/%s/topics/%s"}`, project, pubTopic)
		st, body, err = a.do(ctx, http.MethodPut,
			"/v1/projects/"+project+"/subscriptions/"+sub, []byte(reqBody), nil)
		if err != nil {
			return err
		}
		if st != http.StatusOK {
			return fmt.Errorf("create subscription %s: status %d: %s", sub, st, body)
		}
	}
	return nil
}

// ─── Pub/Sub: bounded fan-out correctness ────────────────────────────────────

// verifyFanout publishes a bounded batch of uniquely-marked messages and asserts
// every pull subscription received a copy of every message. It is the
// deterministic fan-out contract check that runs once per gate (the load window
// then hammers publish/pull without tracking individual markers).
func verifyFanout(ctx context.Context, a *api, messages int) error {
	published := make([]string, 0, messages)
	for i := 0; i < messages; i++ {
		marker := fmt.Sprintf("fanout-%d-%d", time.Now().UnixNano(), i)
		data := base64.StdEncoding.EncodeToString([]byte(marker))
		st, body, err := a.do(ctx, http.MethodPost,
			"/v1/projects/"+project+"/topics/"+pubTopic+":publish",
			[]byte(`{"messages":[{"data":"`+data+`"}]}`), nil)
		if err != nil {
			return err
		}
		if st != http.StatusOK {
			return fmt.Errorf("publish %q: status %d: %s", marker, st, body)
		}
		published = append(published, marker)
	}
	for i := 0; i < pubSubscriptions; i++ {
		sub := pubSubName(i)
		got, err := drainSubscription(ctx, a, sub)
		if err != nil {
			return fmt.Errorf("drain %s: %w", sub, err)
		}
		for _, marker := range published {
			if !got[marker] {
				return fmt.Errorf("subscription %s did not receive fan-out copy %q", sub, marker)
			}
		}
	}
	return nil
}

// drainSubscription pulls a subscription to empty (bounded retries), acking as
// it goes, and returns the set of decoded message payloads.
func drainSubscription(ctx context.Context, a *api, sub string) (map[string]bool, error) {
	got := map[string]bool{}
	emptyRounds := 0
	for pulls := 0; pulls < 1000 && emptyRounds < 3; pulls++ {
		st, body, err := a.do(ctx, http.MethodPost,
			"/v1/projects/"+project+"/subscriptions/"+sub+":pull",
			[]byte(`{"maxMessages":1000,"returnImmediately":true}`), nil)
		if err != nil {
			return nil, err
		}
		if st != http.StatusOK {
			return nil, fmt.Errorf("pull: status %d: %s", st, body)
		}
		var out struct {
			Received []struct {
				AckID   string `json:"ackId"`
				Message struct {
					Data string `json:"data"`
				} `json:"message"`
			} `json:"receivedMessages"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, fmt.Errorf("decode pull response: %w", err)
		}
		if len(out.Received) == 0 {
			emptyRounds++
			continue
		}
		emptyRounds = 0
		ackIDs := make([]string, 0, len(out.Received))
		for _, m := range out.Received {
			raw, err := base64.StdEncoding.DecodeString(m.Message.Data)
			if err != nil {
				return nil, fmt.Errorf("decode message data: %w", err)
			}
			got[string(raw)] = true
			ackIDs = append(ackIDs, m.AckID)
		}
		ackBody, err := json.Marshal(map[string]any{"ackIds": ackIDs})
		if err != nil {
			return nil, err
		}
		if st, body, err := a.do(ctx, http.MethodPost,
			"/v1/projects/"+project+"/subscriptions/"+sub+":acknowledge", ackBody, nil); err != nil {
			return nil, err
		} else if st != http.StatusOK {
			return nil, fmt.Errorf("acknowledge: status %d: %s", st, body)
		}
	}
	return got, nil
}

// ─── The three load windows ──────────────────────────────────────────────────

// loadGCS hammers object PUT/GET at full concurrency. Each worker owns one
// object name (so writes overwrite rather than accumulate) and asserts every
// download matches the bytes it just uploaded.
func loadGCS(ctx context.Context, a *api, concurrency int, window time.Duration) *LoadResult {
	payload := bytes.Repeat([]byte("jaiscloud-load-"), 256) // 4 KiB
	rctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	return RunLoad(rctx, concurrency, func(status int) bool { return status == http.StatusOK },
		func(worker int) (int, error) {
			reqCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
			defer c()
			obj := fmt.Sprintf("load-obj-%d", worker)
			st, body, err := a.do(reqCtx, http.MethodPost,
				"/upload/storage/v1/b/"+gcsBucket+"/o?uploadType=media&name="+obj,
				payload, map[string]string{"Content-Type": "text/plain"})
			if err != nil {
				return 0, err
			}
			if st != http.StatusOK {
				return st, nil
			}
			st, body, err = a.do(reqCtx, http.MethodGet,
				"/storage/v1/b/"+gcsBucket+"/o/"+obj+"?alt=media", nil, nil)
			if err != nil {
				return 0, err
			}
			if st != http.StatusOK {
				return st, nil
			}
			if !bytes.Equal(body, payload) {
				return st, fmt.Errorf("object %s round-trip mismatch: got %d bytes, want %d", obj, len(body), len(payload))
			}
			return st, nil
		})
}

// loadPubSub hammers publish (bounded, so the fan-out backlog stays finite) and
// the pull/ack read path at full concurrency.
func loadPubSub(ctx context.Context, a *api, concurrency int, window time.Duration, maxPublish int64) *LoadResult {
	rctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	var published int64
	return RunLoad(rctx, concurrency, func(status int) bool { return status == http.StatusOK },
		func(worker int) (int, error) {
			reqCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
			defer c()
			if seq := atomic.AddInt64(&published, 1); seq <= maxPublish {
				data := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("load-%d", seq)))
				body, _ := json.Marshal(map[string]any{"messages": []any{map[string]any{"data": data}}})
				st, b, err := a.do(reqCtx, http.MethodPost,
					"/v1/projects/"+project+"/topics/"+pubTopic+":publish", body, nil)
				if err != nil {
					return 0, err
				}
				if st != http.StatusOK {
					return st, nil
				}
				var resp struct {
					MessageIDs []string `json:"messageIds"`
				}
				if err := json.Unmarshal(b, &resp); err != nil || len(resp.MessageIDs) != 1 {
					return st, fmt.Errorf("publish response: %s", b)
				}
			}

			sub := pubSubName(worker % pubSubscriptions)
			st, body, err := a.do(reqCtx, http.MethodPost,
				"/v1/projects/"+project+"/subscriptions/"+sub+":pull",
				[]byte(`{"maxMessages":50,"returnImmediately":true}`), nil)
			if err != nil {
				return 0, err
			}
			if st != http.StatusOK {
				return st, nil
			}
			var out struct {
				Received []struct {
					AckID string `json:"ackId"`
				} `json:"receivedMessages"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				return st, fmt.Errorf("decode pull response: %w", err)
			}
			if len(out.Received) > 0 {
				ids := make([]string, 0, len(out.Received))
				for _, m := range out.Received {
					ids = append(ids, m.AckID)
				}
				ackBody, _ := json.Marshal(map[string]any{"ackIds": ids})
				ast, abody, err := a.do(reqCtx, http.MethodPost,
					"/v1/projects/"+project+"/subscriptions/"+sub+":acknowledge", ackBody, nil)
				if err != nil {
					return 0, err
				}
				if ast != http.StatusOK {
					return ast, fmt.Errorf("acknowledge %s: %s", sub, abody)
				}
			}
			return st, nil
		})
}

// loadFirestore hammers documents:commit. Each worker owns one document (so the
// store stays bounded) and asserts the commit envelope carries a commitTime and
// one write result.
func loadFirestore(ctx context.Context, a *api, concurrency int, window time.Duration) *LoadResult {
	commitPath := "/v1/" + firestoreDB + "/documents:commit"
	rctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	var seq int64
	return RunLoad(rctx, concurrency, func(status int) bool { return status == http.StatusOK },
		func(worker int) (int, error) {
			reqCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
			defer c()
			name := fmt.Sprintf("%s/documents/loaddoc/load-doc-%d", firestoreDB, worker)
			body, _ := json.Marshal(map[string]any{
				"writes": []any{map[string]any{
					"update": map[string]any{
						"name": name,
						"fields": map[string]any{
							"n": map[string]any{"integerValue": strconv.FormatInt(atomic.AddInt64(&seq, 1), 10)},
						},
					},
				}},
			})
			st, b, err := a.do(reqCtx, http.MethodPost, commitPath, body, nil)
			if err != nil {
				return 0, err
			}
			if st != http.StatusOK {
				return st, nil
			}
			var resp struct {
				CommitTime   string `json:"commitTime"`
				WriteResults []any  `json:"writeResults"`
			}
			if err := json.Unmarshal(b, &resp); err != nil {
				return st, fmt.Errorf("decode commit response: %w", err)
			}
			if resp.CommitTime == "" || len(resp.WriteResults) != 1 {
				return st, fmt.Errorf("commit response missing commitTime/writeResults: %s", strings.TrimSpace(string(b)))
			}
			return st, nil
		})
}
