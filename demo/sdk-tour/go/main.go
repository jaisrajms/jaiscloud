// Command sdk-tour runs one realistic workflow against the jaiscloud-gcp
// emulator using the OFFICIAL Google Cloud Go clients, and emits one JSONL
// record per scenario so run.sh can build a cross-language PASS/FAIL matrix.
//
// It is a demo/compliance artifact, not a conformance gate: every scenario is
// run the same way in Go, Python, Java and Node, and the observable result is
// normalized so the four languages can be compared. Nothing here paper-overs a
// failure; a scenario that cannot be wired or is not implemented is recorded
// with a classification (emulator-bug / wiring-gap / unimplemented).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config mirrors the env contract run.sh establishes for every language.
type Config struct {
	REST    string // emulator REST base, no trailing slash
	GRPC    string // emulator gRPC host:port
	HMS     string // emulator HMS host:port
	Project string
	RunID   string // per-run unique suffix so repeated runs never collide
	Lang    string
	Mode    string // "tour" (default) or "errors"
	Out     string // JSONL results path ("" = stdout only)
}

func configFromEnv() Config {
	get := func(k, def string) string {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
		return def
	}
	return Config{
		REST:    strings.TrimRight(get("EMULATOR_REST", "http://localhost:8080"), "/"),
		GRPC:    strings.TrimSpace(get("EMULATOR_GRPC", "localhost:8081")),
		HMS:     strings.TrimSpace(get("EMULATOR_HMS", "localhost:9083")),
		Project: get("PROJECT_ID", "jaiscloud-project"),
		RunID:   get("SDK_TOUR_RUN_ID", "local"),
		Lang:    "go",
		Mode:    get("SDK_TOUR_MODE", "tour"),
		Out:     strings.TrimSpace(os.Getenv("SDK_TOUR_RESULTS")),
	}
}

// Record is the cross-language matrix row. Status is OK|FAIL|SKIP; Expect is
// the pre-declared expectation (OK|SKIP) so an unimplemented surface is a
// documented gap rather than a hard failure.
type Record struct {
	Lang       string `json:"lang"`
	Scenario   string `json:"scenario"`
	Status     string `json:"status"`
	Expect     string `json:"expect"`
	Observable string `json:"observable,omitempty"`
	Detail     string `json:"detail,omitempty"`
	Error      string `json:"error,omitempty"`
	Class      string `json:"classification,omitempty"`
}

type recorder struct {
	cfg  Config
	file *os.File
}

func newRecorder(cfg Config) *recorder {
	r := &recorder{cfg: cfg}
	if cfg.Out != "" {
		f, err := os.Create(cfg.Out)
		if err == nil {
			r.file = f
		}
	}
	return r
}

func (r *recorder) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// record writes the human line (`LANG service.op OK|FAIL|SKIP detail`) and the
// JSONL row run.sh aggregates.
func (r *recorder) record(rec Record) {
	line := fmt.Sprintf("%s %s %s", strings.ToUpper(rec.Lang), rec.Scenario, rec.Status)
	if rec.Detail != "" {
		line += " " + rec.Detail
	}
	if rec.Error != "" {
		line += " err=" + rec.Error
	}
	fmt.Println(line)
	if r.file != nil {
		if b, err := json.Marshal(rec); err == nil {
			fmt.Fprintln(r.file, string(b))
		}
	}
}

// classify attempts to say WHY a scenario failed: an unimplemented surface, a
// client→emulator wiring gap (auth/TLS/connection), or a probable emulator bug.
func classify(err error) string {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unimplemented:
			return "unimplemented"
		case codes.Unavailable, codes.Unauthenticated, codes.PermissionDenied:
			return "wiring-gap"
		}
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case 501, 404:
			if gerr.Code == 501 {
				return "unimplemented"
			}
		case 401, 403:
			return "wiring-gap"
		}
	}
	var aerr *apierror.APIError
	if errors.As(err, &aerr) && aerr.GRPCStatus() != nil {
		switch aerr.GRPCStatus().Code() {
		case codes.Unimplemented:
			return "unimplemented"
		case codes.Unavailable, codes.Unauthenticated, codes.PermissionDenied:
			return "wiring-gap"
		}
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{"connection refused", "no such host", "tls", "handshake", "dial tcp", "transport: authentication"} {
		if strings.Contains(msg, needle) {
			return "wiring-gap"
		}
	}
	if strings.Contains(msg, "not implemented") || strings.Contains(msg, "unimplemented") {
		return "unimplemented"
	}
	return "emulator-bug"
}

// runner executes scenarios in order and records each one. Fixtures shared by
// scenarios are created by the first scenario that needs them; a broken fixture
// naturally fails its dependents (reported, not hidden).
type runner struct {
	cfg Config
	rec *recorder
}

func (r *runner) run(name, expect string, fn func(context.Context) (observable, detail string, err error)) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	var obs, detail string
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic: %v", p)
			}
		}()
		obs, detail, err = fn(ctx)
	}()
	rec := Record{Lang: r.cfg.Lang, Scenario: name, Expect: expect}
	switch {
	case err == nil:
		rec.Status = "OK"
		rec.Observable = obs
		rec.Detail = detail
	case expect == "SKIP":
		rec.Status = "SKIP"
		rec.Error = err.Error()
		rec.Class = classify(err)
	default:
		rec.Status = "FAIL"
		rec.Error = err.Error()
		rec.Class = classify(err)
	}
	if rec.Detail == "" {
		rec.Detail = fmt.Sprintf("%dms", time.Since(start).Milliseconds())
	}
	r.rec.record(rec)
}

func main() {
	cfg := configFromEnv()
	// The Storage/Pub/Sub/Firestore Go clients pick up their emulator hook from
	// the environment; set it here so the same binary works when only
	// EMULATOR_REST/GRPC are provided.
	setDefault("STORAGE_EMULATOR_HOST", cfg.REST)
	setDefault("PUBSUB_EMULATOR_HOST", cfg.GRPC)
	setDefault("FIRESTORE_EMULATOR_HOST", cfg.GRPC)

	rec := newRecorder(cfg)
	defer rec.close()
	r := &runner{cfg: cfg, rec: rec}

	if cfg.Mode == "errors" {
		runErrors(r)
		return
	}
	if cfg.Mode == "streaming" {
		runStreaming(r)
		return
	}
	runAll(r)
}

func setDefault(k, v string) {
	if strings.TrimSpace(os.Getenv(k)) == "" {
		_ = os.Setenv(k, v)
	}
}
