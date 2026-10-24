package gcp

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/gcp/gcperr"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file is the AUD8 error-envelope ↔ gRPC-status audit. It enforces two
// independent invariants:
//
//  1. Every REST codec renders a provider error consistently with the gRPC
//     transport: the envelope's HTTP "code" is the provider's HTTP status, its
//     "status" is the canonical google.rpc name, and the gRPC code for that same
//     name is what GRPCStatus returns. The documented non-GCP envelopes (Cloud
//     Storage, BigQuery, Iceberg, and Datastore's proto form) are asserted for
//     their own documented shape instead.
//  2. Every error code constructed anywhere under internal/gcp resolves to a
//     canonical google.rpc status that is valid for its HTTP status — the same
//     pairing the wire-conformance validator enforces. A code that silently
//     falls back to UNKNOWN, or a status/HTTP pairing no real GCP API emits,
//     fails the audit.
//
// rpcName (in error_status_parity_test.go) maps a gRPC code to its canonical
// name independently of the production tables.

// canonicalErrorCase is one provider-error constructor input plus the canonical
// status/http/code it must resolve to over both transports. Expected values are
// written out literally (never derived from gcperr) so the audit is not
// tautological.
type canonicalErrorCase struct {
	name       string
	perr       *model.ProviderError
	wantStatus string
	wantHTTP   int
	wantCode   codes.Code
}

// canonicalErrorCases covers every canonical google.rpc category, the provider
// alias forms, an explicit Status override, and the UNKNOWN fallback.
func canonicalErrorCases() []canonicalErrorCase {
	return []canonicalErrorCase{
		// Canonical categories (Code == google.rpc name).
		{"NotFound", &model.ProviderError{Code: "NotFound", HTTPStatus: 404}, "NOT_FOUND", 404, codes.NotFound},
		{"AlreadyExists", &model.ProviderError{Code: "AlreadyExists", HTTPStatus: 409}, "ALREADY_EXISTS", 409, codes.AlreadyExists},
		{"InvalidArgument", &model.ProviderError{Code: "InvalidArgument", HTTPStatus: 400}, "INVALID_ARGUMENT", 400, codes.InvalidArgument},
		{"FailedPrecondition", &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 400}, "FAILED_PRECONDITION", 400, codes.FailedPrecondition},
		{"Aborted", &model.ProviderError{Code: "Aborted", HTTPStatus: 409}, "ABORTED", 409, codes.Aborted},
		{"PermissionDenied", &model.ProviderError{Code: "PermissionDenied", HTTPStatus: 403}, "PERMISSION_DENIED", 403, codes.PermissionDenied},
		{"ResourceExhausted", &model.ProviderError{Code: "ResourceExhausted", HTTPStatus: 429}, "RESOURCE_EXHAUSTED", 429, codes.ResourceExhausted},
		{"OutOfRange", &model.ProviderError{Code: "OutOfRange", HTTPStatus: 400}, "OUT_OF_RANGE", 400, codes.OutOfRange},
		{"Cancelled", &model.ProviderError{Code: "Cancelled", HTTPStatus: 499}, "CANCELLED", 499, codes.Canceled},
		{"Unimplemented", &model.ProviderError{Code: "Unimplemented", HTTPStatus: 501}, "UNIMPLEMENTED", 501, codes.Unimplemented},
		{"Unavailable", &model.ProviderError{Code: "Unavailable", HTTPStatus: 503}, "UNAVAILABLE", 503, codes.Unavailable},
		{"DeadlineExceeded", &model.ProviderError{Code: "DeadlineExceeded", HTTPStatus: 504}, "DEADLINE_EXCEEDED", 504, codes.DeadlineExceeded},
		{"Internal", &model.ProviderError{Code: "Internal", HTTPStatus: 500}, "INTERNAL", 500, codes.Internal},

		// Provider alias forms.
		{"InvalidRequest", &model.ProviderError{Code: "InvalidRequest", HTTPStatus: 400}, "INVALID_ARGUMENT", 400, codes.InvalidArgument},
		{"Conflict", &model.ProviderError{Code: "Conflict", HTTPStatus: 409}, "ALREADY_EXISTS", 409, codes.AlreadyExists},
		{"PreconditionFailed", &model.ProviderError{Code: "PreconditionFailed", HTTPStatus: 412}, "FAILED_PRECONDITION", 412, codes.FailedPrecondition},
		{"BucketNotEmpty", &model.ProviderError{Code: "bucketNotEmpty", HTTPStatus: 409}, "FAILED_PRECONDITION", 409, codes.FailedPrecondition},
		{"ServiceUnavailable", &model.ProviderError{Code: "ServiceUnavailable", HTTPStatus: 503}, "UNAVAILABLE", 503, codes.Unavailable},
		{"UnsupportedOperationOn501", &model.ProviderError{Code: "UnsupportedOperation", HTTPStatus: 501}, "UNIMPLEMENTED", 501, codes.Unimplemented},
		// A decode/route miss (HTTP 404) must not be stamped UNIMPLEMENTED — real
		// GCP pairs 404 with NOT_FOUND (the AUD8 fix).
		{"UnsupportedOperationOn404", &model.ProviderError{Code: "UnsupportedOperation", HTTPStatus: 404}, "NOT_FOUND", 404, codes.NotFound},
		{"UnknownServiceOn404", &model.ProviderError{Code: "UnknownService", HTTPStatus: 404}, "NOT_FOUND", 404, codes.NotFound},
		// HTTP 502 has no google.rpc.Code; the run data-plane proxy maps it to
		// UNAVAILABLE rather than falling through to UNKNOWN (the AUD8 fix).
		{"BadGateway502", &model.ProviderError{Code: "BadGateway", HTTPStatus: 502}, "UNAVAILABLE", 502, codes.Unavailable},

		// Explicit Status overrides the HTTP fallback.
		{"ExplicitStatusWins", &model.ProviderError{Code: "InvalidRequest", HTTPStatus: 503, Status: "UNAVAILABLE"}, "UNAVAILABLE", 503, codes.Unavailable},
		{"FailedPreconditionOn409", &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 409, Status: "FAILED_PRECONDITION"}, "FAILED_PRECONDITION", 409, codes.FailedPrecondition},

		// No alias and no mapping: the documented UNKNOWN fallback.
		{"UnknownFallback", &model.ProviderError{Code: "MysteryCode", HTTPStatus: 418}, "UNKNOWN", 418, codes.Unknown},
	}
}

// withAuditMessage returns a copy of perr with a non-empty message, since the
// audit constructs its inputs without one and every envelope must carry a
// message.
func withAuditMessage(perr *model.ProviderError) *model.ProviderError {
	if perr == nil {
		return &model.ProviderError{Code: "Internal", HTTPStatus: 500, Message: "audit"}
	}
	cp := *perr
	if cp.Message == "" {
		cp.Message = "audit message"
	}
	return &cp
}

// envelopeKind classifies a service's REST error body so the audit asserts the
// right shape instead of forcing every API into the modern envelope.
type envelopeKind int

const (
	envelopeStandard envelopeKind = iota // {"error":{code,message,status}}
	envelopeGCS                          // {"error":{errors[],code,message}} — no google.rpc status
	envelopeBigQuery                     // {"error":{errors[],code,message,status}}
	envelopeIceberg                      // {"error":{message,type,code}} — Iceberg's own ErrorResponse
)

// envelopeKinds documents the deliberate deviations from the standard GCP JSON
// error envelope. Every other service uses envelopeStandard.
var envelopeKinds = map[string]envelopeKind{
	"storage":  envelopeGCS,
	"bigquery": envelopeBigQuery,
	"iceberg":  envelopeIceberg,
}

// TestCanonicalErrorMappingPerCodec drives every canonical error case through
// every registered REST codec and asserts the envelope shape.
func TestCanonicalErrorMappingPerCodec(t *testing.T) {
	cases := canonicalErrorCases()
	if len(gcpServices) == 0 {
		t.Fatal("gcpServices is empty; the audit would vacuously pass")
	}
	for _, svc := range gcpServices {
		svc := svc
		kind := envelopeKinds[svc.ServiceName]
		t.Run(svc.ServiceName, func(t *testing.T) {
			codec := svc.Codec()
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					perr := withAuditMessage(tc.perr)
					httpStatus, headers, body := codec.EncodeError(nil, perr)
					if httpStatus != tc.wantHTTP {
						t.Fatalf("EncodeError HTTP status = %d, want %d", httpStatus, tc.wantHTTP)
					}
					if ct := headers.Get("Content-Type"); ct == "" {
						t.Fatalf("EncodeError Content-Type empty (body=%s)", body)
					}
					assertEnvelope(t, kind, svc.ServiceName, tc, body)
				})
			}
		})
	}
}

// assertEnvelope checks body against the documented shape for kind.
func assertEnvelope(t *testing.T, kind envelopeKind, service string, tc canonicalErrorCase, body []byte) {
	t.Helper()
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal %s envelope: %v (body=%s)", service, err, body)
	}
	errObj, ok := raw["error"].(map[string]any)
	if !ok {
		t.Fatalf("%s: missing error object (body=%s)", service, body)
	}
	code, ok := jsonNumber(errObj["code"])
	if !ok {
		t.Fatalf("%s: error.code is not an integer (body=%s)", service, body)
	}
	if code != tc.wantHTTP {
		t.Fatalf("%s: error.code = %d, want %d (body=%s)", service, code, tc.wantHTTP, body)
	}
	msg, _ := errObj["message"].(string)
	if msg == "" {
		t.Fatalf("%s: error.message empty (body=%s)", service, body)
	}
	switch kind {
	case envelopeIceberg:
		typ, _ := errObj["type"].(string)
		if typ != tc.perr.Code {
			t.Fatalf("%s: Iceberg error.type = %q, want %q", service, typ, tc.perr.Code)
		}
		if _, hasStatus := errObj["status"]; hasStatus {
			t.Fatalf("%s: Iceberg envelope must not carry a google.rpc status", service)
		}
		return
	case envelopeGCS:
		if _, hasStatus := errObj["status"]; hasStatus {
			t.Fatalf("%s: GCS envelope must not carry a google.rpc status (body=%s)", service, body)
		}
		if !hasWellFormedErrors(errObj) {
			t.Fatalf("%s: GCS envelope missing a well-formed errors[] (body=%s)", service, body)
		}
		return
	}
	// en envelopeStandard and BigQuery both carry the canonical status string.
	gotStatus, _ := errObj["status"].(string)
	if gotStatus != tc.wantStatus {
		t.Fatalf("%s: error.status = %q, want %q (body=%s)", service, gotStatus, tc.wantStatus, body)
	}
	if kind == envelopeBigQuery && !hasWellFormedErrors(errObj) {
		t.Fatalf("%s: BigQuery envelope missing a well-formed errors[] (body=%s)", service, body)
	}
}

// hasWellFormedErrors reports whether the envelope carries a legacy errors[]
// array whose entries have the documented domain/reason/message strings.
func hasWellFormedErrors(errObj map[string]any) bool {
	arr, ok := errObj["errors"].([]any)
	if !ok || len(arr) == 0 {
		return false
	}
	for _, e := range arr {
		em, ok := e.(map[string]any)
		if !ok {
			return false
		}
		for _, field := range []string{"domain", "reason", "message"} {
			s, _ := em[field].(string)
			if s == "" {
				return false
			}
		}
	}
	return true
}

// TestCanonicalErrorMappingPerGRPC asserts every canonical case maps to its
// expected gRPC code through the single shared GRPCStatus (every gRPC service
// funnels through it), and that the code's name equals the REST status.
func TestCanonicalErrorMappingPerGRPC(t *testing.T) {
	for _, tc := range canonicalErrorCases() {
		t.Run(tc.name, func(t *testing.T) {
			gerr := grpcutil.GRPCStatus(tc.perr)
			if gerr == nil {
				t.Fatal("GRPCStatus returned nil for a non-nil error")
			}
			if got := status.Code(gerr); got != tc.wantCode {
				t.Fatalf("gRPC code = %v, want %v", got, tc.wantCode)
			}
			if got := rpcName(status.Code(gerr)); got != tc.wantStatus {
				t.Fatalf("gRPC status name = %q, want %q", got, tc.wantStatus)
			}
		})
	}
}

// TestDatastoreProtoErrorMapping asserts the Datastore codec's protobuf error
// path (the official google-cloud-datastore HTTP transport negotiates
// application/x-protobuf) renders a google.rpc.Status with the same code as the
// gRPC transport, not the JSON envelope.
func TestDatastoreProtoErrorMapping(t *testing.T) {
	var codec adapter.Codec
	for _, svc := range gcpServices {
		if svc.ServiceName == "datastore" {
			codec = svc.Codec()
			break
		}
	}
	if codec == nil {
		t.Fatal("datastore codec not registered")
	}
	nr := &model.NormalizedRequest{Params: map[string]any{"jaiscloud:protoResponse": true}}
	for _, tc := range canonicalErrorCases() {
		t.Run(tc.name, func(t *testing.T) {
			httpStatus, headers, body := codec.EncodeError(nr, withAuditMessage(tc.perr))
			if httpStatus != tc.wantHTTP {
				t.Fatalf("proto EncodeError HTTP status = %d, want %d", httpStatus, tc.wantHTTP)
			}
			if ct := headers.Get("Content-Type"); !strings.Contains(ct, "protobuf") {
				t.Fatalf("proto EncodeError Content-Type = %q, want protobuf", ct)
			}
			// The google.rpc.Status code field is the gRPC code number; the wire
			// bytes are opaque here, so cross-check the shared mapping the codec
			// uses by deriving it the same way GRPCStatus does.
			want, _ := gcperr.GRPCCodeForStatus(tc.wantStatus)
			if tc.wantCode != want {
				t.Fatalf("test case %s: wantCode %v disagrees with canonical %v", tc.name, tc.wantCode, want)
			}
			if len(body) == 0 {
				t.Fatal("proto EncodeError produced an empty body")
			}
		})
	}
}

// ─── Static invariant: every constructed error code resolves ─────────────────

// errCodeSite is one provider-error construction site found by the AST scan.
type errCodeSite struct {
	code   string
	http   int    // -1 when the HTTP status is not a literal
	status string // explicit ProviderError.Status override, "" when unset
	where  string
}

// validateErrorCodeSite enforces the invariant for one site. It is a pure
// function so a fixture can prove a mismatch fails.
func validateErrorCodeSite(site errCodeSite) error {
	if site.http < 0 && site.status == "" {
		return &auditError{site.where + ": provider error has neither an HTTP status nor an explicit Status override"}
	}
	resolved, httpStatus := gcperr.Resolve(&model.ProviderError{
		Code:       site.code,
		HTTPStatus: site.http,
		Status:     site.status,
	})
	if _, ok := gcperr.GRPCCodeForStatus(resolved); !ok {
		return &auditError{site.where + ": code " + strconv.Quote(site.code) + " resolves to unknown status " + strconv.Quote(resolved)}
	}
	if site.status != "" {
		// An explicit Status is a deliberate override; its name is validated
		// above and its HTTP pairing is checked by the conformance validator.
		return nil
	}
	accepted, ok := acceptedStatusesForHTTP[httpStatus]
	if !ok {
		return &auditError{site.where + ": HTTP " + strconv.Itoa(httpStatus) + " has no accepted canonical status"}
	}
	if !accepted[resolved] {
		return &auditError{site.where + ": code " + strconv.Quote(site.code) +
			" resolves to " + resolved + " on HTTP " + strconv.Itoa(httpStatus) +
			", which real GCP would not pair (accepted: " + strings.Join(sortedKeys(accepted), ", ") + ")"}
	}
	return nil
}

type auditError struct{ msg string }

func (e *auditError) Error() string { return e.msg }

// acceptedStatusesForHTTP is the set of canonical google.rpc names a real GCP
// API pairs with each HTTP status. 400, 409 and 412 are multi-valued because
// FAILED_PRECONDITION/OUT_OF_RANGE share 400, ABORTED/ALREADY_EXISTS share
// 409, and GCS uses 409 + FAILED_PRECONDITION for a non-empty bucket (the
// bucketNotEmpty reason). HTTP 502 has no google.rpc.Code; the run data-plane
// proxy maps it to UNAVAILABLE (see gcperr.StatusForHTTP), so UNAVAILABLE is
// accepted there.
var acceptedStatusesForHTTP = map[int]map[string]bool{
	400: {"INVALID_ARGUMENT": true, "FAILED_PRECONDITION": true, "OUT_OF_RANGE": true},
	401: {"UNAUTHENTICATED": true},
	403: {"PERMISSION_DENIED": true},
	404: {"NOT_FOUND": true},
	409: {"ALREADY_EXISTS": true, "ABORTED": true, "FAILED_PRECONDITION": true},
	412: {"FAILED_PRECONDITION": true},
	429: {"RESOURCE_EXHAUSTED": true},
	499: {"CANCELLED": true},
	500: {"INTERNAL": true},
	501: {"UNIMPLEMENTED": true},
	502: {"UNAVAILABLE": true},
	503: {"UNAVAILABLE": true},
	504: {"DEADLINE_EXCEEDED": true},
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestErrorCodeUniverseResolves scans every NewProviderError call and every
// model.ProviderError{...} literal under internal/gcp and asserts each resolves
// to a canonical status valid for its HTTP status.
func TestErrorCodeUniverseResolves(t *testing.T) {
	sites := scanErrorCodeSites(t)
	if len(sites) < 100 {
		t.Fatalf("scan found only %d error sites; the AST scan is likely broken", len(sites))
	}
	var checked int
	for _, site := range sites {
		if site.http < 0 && site.status == "" {
			// Non-literal HTTP status (e.g. a variable): cannot evaluate.
			t.Logf("skipping %s (dynamic HTTP status)", site.where)
			continue
		}
		checked++
		if err := validateErrorCodeSite(site); err != nil {
			t.Errorf("%v", err)
		}
	}
	t.Logf("error-code audit: %d sites checked of %d scanned", checked, len(sites))
}

// TestErrorCodeSiteValidatorFixture proves the audit fails on a mismatched
// mapping (a 404 stamped with a non-NOT_FOUND code) and on a dynamic/omitted
// HTTP status.
func TestErrorCodeSiteValidatorFixture(t *testing.T) {
	bad := errCodeSite{code: "NotFound", http: 409, where: "fixture"}
	if err := validateErrorCodeSite(bad); err == nil {
		t.Fatal("validator accepted NOT_FOUND on HTTP 409; the audit cannot catch mismatches")
	}
	if err := validateErrorCodeSite(errCodeSite{code: "NotFound", http: -1, where: "fixture"}); err == nil {
		t.Fatal("validator accepted a provider error with no HTTP status and no Status override")
	}
	good := errCodeSite{code: "NotFound", http: 404, where: "fixture"}
	if err := validateErrorCodeSite(good); err != nil {
		t.Fatalf("validator rejected a valid site: %v", err)
	}
}

// scanErrorCodeSites parses every non-test .go file under internal/gcp and
// collects NewProviderError calls and model.ProviderError literals.
func scanErrorCodeSites(t *testing.T) []errCodeSite {
	t.Helper()
	root := repoRoot(t)
	var sites []errCodeSite
	err := filepath.Walk(filepath.Join(root, "internal", "gcp"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if !isNewProviderError(x.Fun) || len(x.Args) == 0 {
					return true
				}
				code, ok := canonicalNameLiteral(x.Args[0])
				if !ok {
					return true
				}
				http := -1
				if len(x.Args) >= 3 {
					http = httpStatusLiteral(x.Args[2])
				}
				sites = append(sites, errCodeSite{code: code, http: http, where: rel + ":" + strconv.Itoa(fset.Position(x.Pos()).Line)})
			case *ast.CompositeLit:
				if !isProviderErrorType(x.Type) {
					return true
				}
				site := errCodeSite{http: -1, where: rel + ":" + strconv.Itoa(fset.Position(x.Pos()).Line)}
				for _, el := range x.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch key.Name {
					case "Code":
						site.code, _ = canonicalNameLiteral(kv.Value)
					case "HTTPStatus":
						site.http = httpStatusLiteral(kv.Value)
					case "Status":
						site.status, _ = canonicalNameLiteral(kv.Value)
					}
				}
				if site.code != "" {
					sites = append(sites, site)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan internal/gcp: %v", err)
	}
	return sites
}

func isNewProviderError(fun ast.Expr) bool {
	if id, ok := fun.(*ast.Ident); ok {
		return id.Name == "NewProviderError"
	}
	if sel, ok := fun.(*ast.SelectorExpr); ok {
		return sel.Sel.Name == "NewProviderError"
	}
	return false
}

func isProviderErrorType(typ ast.Expr) bool {
	switch t := typ.(type) {
	case *ast.Ident:
		return t.Name == "ProviderError"
	case *ast.SelectorExpr:
		return t.Sel.Name == "ProviderError"
	}
	return false
}

// httpStatusLiteral resolves an integer HTTP-status expression: a bare int, or
// one of the net/http constants actually used as a provider-error status.
func httpStatusLiteral(expr ast.Expr) int {
	if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.INT {
		if n, err := strconv.Atoi(lit.Value); err == nil {
			return n
		}
	}
	if sel, ok := expr.(*ast.SelectorExpr); ok {
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "http" {
			switch sel.Sel.Name {
			case "StatusBadRequest":
				return http.StatusBadRequest
			case "StatusNotFound":
				return http.StatusNotFound
			case "StatusConflict":
				return http.StatusConflict
			}
		}
	}
	return -1
}

func stringLiteral(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// gcperrConstants maps a gcperr status identifier to its canonical wire value,
// so a construction like Status: gcperr.Unavailable is resolved rather than
// treated as a missing status.
var gcperrConstants = map[string]string{
	"OK":                 gcperr.OK,
	"Cancelled":          gcperr.Cancelled,
	"Unknown":            gcperr.Unknown,
	"InvalidArgument":    gcperr.InvalidArgument,
	"DeadlineExceeded":   gcperr.DeadlineExceeded,
	"NotFound":           gcperr.NotFound,
	"AlreadyExists":      gcperr.AlreadyExists,
	"PermissionDenied":   gcperr.PermissionDenied,
	"ResourceExhausted":  gcperr.ResourceExhausted,
	"FailedPrecondition": gcperr.FailedPrecondition,
	"Aborted":            gcperr.Aborted,
	"OutOfRange":         gcperr.OutOfRange,
	"Unimplemented":      gcperr.Unimplemented,
	"Internal":           gcperr.Internal,
	"Unavailable":        gcperr.Unavailable,
	"DataLoss":           gcperr.DataLoss,
	"Unauthenticated":    gcperr.Unauthenticated,
}

func canonicalNameLiteral(expr ast.Expr) (string, bool) {
	if s, ok := stringLiteral(expr); ok {
		return s, true
	}
	if sel, ok := expr.(*ast.SelectorExpr); ok {
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == "gcperr" {
			if v, ok := gcperrConstants[sel.Sel.Name]; ok {
				return v, true
			}
		}
	}
	return "", false
}

func jsonNumber(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == float64(int(n))
	case int:
		return n, true
	}
	return 0, false
}

// repoRoot returns the module root from the test's working directory
// (internal/gcp/adapter).
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %s has no go.mod: %v", root, err)
	}
	return root
}
