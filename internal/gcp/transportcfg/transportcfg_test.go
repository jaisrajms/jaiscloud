package transportcfg

import (
	"reflect"
	"testing"
)

var known = []string{"storage", "pubsub", "kms"}

func TestParseGlobalDefault(t *testing.T) {
	sel, err := Parse("", "", known)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !sel.REST() || !sel.GRPC() {
		t.Fatalf("empty default = rest=%v grpc=%v, want both", sel.REST(), sel.GRPC())
	}
}

func TestParseGlobalOnly(t *testing.T) {
	sel, err := Parse("grpc", "", known)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if sel.REST() || !sel.GRPC() {
		t.Fatalf("grpc-only = rest=%v grpc=%v, want grpc only", sel.REST(), sel.GRPC())
	}
	for _, svc := range known {
		if sel.RESTFor(svc) || !sel.GRPCFor(svc) {
			t.Fatalf("%s: rest=%v grpc=%v, want grpc only", svc, sel.RESTFor(svc), sel.GRPCFor(svc))
		}
	}
}

func TestParseOverrideWins(t *testing.T) {
	sel, err := Parse("grpc", "storage=rest,pubsub=none", known)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// storage REST override means the REST listener must come up.
	if !sel.REST() || !sel.GRPC() {
		t.Fatalf("mixed selection = rest=%v grpc=%v, want both listeners", sel.REST(), sel.GRPC())
	}
	if !sel.RESTFor("storage") || sel.GRPCFor("storage") {
		t.Fatalf("storage: rest=%v grpc=%v, want rest only", sel.RESTFor("storage"), sel.GRPCFor("storage"))
	}
	if sel.RESTFor("pubsub") || sel.GRPCFor("pubsub") {
		t.Fatalf("pubsub: rest=%v grpc=%v, want none", sel.RESTFor("pubsub"), sel.GRPCFor("pubsub"))
	}
	if sel.RESTFor("kms") || !sel.GRPCFor("kms") {
		t.Fatalf("kms: rest=%v grpc=%v, want grpc only", sel.RESTFor("kms"), sel.GRPCFor("kms"))
	}
}

func TestParseBothAndHTTPAlias(t *testing.T) {
	sel, err := Parse("http,grpc", "", known)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !sel.REST() || !sel.GRPC() {
		t.Fatalf("http,grpc = rest=%v grpc=%v, want both", sel.REST(), sel.GRPC())
	}
	sel, err = Parse("rest=none", "", known) // nonsense global token -> error
	if err == nil {
		t.Fatalf("expected error for %q, got %+v", "rest=none", sel)
	}
}

func TestParseErrors(t *testing.T) {
	for _, tc := range []struct{ global, overrides string }{
		{"carrier-pigeon", ""},
		{"rest", "nope=grpc"},
		{"rest", "storage"},
		{"rest", "storage="},
	} {
		if _, err := Parse(tc.global, tc.overrides, known); err == nil {
			t.Errorf("Parse(%q,%q): expected error", tc.global, tc.overrides)
		}
	}
}

func TestParseServices(t *testing.T) {
	sel, err := Parse("rest", "pubsub=grpc", known)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := sel.Services(), []string{"kms", "pubsub", "storage"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Services = %v, want %v", got, want)
	}
}
