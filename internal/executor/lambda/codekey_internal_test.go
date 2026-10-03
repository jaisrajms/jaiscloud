package lambda

import "testing"

func TestCodeKeyFallsBackToFunctionName(t *testing.T) {
	if got := codeKey(InvokeRequest{FunctionName: "fn"}); got != "fn" {
		t.Fatalf("codeKey = %q, want fn", got)
	}
}

func TestCodeKeyUsesExplicitKey(t *testing.T) {
	if got := codeKey(InvokeRequest{FunctionName: "fn", CodeKey: "us-central1.fn"}); got != "us-central1.fn" {
		t.Fatalf("codeKey = %q, want us-central1.fn", got)
	}
}
