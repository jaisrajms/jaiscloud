package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// statusCode returns the gRPC status code for err (codes.OK when err is nil).
func statusCode(err error) codes.Code { return status.Code(err) }

// suffix uniquely names resources so repeated runs against the (stateful)
// emulator don't collide with leftovers from prior runs.
var suffix = fmt.Sprintf("%x", time.Now().UnixNano())

// envOr returns the named environment variable or a default.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// grpcEndpoint is the gRPC target for the emulator's HTTP/2 surface.
func grpcEndpoint() string { return envOr("GCP_GRPC_ENDPOINT", "localhost:8081") }

type Result struct {
	Service string
	Case    string
	Pass    bool
	Detail  string
}

var results []Result

func record(service, name string, err error) {
	r := Result{Service: service, Case: name, Pass: err == nil}
	if err != nil {
		r.Detail = err.Error()
	}
	results = append(results, r)
	status := "PASS"
	if !r.Pass {
		status = "FAIL"
	}
	fmt.Printf("  [%s] %-45s %s\n", status, name, r.Detail)
}

func recordNA(service, name, detail string) {
	results = append(results, Result{Service: service, Case: name, Pass: true, Detail: "N/A: " + detail})
	fmt.Printf("  [N/A] %-45s %s\n", name, detail)
}

func main() {
	fmt.Printf("=== GCS (REST, %s) ===\n", base)
	runGCS()
	fmt.Println()
	fmt.Printf("=== Firestore (gRPC, %s) ===\n", grpcEndpoint())
	runFirestore()
	fmt.Println()
	fmt.Printf("=== Pub/Sub (gRPC, %s) ===\n", grpcEndpoint())
	runPubSub()
	fmt.Println()
	fmt.Printf("=== Secret Manager (gRPC, %s) ===\n", grpcEndpoint())
	runSecretManager()
	fmt.Println()
	fmt.Printf("=== Cloud KMS (gRPC, %s) ===\n", grpcEndpoint())
	runKMS()
	fmt.Println()
	fmt.Printf("=== Cloud Logging (gRPC, %s) ===\n", grpcEndpoint())
	runLogging()

	// Summaries per service.
	fmt.Println()
	fmt.Println("=== SUMMARY ===")
	byService := map[string]struct{ pass, fail int }{}
	for _, r := range results {
		s := byService[r.Service]
		if r.Pass {
			s.pass++
		} else {
			s.fail++
		}
		byService[r.Service] = s
	}
	var svcs []string
	for s := range byService {
		svcs = append(svcs, s)
	}
	sort.Strings(svcs)
	for _, s := range svcs {
		c := byService[s]
		fmt.Printf("%s: %d pass, %d fail\n", s, c.pass, c.fail)
	}
	if err := writeReport(); err != nil {
		fmt.Fprintf(os.Stderr, "report write error: %v\n", err)
	}
	fmt.Printf("report written to %s\n", reportPath())
}
