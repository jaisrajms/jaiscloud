package operations

import (
	"context"
	"net"
	"testing"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// TestEndpointToken pins the authority → service-token parsing: the first DNS
// label, lowercased, with the port and IPv6 brackets stripped.
func TestEndpointToken(t *testing.T) {
	with := func(authority string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs(":authority", authority))
	}
	cases := map[string]string{
		"managedkafka.localhost:8081":     "managedkafka",
		"managedkafka.googleapis.com":     "managedkafka",
		"managedkafka":                    "managedkafka",
		"managedkafka.googleapis.com:443": "managedkafka",
		"managedkafka.googleapis.com.":    "managedkafka",
		"METASTORE.localhost":             "metastore",
		"localhost:8081":                  "localhost",
		"127.0.0.1:8081":                  "127",
		"[::1]:8081":                      "[::1]",
		"":                                "",
	}
	for host, want := range cases {
		if got := endpointToken(with(host)); got != want {
			t.Errorf("endpointToken(%q) = %q, want %q", host, got, want)
		}
	}
	if got := endpointToken(context.Background()); got != "" {
		t.Errorf("endpointToken(no metadata) = %q, want empty", got)
	}
}

// startOperationsServer serves svc on a loopback listener and returns its
// address.
func startOperationsServer(t *testing.T, svc *Service) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	longrunningpb.RegisterOperationsServer(srv, svc)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), srv.Stop
}

// dialOperations dials addr with an explicit :authority so the shared service's
// endpoint scoping can be exercised (grpc.WithAuthority overrides the dial
// target's host, like a client configured for the service's endpoint).
func dialOperations(t *testing.T, addr, authority string) (longrunningpb.OperationsClient, func()) {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if authority != "" {
		opts = append(opts, grpc.WithAuthority(authority))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return longrunningpb.NewOperationsClient(conn), func() { _ = conn.Close() }
}

// TestEndpointAuthorityMatrix is the gRPC analogue of the REST routing matrix:
// every service whose google.longrunning.Operations surface the emulator scopes
// by endpoint is registered under its canonical host token (mirroring
// cmd/jaiscloud-gcp/main.go), and each is isolated — a request addressed to one
// host lists only that service's operations. A request to an unregistered host
// token uses the untokened fallback chain and never sees an endpoint-only page.
func TestEndpointAuthorityMatrix(t *testing.T) {
	// The LRO-owning endpoint host tokens the emulator wires (the first DNS
	// label of each service's canonical host). Cloud Functions v1 and v2 share
	// cloudfunctions.googleapis.com; Service Usage and Resource Manager
	// register only in the opt-in async mode, but the scoping mechanism is the
	// same.
	tokens := []string{
		"managedkafka", "metastore", "dataproc", "cloudfunctions",
		"workflows", "run", "serviceusage", "cloudresourcemanager",
	}
	parent := "projects/p/locations/us"

	// A distinct registry per token, each claiming only its own operation. All
	// are also in the untokened chain; being endpoint-registered they must be
	// excluded from the untokened List fallback.
	var resolvers []Resolver
	res := map[string]*fakeRegistry{}
	for _, tok := range tokens {
		name := parent + "/operations/" + tok + "-op"
		reg := &fakeRegistry{
			ops:      map[string]*longrunningpb.Operation{name: {Name: name, Done: false}},
			listAck:  true,
			listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: name}}},
		}
		res[tok] = reg
		resolvers = append(resolvers, reg)
	}
	// An explicitly untokened registry sits in the fallback chain the
	// endpoint-only resolvers are excluded from.
	fallbackName := parent + "/operations/fallback-op"
	fallback := &fakeRegistry{
		ops:      map[string]*longrunningpb.Operation{fallbackName: {Name: fallbackName, Done: false}},
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: fallbackName}}},
	}
	svc := New(append([]Resolver{fallback}, resolvers...)...)
	for _, tok := range tokens {
		svc.SetEndpointResolvers(tok, res[tok])
	}

	addr, stop := startOperationsServer(t, svc)
	defer stop()
	ctx := context.Background()

	for _, tok := range tokens {
		client, closeClient := dialOperations(t, addr, tok+".googleapis.com:443")
		page, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: parent})
		if err != nil {
			t.Fatalf("%s ListOperations: %v", tok, err)
		}
		want := parent + "/operations/" + tok + "-op"
		if n := len(page.GetOperations()); n != 1 || page.GetOperations()[0].GetName() != want {
			t.Fatalf("%s ListOperations = %+v, want only %s", tok, page.GetOperations(), want)
		}
		closeClient()
	}

	// An unregistered host token is not endpoint-scoped: the untokened fallback
	// answers, and no endpoint-only registry leaks into its page.
	client, closeClient := dialOperations(t, addr, "redis.googleapis.com:443")
	defer closeClient()
	page, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: parent})
	if err != nil {
		t.Fatalf("unregistered-token ListOperations: %v", err)
	}
	if n := len(page.GetOperations()); n != 1 || page.GetOperations()[0].GetName() != fallbackName {
		t.Fatalf("unregistered-token ListOperations = %+v, want the fallback's page %s", page.GetOperations(), fallbackName)
	}
}

// TestEndpointScopedListIsolatesServices verifies that a request addressed to a
// service's endpoint is served only by that service — as real GCP serves
// Operations per endpoint — even when two services persist operations under the
// same location parent.
func TestEndpointScopedListIsolatesServices(t *testing.T) {
	parent := "projects/p/locations/us"
	kafkaOp := parent + "/operations/kafka-op"
	msOp := parent + "/operations/ms-op"
	kafka := &fakeRegistry{
		ops:      map[string]*longrunningpb.Operation{kafkaOp: {Name: kafkaOp, Done: false}},
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: kafkaOp}}},
	}
	ms := &fakeRegistry{
		ops:      map[string]*longrunningpb.Operation{msOp: {Name: msOp, Done: false}},
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: msOp}}},
	}
	svc := New(kafka, ms)
	svc.SetEndpointResolvers("managedkafka", kafka)
	svc.SetEndpointResolvers("metastore", ms)

	addr, stop := startOperationsServer(t, svc)
	defer stop()
	ctx := context.Background()

	for _, tc := range []struct{ authority, own, foreign string }{
		{"managedkafka.localhost:8081", kafkaOp, msOp},
		{"metastore.localhost:8081", msOp, kafkaOp},
	} {
		client, closeClient := dialOperations(t, addr, tc.authority)

		// List returns only the endpoint service's operations.
		page, err := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: parent})
		if err != nil {
			t.Fatalf("%s ListOperations: %v", tc.authority, err)
		}
		if n := len(page.GetOperations()); n != 1 || page.GetOperations()[0].GetName() != tc.own {
			t.Fatalf("%s ListOperations = %+v, want only %s", tc.authority, page.GetOperations(), tc.own)
		}

		// The endpoint's own operation resolves to the service's in-flight op.
		own, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: tc.own})
		if err != nil {
			t.Fatalf("%s GetOperation(own): %v", tc.authority, err)
		}
		if own.GetName() != tc.own || own.GetDone() {
			t.Fatalf("%s GetOperation(own) = %+v, want the service's in-flight op", tc.authority, own)
		}

		// The other service's operation is not visible on this endpoint: the
		// resolver declines and the lenient terminal stub answers.
		foreign, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: tc.foreign})
		if err != nil {
			t.Fatalf("%s GetOperation(foreign): %v", tc.authority, err)
		}
		if foreign.GetName() != tc.foreign || !foreign.GetDone() {
			t.Fatalf("%s GetOperation(foreign) = %+v, want the terminal stub", tc.authority, foreign)
		}
		closeClient()
	}
}

// TestEndpointScopedListEmptyDoesNotFallThrough verifies that when the endpoint's
// service holds no operations its empty page is authoritative: it does not fall
// through to another (untokened-chain) registry that claims the same parent.
func TestEndpointScopedListEmptyDoesNotFallThrough(t *testing.T) {
	parent := "projects/p/locations/us"
	declining := &fakeRegistry{listAck: false}
	claiming := &fakeRegistry{
		listAck:  true,
		listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: parent + "/operations/other"}}},
	}
	svc := New(declining, claiming)
	svc.SetEndpointResolvers("managedkafka", declining)

	addr, stop := startOperationsServer(t, svc)
	defer stop()
	client, closeClient := dialOperations(t, addr, "managedkafka.localhost:8081")
	defer closeClient()

	page, err := client.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{Name: parent})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if n := len(page.GetOperations()); n != 0 {
		t.Fatalf("endpoint-scoped ListOperations = %+v, want the endpoint's empty page", page.GetOperations())
	}
}

// TestUntokenedListDoesNotCrossContaminate verifies endpoint-registered services
// are excluded from the untokened List fallback: a request that did not address
// their endpoint gets an empty page, never another service's operations.
func TestUntokenedListDoesNotCrossContaminate(t *testing.T) {
	parent := "projects/p/locations/us"
	kafka := &fakeRegistry{listAck: true, listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: parent + "/operations/kafka-op"}}}}
	ms := &fakeRegistry{listAck: true, listPage: &longrunningpb.ListOperationsResponse{Operations: []*longrunningpb.Operation{{Name: parent + "/operations/ms-op"}}}}
	svc := New(kafka, ms)
	svc.SetEndpointResolvers("managedkafka", kafka)
	svc.SetEndpointResolvers("metastore", ms)

	addr, stop := startOperationsServer(t, svc)
	defer stop()
	client, closeClient := dialOperations(t, addr, "") // authority = the dial target host
	defer closeClient()

	page, err := client.ListOperations(context.Background(), &longrunningpb.ListOperationsRequest{Name: parent})
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if n := len(page.GetOperations()); n != 0 {
		t.Fatalf("untokened ListOperations = %+v, want empty", page.GetOperations())
	}
}

// TestEndpointTokenCarriesMultipleResolvers verifies a token can own more than
// one resolver (Cloud Functions v1 and v2 share cloudfunctions.googleapis.com):
// an endpoint request consults them in registration order, so a name owned by
// either resolves.
func TestEndpointTokenCarriesMultipleResolvers(t *testing.T) {
	v1Name := "operations/v1-op"
	v2Name := "projects/p/locations/us/operations/v2-op"
	v1 := fakeResolver{name: v1Name, op: &longrunningpb.Operation{Name: v1Name, Done: false}}
	v2 := fakeResolver{name: v2Name, op: &longrunningpb.Operation{Name: v2Name, Done: false}}
	svc := New(v1, v2)
	svc.SetEndpointResolvers("cloudfunctions", v1, v2)

	addr, stop := startOperationsServer(t, svc)
	defer stop()
	client, closeClient := dialOperations(t, addr, "cloudfunctions.localhost:8081")
	defer closeClient()
	ctx := context.Background()

	for _, name := range []string{v1Name, v2Name} {
		op, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
		if err != nil {
			t.Fatalf("GetOperation(%s): %v", name, err)
		}
		if op.GetName() != name || op.GetDone() {
			t.Fatalf("GetOperation(%s) = %+v, want the resolver's in-flight op", name, op)
		}
	}
}
