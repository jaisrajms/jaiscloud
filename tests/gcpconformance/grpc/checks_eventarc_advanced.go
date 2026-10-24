package grpcconformance

// gRPC conformance probes for the Eventarc advanced control plane
// (google.cloud.eventarc.v1.Eventarc): message buses, enrollments, pipelines,
// Google API sources, channel connections, and the Google channel config
// singleton — the surface beyond triggers/channels/providers in
// checks_eventarc.go. Create/update/delete return done operations whose typed
// response is packed inline; GoogleChannelConfig.Get/Update return the config
// directly.

import (
	"context"
	"fmt"
	"strings"

	eventarc "cloud.google.com/go/eventarc/apiv1"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// eventarcAdvancedChecks returns the probes for the advanced-surface RPCs.
func eventarcAdvancedChecks() []Check {
	return []Check{
		// Message buses
		{Service: "eventarc", RPC: "CreateMessageBus", Method: "CreateMessageBus", KeyField: "LRO done + message bus echo", Run: checkEACreateMessageBus},
		{Service: "eventarc", RPC: "GetMessageBus", Method: "GetMessageBus", KeyField: "name round-trip", Run: checkEAGetMessageBus},
		{Service: "eventarc", RPC: "ListMessageBuses", Method: "ListMessageBuses", KeyField: "created bus present", Run: checkEAListMessageBuses},
		{Service: "eventarc", RPC: "UpdateMessageBus", Method: "UpdateMessageBus", KeyField: "labels updated via LRO", Run: checkEAUpdateMessageBus},
		{Service: "eventarc", RPC: "DeleteMessageBus", Method: "DeleteMessageBus", KeyField: "LRO done + NotFound after", Run: checkEADeleteMessageBus},
		{Service: "eventarc", RPC: "ListMessageBusEnrollments", Method: "ListMessageBusEnrollments", KeyField: "enrollment name listed", Run: checkEAListMessageBusEnrollments},
		// Enrollments
		{Service: "eventarc", RPC: "CreateEnrollment", Method: "CreateEnrollment", KeyField: "LRO done + enrollment echo", Run: checkEACreateEnrollment},
		{Service: "eventarc", RPC: "GetEnrollment", Method: "GetEnrollment", KeyField: "name round-trip", Run: checkEAGetEnrollment},
		{Service: "eventarc", RPC: "ListEnrollments", Method: "ListEnrollments", KeyField: "created enrollment present", Run: checkEAListEnrollments},
		{Service: "eventarc", RPC: "UpdateEnrollment", Method: "UpdateEnrollment", KeyField: "labels updated via LRO", Run: checkEAUpdateEnrollment},
		{Service: "eventarc", RPC: "DeleteEnrollment", Method: "DeleteEnrollment", KeyField: "LRO done + NotFound after", Run: checkEADeleteEnrollment},
		// Pipelines
		{Service: "eventarc", RPC: "CreatePipeline", Method: "CreatePipeline", KeyField: "LRO done + pipeline echo", Run: checkEACreatePipeline},
		{Service: "eventarc", RPC: "GetPipeline", Method: "GetPipeline", KeyField: "name round-trip", Run: checkEAGetPipeline},
		{Service: "eventarc", RPC: "ListPipelines", Method: "ListPipelines", KeyField: "created pipeline present", Run: checkEAListPipelines},
		{Service: "eventarc", RPC: "UpdatePipeline", Method: "UpdatePipeline", KeyField: "labels updated via LRO", Run: checkEAUpdatePipeline},
		{Service: "eventarc", RPC: "DeletePipeline", Method: "DeletePipeline", KeyField: "LRO done + NotFound after", Run: checkEADeletePipeline},
		// Google API sources
		{Service: "eventarc", RPC: "CreateGoogleApiSource", Method: "CreateGoogleApiSource", KeyField: "LRO done + source echo", Run: checkEACreateGoogleApiSource},
		{Service: "eventarc", RPC: "GetGoogleApiSource", Method: "GetGoogleApiSource", KeyField: "name round-trip", Run: checkEAGetGoogleApiSource},
		{Service: "eventarc", RPC: "ListGoogleApiSources", Method: "ListGoogleApiSources", KeyField: "created source present", Run: checkEAListGoogleApiSources},
		{Service: "eventarc", RPC: "UpdateGoogleApiSource", Method: "UpdateGoogleApiSource", KeyField: "labels updated via LRO", Run: checkEAUpdateGoogleApiSource},
		{Service: "eventarc", RPC: "DeleteGoogleApiSource", Method: "DeleteGoogleApiSource", KeyField: "LRO done + NotFound after", Run: checkEADeleteGoogleApiSource},
		// Channel connections
		{Service: "eventarc", RPC: "CreateChannelConnection", Method: "CreateChannelConnection", KeyField: "LRO done + connection echo", Run: checkEACreateChannelConnection},
		{Service: "eventarc", RPC: "GetChannelConnection", Method: "GetChannelConnection", KeyField: "name round-trip", Run: checkEAGetChannelConnection},
		{Service: "eventarc", RPC: "ListChannelConnections", Method: "ListChannelConnections", KeyField: "created connection present", Run: checkEAListChannelConnections},
		{Service: "eventarc", RPC: "DeleteChannelConnection", Method: "DeleteChannelConnection", KeyField: "LRO done + NotFound after", Run: checkEADeleteChannelConnection},
		// Google channel config (per-location singleton)
		{Service: "eventarc", RPC: "GetGoogleChannelConfig", Method: "GetGoogleChannelConfig", KeyField: "singleton name", Run: checkEAGetGoogleChannelConfig},
		{Service: "eventarc", RPC: "UpdateGoogleChannelConfig", Method: "UpdateGoogleChannelConfig", KeyField: "cryptoKeyName round-trip", Run: checkEAUpdateGoogleChannelConfig},
		// IAM on an advanced resource, served through the shared IAMPolicy
		// router (aggregates into the iam service's Get/Set/Test cells).
		{Service: "iam", RPC: "GetIamPolicy (Eventarc message bus)", Method: "GetIamPolicy", KeyField: "empty policy on a fresh bus", Run: checkEAMessageBusIAMGet},
		{Service: "iam", RPC: "SetIamPolicy (Eventarc message bus)", Method: "SetIamPolicy", KeyField: "binding round-trips through the router", Run: checkEAMessageBusIAMSet},
		{Service: "iam", RPC: "TestIamPermissions (Eventarc message bus)", Method: "TestIamPermissions", KeyField: "requested permissions echoed back", Run: checkEAMessageBusIAMTest},
	}
}

// ─── Message buses ────────────────────────────────────────────────────────────

func ensureEAMessageBus(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	name := eventarcParent(cfg) + "/messageBuses/" + id
	op, err := client.CreateMessageBus(ctx, &eventarcpb.CreateMessageBusRequest{
		Parent: eventarcParent(cfg), MessageBusId: id,
		MessageBus: &eventarcpb.MessageBus{DisplayName: "conformance"},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return name, nil
		}
		return "", fmt.Errorf("create message bus: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait message bus: %w", err)
	}
	return name, nil
}

func checkEACreateMessageBus(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-ea-mb-create")
	op, err := client.CreateMessageBus(ctx, &eventarcpb.CreateMessageBusRequest{
		Parent: eventarcParent(cfg), MessageBusId: id,
		MessageBus: &eventarcpb.MessageBus{DisplayName: "conformance"},
	})
	if err != nil {
		return fmt.Errorf("CreateMessageBus: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateMessageBus operation not done")
	}
	mb, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if mb.GetName() != eventarcParent(cfg)+"/messageBuses/"+id {
		return fmt.Errorf("message bus name = %q", mb.GetName())
	}
	if mb.GetUid() == "" || mb.GetEtag() == "" {
		return fmt.Errorf("message bus output-only fields missing: %+v", mb)
	}
	if mb.GetDisplayName() != "conformance" {
		return fmt.Errorf("displayName not echoed: %+v", mb)
	}
	return nil
}

func checkEAGetMessageBus(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAMessageBus(ctx, client, cfg, cfg.ResourceName("gcpc-ea-mb-get"))
	if err != nil {
		return err
	}
	got, err := client.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetMessageBus: %w", err)
	}
	if got.GetName() != name || got.GetUid() == "" {
		return fmt.Errorf("GetMessageBus = %+v", got)
	}
	return nil
}

func checkEAListMessageBuses(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-ea-mb-list")
	name, err := ensureEAMessageBus(ctx, client, cfg, id)
	if err != nil {
		return err
	}

	// The unfiltered list contains the created bus.
	found := false
	it := client.ListMessageBuses(ctx, &eventarcpb.ListMessageBusesRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListMessageBuses: %w", err)
		}
		if got.GetName() == name {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("ListMessageBuses did not include %q", name)
	}

	// The documented `filter` restricts the list and `orderBy` sorts it: a
	// client filtering on the created bus's name must see only matches, in
	// non-increasing name order. Before TD17 both were silently dropped.
	prev := ""
	count := 0
	ordered := client.ListMessageBuses(ctx, &eventarcpb.ListMessageBusesRequest{
		Parent: eventarcParent(cfg), Filter: fmt.Sprintf("name:%q", id), OrderBy: "name desc",
	})
	for {
		got, err := ordered.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("filtered ListMessageBuses: %w", err)
		}
		if !strings.Contains(got.GetName(), id) {
			return fmt.Errorf("filter name:%q returned %q", id, got.GetName())
		}
		if prev != "" && got.GetName() > prev {
			return fmt.Errorf("orderBy name desc returned %q before %q", prev, got.GetName())
		}
		prev = got.GetName()
		count++
	}
	if count == 0 {
		return fmt.Errorf("filtered ListMessageBuses returned nothing")
	}

	// An unsupported filter is InvalidArgument, never a silently unfiltered
	// list.
	if _, err := client.ListMessageBuses(ctx, &eventarcpb.ListMessageBusesRequest{
		Parent: eventarcParent(cfg), Filter: "bogus=1",
	}).Next(); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("bogus filter code = %v, want InvalidArgument", status.Code(err))
	}
	return nil
}

func checkEAUpdateMessageBus(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAMessageBus(ctx, client, cfg, cfg.ResourceName("gcpc-ea-mb-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateMessageBus(ctx, &eventarcpb.UpdateMessageBusRequest{
		MessageBus: &eventarcpb.MessageBus{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateMessageBus: %w", err)
	}
	mb, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if mb.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", mb.GetLabels())
	}
	return nil
}

func checkEADeleteMessageBus(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-ea-mb-del")
	name, err := ensureEAMessageBus(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	op, err := client.DeleteMessageBus(ctx, &eventarcpb.DeleteMessageBusRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteMessageBus: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetMessageBus(ctx, &eventarcpb.GetMessageBusRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetMessageBus after delete = %v, want NotFound", err)
	}
	return nil
}

func checkEAListMessageBusEnrollments(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	busName, err := ensureEAMessageBus(ctx, client, cfg, cfg.ResourceName("gcpc-ea-mb-enr"))
	if err != nil {
		return err
	}
	enrID := cfg.ResourceName("gcpc-ea-mb-enr-child")
	if op, err := client.CreateEnrollment(ctx, &eventarcpb.CreateEnrollmentRequest{
		Parent: eventarcParent(cfg), EnrollmentId: enrID,
		Enrollment: &eventarcpb.Enrollment{MessageBus: busName},
	}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("create enrollment: %w", err)
		}
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create enrollment wait: %w", err)
	}
	want := eventarcParent(cfg) + "/enrollments/" + enrID
	it := client.ListMessageBusEnrollments(ctx, &eventarcpb.ListMessageBusEnrollmentsRequest{Parent: busName})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListMessageBusEnrollments did not include %q", want)
		}
		if err != nil {
			return fmt.Errorf("ListMessageBusEnrollments: %w", err)
		}
		if got == want {
			return nil
		}
	}
}

// ─── Enrollments ──────────────────────────────────────────────────────────────

func ensureEAEnrollment(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	busName, err := ensureEAMessageBus(ctx, client, cfg, id+"-bus")
	if err != nil {
		return "", err
	}
	name := eventarcParent(cfg) + "/enrollments/" + id
	op, err := client.CreateEnrollment(ctx, &eventarcpb.CreateEnrollmentRequest{
		Parent: eventarcParent(cfg), EnrollmentId: id,
		Enrollment: &eventarcpb.Enrollment{MessageBus: busName},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return name, nil
		}
		return "", fmt.Errorf("create enrollment: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait enrollment: %w", err)
	}
	return name, nil
}

func checkEACreateEnrollment(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-ea-enr-create")
	name, err := ensureEAEnrollment(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	got, err := client.GetEnrollment(ctx, &eventarcpb.GetEnrollmentRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetEnrollment: %w", err)
	}
	if got.GetName() != name || got.GetUid() == "" {
		return fmt.Errorf("enrollment = %+v", got)
	}
	return nil
}

func checkEAGetEnrollment(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAEnrollment(ctx, client, cfg, cfg.ResourceName("gcpc-ea-enr-get"))
	if err != nil {
		return err
	}
	got, err := client.GetEnrollment(ctx, &eventarcpb.GetEnrollmentRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetEnrollment: %w", err)
	}
	if got.GetName() != name {
		return fmt.Errorf("GetEnrollment = %+v", got)
	}
	return nil
}

func checkEAListEnrollments(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAEnrollment(ctx, client, cfg, cfg.ResourceName("gcpc-ea-enr-list"))
	if err != nil {
		return err
	}
	it := client.ListEnrollments(ctx, &eventarcpb.ListEnrollmentsRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListEnrollments did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListEnrollments: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

func checkEAUpdateEnrollment(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAEnrollment(ctx, client, cfg, cfg.ResourceName("gcpc-ea-enr-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateEnrollment(ctx, &eventarcpb.UpdateEnrollmentRequest{
		Enrollment: &eventarcpb.Enrollment{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateEnrollment: %w", err)
	}
	got, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if got.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", got.GetLabels())
	}
	return nil
}

func checkEADeleteEnrollment(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAEnrollment(ctx, client, cfg, cfg.ResourceName("gcpc-ea-enr-del"))
	if err != nil {
		return err
	}
	op, err := client.DeleteEnrollment(ctx, &eventarcpb.DeleteEnrollmentRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteEnrollment: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetEnrollment(ctx, &eventarcpb.GetEnrollmentRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetEnrollment after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Pipelines ────────────────────────────────────────────────────────────────

func ensureEAPipeline(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	name := eventarcParent(cfg) + "/pipelines/" + id
	op, err := client.CreatePipeline(ctx, &eventarcpb.CreatePipelineRequest{
		Parent: eventarcParent(cfg), PipelineId: id,
		Pipeline: &eventarcpb.Pipeline{DisplayName: "conformance"},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return name, nil
		}
		return "", fmt.Errorf("create pipeline: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait pipeline: %w", err)
	}
	return name, nil
}

func checkEACreatePipeline(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAPipeline(ctx, client, cfg, cfg.ResourceName("gcpc-ea-pl-create"))
	if err != nil {
		return err
	}
	got, err := client.GetPipeline(ctx, &eventarcpb.GetPipelineRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetPipeline: %w", err)
	}
	if got.GetName() != name || got.GetUid() == "" || got.GetDisplayName() != "conformance" {
		return fmt.Errorf("pipeline = %+v", got)
	}
	return nil
}

func checkEAGetPipeline(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAPipeline(ctx, client, cfg, cfg.ResourceName("gcpc-ea-pl-get"))
	if err != nil {
		return err
	}
	got, err := client.GetPipeline(ctx, &eventarcpb.GetPipelineRequest{Name: name})
	if err != nil || got.GetName() != name {
		return fmt.Errorf("GetPipeline = %+v, %v", got, err)
	}
	return nil
}

func checkEAListPipelines(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAPipeline(ctx, client, cfg, cfg.ResourceName("gcpc-ea-pl-list"))
	if err != nil {
		return err
	}
	it := client.ListPipelines(ctx, &eventarcpb.ListPipelinesRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListPipelines did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListPipelines: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

func checkEAUpdatePipeline(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAPipeline(ctx, client, cfg, cfg.ResourceName("gcpc-ea-pl-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdatePipeline(ctx, &eventarcpb.UpdatePipelineRequest{
		Pipeline:   &eventarcpb.Pipeline{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdatePipeline: %w", err)
	}
	got, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if got.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", got.GetLabels())
	}
	return nil
}

func checkEADeletePipeline(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAPipeline(ctx, client, cfg, cfg.ResourceName("gcpc-ea-pl-del"))
	if err != nil {
		return err
	}
	op, err := client.DeletePipeline(ctx, &eventarcpb.DeletePipelineRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeletePipeline: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetPipeline(ctx, &eventarcpb.GetPipelineRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetPipeline after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Google API sources ───────────────────────────────────────────────────────

func ensureEAApiSource(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	name := eventarcParent(cfg) + "/googleApiSources/" + id
	op, err := client.CreateGoogleApiSource(ctx, &eventarcpb.CreateGoogleApiSourceRequest{
		Parent: eventarcParent(cfg), GoogleApiSourceId: id,
		GoogleApiSource: &eventarcpb.GoogleApiSource{DisplayName: "conformance"},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return name, nil
		}
		return "", fmt.Errorf("create google api source: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait google api source: %w", err)
	}
	return name, nil
}

func checkEACreateGoogleApiSource(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAApiSource(ctx, client, cfg, cfg.ResourceName("gcpc-ea-gas-create"))
	if err != nil {
		return err
	}
	got, err := client.GetGoogleApiSource(ctx, &eventarcpb.GetGoogleApiSourceRequest{Name: name})
	if err != nil || got.GetName() != name || got.GetUid() == "" {
		return fmt.Errorf("google api source = %+v, %v", got, err)
	}
	return nil
}

func checkEAGetGoogleApiSource(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAApiSource(ctx, client, cfg, cfg.ResourceName("gcpc-ea-gas-get"))
	if err != nil {
		return err
	}
	got, err := client.GetGoogleApiSource(ctx, &eventarcpb.GetGoogleApiSourceRequest{Name: name})
	if err != nil || got.GetName() != name {
		return fmt.Errorf("GetGoogleApiSource = %+v, %v", got, err)
	}
	return nil
}

func checkEAListGoogleApiSources(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAApiSource(ctx, client, cfg, cfg.ResourceName("gcpc-ea-gas-list"))
	if err != nil {
		return err
	}
	it := client.ListGoogleApiSources(ctx, &eventarcpb.ListGoogleApiSourcesRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListGoogleApiSources did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListGoogleApiSources: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

func checkEAUpdateGoogleApiSource(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAApiSource(ctx, client, cfg, cfg.ResourceName("gcpc-ea-gas-upd"))
	if err != nil {
		return err
	}
	op, err := client.UpdateGoogleApiSource(ctx, &eventarcpb.UpdateGoogleApiSourceRequest{
		GoogleApiSource: &eventarcpb.GoogleApiSource{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateGoogleApiSource: %w", err)
	}
	got, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if got.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", got.GetLabels())
	}
	return nil
}

func checkEADeleteGoogleApiSource(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAApiSource(ctx, client, cfg, cfg.ResourceName("gcpc-ea-gas-del"))
	if err != nil {
		return err
	}
	op, err := client.DeleteGoogleApiSource(ctx, &eventarcpb.DeleteGoogleApiSourceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteGoogleApiSource: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetGoogleApiSource(ctx, &eventarcpb.GetGoogleApiSourceRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetGoogleApiSource after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Channel connections ──────────────────────────────────────────────────────

func ensureEAChannelConnection(ctx context.Context, client *eventarc.Client, cfg Config, id string) (string, error) {
	channelName, err := ensureEventarcChannel(ctx, client, cfg, id+"-channel")
	if err != nil {
		return "", err
	}
	name := eventarcParent(cfg) + "/channelConnections/" + id
	op, err := client.CreateChannelConnection(ctx, &eventarcpb.CreateChannelConnectionRequest{
		Parent: eventarcParent(cfg), ChannelConnectionId: id,
		ChannelConnection: &eventarcpb.ChannelConnection{Channel: channelName},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return name, nil
		}
		return "", fmt.Errorf("create channel connection: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait channel connection: %w", err)
	}
	return name, nil
}

func checkEACreateChannelConnection(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAChannelConnection(ctx, client, cfg, cfg.ResourceName("gcpc-ea-cc-create"))
	if err != nil {
		return err
	}
	got, err := client.GetChannelConnection(ctx, &eventarcpb.GetChannelConnectionRequest{Name: name})
	if err != nil || got.GetName() != name || got.GetUid() == "" {
		return fmt.Errorf("channel connection = %+v, %v", got, err)
	}
	if got.GetActivationToken() == "" {
		return fmt.Errorf("channel connection activation token missing: %+v", got)
	}
	return nil
}

func checkEAGetChannelConnection(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAChannelConnection(ctx, client, cfg, cfg.ResourceName("gcpc-ea-cc-get"))
	if err != nil {
		return err
	}
	got, err := client.GetChannelConnection(ctx, &eventarcpb.GetChannelConnectionRequest{Name: name})
	if err != nil || got.GetName() != name {
		return fmt.Errorf("GetChannelConnection = %+v, %v", got, err)
	}
	return nil
}

func checkEAListChannelConnections(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAChannelConnection(ctx, client, cfg, cfg.ResourceName("gcpc-ea-cc-list"))
	if err != nil {
		return err
	}
	it := client.ListChannelConnections(ctx, &eventarcpb.ListChannelConnectionsRequest{Parent: eventarcParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListChannelConnections did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListChannelConnections: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

func checkEADeleteChannelConnection(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureEAChannelConnection(ctx, client, cfg, cfg.ResourceName("gcpc-ea-cc-del"))
	if err != nil {
		return err
	}
	op, err := client.DeleteChannelConnection(ctx, &eventarcpb.DeleteChannelConnectionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteChannelConnection: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetChannelConnection(ctx, &eventarcpb.GetChannelConnectionRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetChannelConnection after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Google channel config ────────────────────────────────────────────────────

func checkEAGetGoogleChannelConfig(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name := eventarcParent(cfg) + "/googleChannelConfig"
	got, err := client.GetGoogleChannelConfig(ctx, &eventarcpb.GetGoogleChannelConfigRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetGoogleChannelConfig: %w", err)
	}
	if got.GetName() != name {
		return fmt.Errorf("google channel config name = %q, want %q", got.GetName(), name)
	}
	return nil
}

func checkEAUpdateGoogleChannelConfig(ctx context.Context, cfg Config) error {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name := eventarcParent(cfg) + "/googleChannelConfig"
	const key = "projects/conformance/locations/us-central1/keyRings/r/cryptoKeys/k"
	got, err := client.UpdateGoogleChannelConfig(ctx, &eventarcpb.UpdateGoogleChannelConfigRequest{
		GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{Name: name, CryptoKeyName: key},
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"crypto_key_name"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateGoogleChannelConfig: %w", err)
	}
	if got.GetCryptoKeyName() != key {
		return fmt.Errorf("cryptoKeyName = %q, want %q", got.GetCryptoKeyName(), key)
	}
	return nil
}

// ─── IAM on an advanced resource (shared IAMPolicy router) ────────────────────

// eventarcMessageBusIAMFixture creates the run-unique message bus an IAM probe
// targets, and returns its name plus a cleanup func.
func eventarcMessageBusIAMFixture(ctx context.Context, cfg Config, prefix string) (string, func(), error) {
	client, err := newEventarcClient(ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	name, err := ensureEAMessageBus(ctx, client, cfg, cfg.ResourceName(prefix))
	if err != nil {
		client.Close()
		return "", nil, err
	}
	cleanup := func() {
		delOp, derr := client.DeleteMessageBus(ctx, &eventarcpb.DeleteMessageBusRequest{Name: name})
		if derr == nil {
			_, _ = delOp.Wait(ctx)
		}
		client.Close()
	}
	return name, cleanup, nil
}

func checkEAMessageBusIAMGet(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcMessageBusIAMFixture(ctx, cfg, "gcpc-ea-mb-iam-get")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", name, err)
	}
	if len(pol.GetBindings()) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) on a fresh bus returned %d bindings", name, len(pol.GetBindings()))
	}
	return nil
}

func checkEAMessageBusIAMSet(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcMessageBusIAMFixture(ctx, cfg, "gcpc-ea-mb-iam-set")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	const role = "roles/eventarc.viewer"
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: name,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: role, Members: []string{"allUsers"}}}},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", name, err)
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: name})
	if err != nil || len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("policy = %+v, %v", pol.GetBindings(), err)
	}
	return nil
}

func checkEAMessageBusIAMTest(ctx context.Context, cfg Config) error {
	name, cleanup, err := eventarcMessageBusIAMFixture(ctx, cfg, "gcpc-ea-mb-iam-test")
	if err != nil {
		return err
	}
	defer cleanup()
	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()
	want := []string{"eventarc.messageBuses.get", "eventarc.messageBuses.update"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: name, Permissions: want})
	if err != nil || len(resp.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions(%s) = %v, %v", name, resp.GetPermissions(), err)
	}
	return nil
}
