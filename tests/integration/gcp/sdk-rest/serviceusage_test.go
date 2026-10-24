package sdkrest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/api/googleapi"
	serviceusage "google.golang.org/api/serviceusage/v1"

	"github.com/stretchr/testify/require"
)

// TestSDKServiceUsageEnableDisableGet exercises the Service Usage v1
// enable/disable lifecycle through the official apiary client. Mutations return
// a google.longrunning.Operation the emulator completes synchronously, and the
// typed EnableServiceResponse/DisableServiceResponse payload carries the
// resulting service.
func TestSDKServiceUsageEnableDisableGet(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj"
	name := parent + "/services/" + serviceID("run")

	// A never-enabled service resolves to DISABLED (the emulator has no API
	// catalog, so an unknown id is simply not enabled rather than NotFound).
	before, err := svc.Services.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, "DISABLED", before.State)
	require.Equal(t, name, before.Name)
	require.Equal(t, parent, before.Parent)
	require.Equal(t, name[len(parent)+len("/services/"):], before.Config.Name)

	// Enable returns a done operation whose EnableServiceResponse carries the
	// ENABLED service.
	op, err := svc.Services.Enable(name, &serviceusage.EnableServiceRequest{}).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "enable must complete synchronously")
	require.NotEmpty(t, op.Name)
	var enabled serviceusage.EnableServiceResponse
	require.NoError(t, json.Unmarshal(op.Response, &enabled))
	require.NotNil(t, enabled.Service)
	require.Equal(t, "ENABLED", enabled.Service.State)

	got, err := svc.Services.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, "ENABLED", got.State)

	// Disable flips it back; the DisableServiceResponse carries the DISABLED
	// service.
	dop, err := svc.Services.Disable(name, &serviceusage.DisableServiceRequest{}).Do()
	require.NoError(t, err)
	require.True(t, dop.Done)
	var disabled serviceusage.DisableServiceResponse
	require.NoError(t, json.Unmarshal(dop.Response, &disabled))
	require.NotNil(t, disabled.Service)
	require.Equal(t, "DISABLED", disabled.Service.State)

	got, err = svc.Services.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, "DISABLED", got.State)

	// Disabling a service that is not enabled is a FailedPrecondition (HTTP 400,
	// status FAILED_PRECONDITION), matching real Service Usage.
	_, err = svc.Services.Disable(name, &serviceusage.DisableServiceRequest{}).Do()
	requireGCPStatus(t, err, 400, "FAILED_PRECONDITION")
}

// TestSDKServiceUsageFilterByState verifies the documented state:ENABLED /
// state:DISABLED list filters and the InvalidArgument for anything else.
func TestSDKServiceUsageFilterByState(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj"
	enabled := parent + "/services/" + serviceID("enabled")
	disabled := parent + "/services/" + serviceID("disabled")

	_, err = svc.Services.Enable(enabled, &serviceusage.EnableServiceRequest{}).Do()
	require.NoError(t, err)
	// Enable then disable `disabled` so it is tracked as DISABLED: the emulator
	// has no API catalog, so List only enumerates services that have been
	// enabled at least once (a never-touched id is absent, not DISABLED).
	_, err = svc.Services.Enable(disabled, &serviceusage.EnableServiceRequest{}).Do()
	require.NoError(t, err)
	_, err = svc.Services.Disable(disabled, &serviceusage.DisableServiceRequest{}).Do()
	require.NoError(t, err)

	epage, err := svc.Services.List(parent).Filter("state:ENABLED").PageSize(200).Do()
	require.NoError(t, err)
	require.True(t, serviceListHasState(epage.Services, enabled, "ENABLED"),
		"the enabled service must appear in filter=state:ENABLED")
	require.False(t, serviceListHasState(epage.Services, disabled, "ENABLED"),
		"a DISABLED service must not appear in filter=state:ENABLED")
	for _, s := range epage.Services {
		require.Equal(t, "ENABLED", s.State, "state:ENABLED must not leak a DISABLED service")
	}

	dpage, err := svc.Services.List(parent).Filter("state:DISABLED").PageSize(200).Do()
	require.NoError(t, err)
	require.True(t, serviceListHasState(dpage.Services, disabled, "DISABLED"),
		"the disabled service must appear in filter=state:DISABLED")
	require.False(t, serviceListHasState(dpage.Services, enabled, "DISABLED"),
		"the enabled service must not appear in filter=state:DISABLED")
	for _, s := range dpage.Services {
		require.Equal(t, "DISABLED", s.State, "state:DISABLED must not leak an ENABLED service")
	}

	// An unsupported filter is an InvalidArgument (HTTP 400, INVALID_ARGUMENT).
	_, err = svc.Services.List(parent).Filter("state:BOGUS").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}

// TestSDKServiceUsageBatchEnableAndGet exercises batchEnable and batchGet: the
// batch operation returns every enabled service, and batchGet preserves request
// order while reporting a never-enabled id as DISABLED.
func TestSDKServiceUsageBatchEnableAndGet(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj"
	idA := serviceID("batch-a")
	idB := serviceID("batch-b")

	op, err := svc.Services.BatchEnable(parent, &serviceusage.BatchEnableServicesRequest{
		ServiceIds: []string{idA, idB},
	}).Do()
	require.NoError(t, err)
	require.True(t, op.Done)
	var batch serviceusage.BatchEnableServicesResponse
	require.NoError(t, json.Unmarshal(op.Response, &batch))
	require.Len(t, batch.Services, 2)
	for _, s := range batch.Services {
		require.Equal(t, "ENABLED", s.State)
		require.Equal(t, parent, s.Parent)
	}

	// BatchGet echoes both enabled services plus a never-enabled id, preserving
	// the requested order exactly.
	never := parent + "/services/" + serviceID("batch-never")
	got, err := svc.Services.BatchGet(parent).Names(
		parent+"/services/"+idB,
		never,
		parent+"/services/"+idA,
	).Do()
	require.NoError(t, err)
	require.Len(t, got.Services, 3)
	require.Equal(t, parent+"/services/"+idB, got.Services[0].Name)
	require.Equal(t, "ENABLED", got.Services[0].State)
	require.Equal(t, never, got.Services[1].Name)
	require.Equal(t, "DISABLED", got.Services[1].State)
	require.Equal(t, parent+"/services/"+idA, got.Services[2].Name)
	require.Equal(t, "ENABLED", got.Services[2].State)
}

// TestSDKServiceUsageBatchValidation covers the Service Usage batch-size and
// name contracts: empty batchEnable, an empty batchGet, an oversized batchGet
// (>30), and a batchGet name belonging to another project are all
// InvalidArgument (HTTP 400).
func TestSDKServiceUsageBatchValidation(t *testing.T) {
	ctx := context.Background()
	svc, err := serviceusage.NewService(ctx, opts()...)
	require.NoError(t, err)

	const parent = "projects/proj"

	_, err = svc.Services.BatchEnable(parent, &serviceusage.BatchEnableServicesRequest{}).Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")

	_, err = svc.Services.BatchGet(parent).Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")

	// 31 names exceeds the documented BatchGetServices cap of 30.
	names := make([]string, 0, 31)
	for i := 0; i < 31; i++ {
		names = append(names, fmt.Sprintf("%s/services/svc-%02d.example.com", parent, i))
	}
	_, err = svc.Services.BatchGet(parent).Names(names...).Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")

	// A name under a different project than the parent is rejected.
	_, err = svc.Services.BatchGet(parent).Names("projects/other/services/run.googleapis.com").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}

// serviceID builds a unique, DNS-shaped service identifier (Service Usage ids
// are bare DNS names such as "run.googleapis.com", never resource paths).
func serviceID(prefix string) string { return unique(prefix) + ".example.com" }

func serviceListHasState(services []*serviceusage.GoogleApiServiceusageV1Service, name, state string) bool {
	for _, s := range services {
		if s.Name == name && s.State == state {
			return true
		}
	}
	return false
}

// requireGCPStatus asserts err is a googleapi.Error carrying the HTTP code and
// the GCP error `status` string (e.g. INVALID_ARGUMENT) from the response
// envelope {"error":{"code","message","status"}}. The apiary client does not
// surface the envelope's `status` as a typed field, so the raw Body is checked.
func requireGCPStatus(t *testing.T, err error, code int, status string) {
	t.Helper()
	require.Error(t, err)
	var ae *googleapi.Error
	require.True(t, errors.As(err, &ae), "expected *googleapi.Error, got %T (%v)", err, err)
	require.Equal(t, code, ae.Code, "unexpected HTTP code (%s)", ae.Message)
	if status != "" {
		require.Contains(t, ae.Body, `"status":"`+status+`"`, "unexpected GCP status (%s)", ae.Body)
	}
}
