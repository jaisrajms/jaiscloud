// Package sdk_eventarc_test exercises the jaiscloud-gcp emulator's Eventarc
// control plane (eventarc.googleapis.com/v1) through the official Google REST
// apiary client. This validates wire-level parity with the real SDK: Trigger and
// Channel create/patch/delete return a done google.longrunning.Operation whose
// response is the resource, Get/List return the resource(s) directly, and
// Providers is read-only discovery.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_eventarc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	eventarc "google.golang.org/api/eventarc/v1"
	"google.golang.org/api/option"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestSDKEventarc(t *testing.T) {
	ctx := context.Background()
	svc, err := eventarc.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	const location = "us-central1"
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)

	// --- Providers (read-only discovery) ---
	providers, err := svc.Projects.Locations.Providers.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, providers.Providers)
	var foundPubsub bool
	for _, pr := range providers.Providers {
		if pr.DisplayName == "Cloud Pub/Sub" {
			foundPubsub = true
		}
	}
	require.True(t, foundPubsub, "expected the Cloud Pub/Sub provider in the catalogue")

	gotProvider, err := svc.Projects.Locations.Providers.Get(
		fmt.Sprintf("%s/providers/storage.googleapis.com", parent)).Do()
	require.NoError(t, err)
	require.Equal(t, "Cloud Storage", gotProvider.DisplayName)

	// --- Trigger CRUD ---
	triggerID := unique("tr")
	op, err := svc.Projects.Locations.Triggers.Create(parent, &eventarc.Trigger{
		Destination: &eventarc.Destination{
			CloudRun: &eventarc.CloudRun{Service: "svc", Region: location},
		},
		EventFilters: []*eventarc.EventFilter{
			{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"},
		},
		Labels: map[string]string{"env": "test"},
	}).TriggerId(triggerID).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "operation should be done")
	require.NotEmpty(t, op.Name)

	var created eventarc.Trigger
	require.NoError(t, json.Unmarshal(op.Response, &created))
	wantName := fmt.Sprintf("%s/triggers/%s", parent, triggerID)
	require.Equal(t, wantName, created.Name)
	require.NotEmpty(t, created.Uid)
	require.Equal(t, "test", created.Labels["env"])

	got, err := svc.Projects.Locations.Triggers.Get(wantName).Do()
	require.NoError(t, err)
	require.Equal(t, wantName, got.Name)
	require.Equal(t, created.Uid, got.Uid, "uid must be stable across reads")

	list, err := svc.Projects.Locations.Triggers.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Triggers)

	// Patch labels. The body also carries a conflicting destination, which the
	// "labels" update mask must leave untouched.
	patchOp, err := svc.Projects.Locations.Triggers.Patch(wantName, &eventarc.Trigger{
		Labels:      map[string]string{"env": "prod"},
		Destination: &eventarc.Destination{CloudRun: &eventarc.CloudRun{Service: "clobber", Region: location}},
	}).UpdateMask("labels").Do()
	require.NoError(t, err)
	require.True(t, patchOp.Done)
	var patched eventarc.Trigger
	require.NoError(t, json.Unmarshal(patchOp.Response, &patched))
	require.Equal(t, "prod", patched.Labels["env"])
	require.NotNil(t, patched.Destination)
	require.NotNil(t, patched.Destination.CloudRun)
	require.Equal(t, "svc", patched.Destination.CloudRun.Service, "unmasked destination must retain its stored value")
	require.NotEmpty(t, patched.Etag)

	// A stale etag on patch is rejected with 409 ABORTED.
	_, err = svc.Projects.Locations.Triggers.Patch(wantName, &eventarc.Trigger{
		Etag:   created.Etag,
		Labels: map[string]string{"env": "stale"},
	}).UpdateMask("labels").Do()
	require.Error(t, err, "a stale etag must be rejected")

	// A stale etag on delete is rejected; the fresh etag succeeds.
	_, err = svc.Projects.Locations.Triggers.Delete(wantName).Etag(created.Etag).Do()
	require.Error(t, err, "a stale delete etag must be rejected")
	del, err := svc.Projects.Locations.Triggers.Delete(wantName).Etag(patched.Etag).Do()
	require.NoError(t, err)
	require.True(t, del.Done)

	_, err = svc.Projects.Locations.Triggers.Get(wantName).Do()
	require.Error(t, err)

	// --- Channel CRUD ---
	channelID := unique("ch")
	channelOp, err := svc.Projects.Locations.Channels.Create(parent, &eventarc.Channel{
		Provider: fmt.Sprintf("%s/providers/pubsub.googleapis.com", parent),
	}).ChannelId(channelID).Do()
	require.NoError(t, err)
	require.True(t, channelOp.Done)

	var createdChannel eventarc.Channel
	require.NoError(t, json.Unmarshal(channelOp.Response, &createdChannel))
	wantChannelName := fmt.Sprintf("%s/channels/%s", parent, channelID)
	require.Equal(t, wantChannelName, createdChannel.Name)
	require.NotEmpty(t, createdChannel.ActivationToken)
	require.NotEmpty(t, createdChannel.PubsubTopic)
	// A newly created channel is PENDING until a provider Connection is made.
	require.Equal(t, "PENDING", createdChannel.State)

	gotChannel, err := svc.Projects.Locations.Channels.Get(wantChannelName).Do()
	require.NoError(t, err)
	require.Equal(t, wantChannelName, gotChannel.Name)

	channelDel, err := svc.Projects.Locations.Channels.Delete(wantChannelName).Do()
	require.NoError(t, err)
	require.True(t, channelDel.Done)

	// --- Source/destination reference validation ---
	// A destination.workflow referencing a non-existent Workflow fails loud.
	_, err = svc.Projects.Locations.Triggers.Create(parent, &eventarc.Trigger{
		Destination: &eventarc.Destination{
			Workflow: fmt.Sprintf("%s/workflows/%s", parent, unique("wf-missing")),
		},
		EventFilters: []*eventarc.EventFilter{
			{Attribute: "type", Value: "google.cloud.workflows.workflow.v1.executed"},
		},
	}).TriggerId(unique("tr")).Do()
	require.Error(t, err, "a trigger referencing a missing Workflow must be rejected")
}
