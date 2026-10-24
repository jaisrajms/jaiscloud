package sdk_eventarc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	eventarc "google.golang.org/api/eventarc/v1"
)

// TestSDKEventarcAdvanced exercises the Eventarc advanced control plane over the
// official REST apiary client: message-bus CRUD, an enrollment attached to a
// bus (with the listEnrollments custom method), a channel connection, and the
// Google channel config singleton.
func TestSDKEventarcAdvanced(t *testing.T) {
	ctx := context.Background()
	svc, err := eventarc.NewService(ctx, opts()...)
	require.NoError(t, err)

	const location = "us-central1"
	parent := fmt.Sprintf("projects/%s/locations/%s", projectID(), location)

	// --- Message bus CRUD ---
	busID := unique("mb")
	busName := fmt.Sprintf("%s/messageBuses/%s", parent, busID)
	op, err := svc.Projects.Locations.MessageBuses.Create(parent, &eventarc.MessageBus{DisplayName: "sdk"}).
		MessageBusId(busID).Context(ctx).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create message bus operation not done")

	got, err := svc.Projects.Locations.MessageBuses.Get(busName).Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, busName, got.Name)
	require.NotEmpty(t, got.Uid)
	require.NotEmpty(t, got.Etag)

	list, err := svc.Projects.Locations.MessageBuses.List(parent).Context(ctx).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.MessageBuses)

	upd, err := svc.Projects.Locations.MessageBuses.Patch(busName, &eventarc.MessageBus{
		Labels: map[string]string{"updated": "yes"},
	}).UpdateMask("labels").Context(ctx).Do()
	require.NoError(t, err)
	require.True(t, upd.Done)

	// --- Enrollment attached to the bus + listEnrollments ---
	enrID := unique("enr")
	enrOp, err := svc.Projects.Locations.Enrollments.Create(parent, &eventarc.Enrollment{
		MessageBus: busName,
	}).EnrollmentId(enrID).Context(ctx).Do()
	require.NoError(t, err)
	require.True(t, enrOp.Done)

	enrList, err := svc.Projects.Locations.MessageBuses.ListEnrollments(busName).Context(ctx).Do()
	require.NoError(t, err)
	require.Contains(t, enrList.Enrollments, fmt.Sprintf("%s/enrollments/%s", parent, enrID))

	// --- Pipeline (pure metadata) ---
	pipeOp, err := svc.Projects.Locations.Pipelines.Create(parent, &eventarc.Pipeline{DisplayName: "sdk"}).
		PipelineId(unique("pl")).Context(ctx).Do()
	require.NoError(t, err)
	require.True(t, pipeOp.Done)

	// --- Channel connection (references an existing channel) ---
	channelID := unique("cc-ch")
	channelName := fmt.Sprintf("%s/channels/%s", parent, channelID)
	_, err = svc.Projects.Locations.Channels.Create(parent, &eventarc.Channel{}).
		ChannelId(channelID).Context(ctx).Do()
	require.NoError(t, err)
	ccID := unique("cc")
	ccOp, err := svc.Projects.Locations.ChannelConnections.Create(parent, &eventarc.ChannelConnection{
		Channel: channelName,
	}).ChannelConnectionId(ccID).Context(ctx).Do()
	require.NoError(t, err)
	require.True(t, ccOp.Done)
	cc, err := svc.Projects.Locations.ChannelConnections.Get(
		fmt.Sprintf("%s/channelConnections/%s", parent, ccID)).Context(ctx).Do()
	require.NoError(t, err)
	require.NotEmpty(t, cc.ActivationToken)

	// --- Google channel config singleton ---
	cfgName := parent + "/googleChannelConfig"
	_, err = svc.Projects.Locations.GetGoogleChannelConfig(cfgName).Context(ctx).Do()
	require.NoError(t, err)
	updated, err := svc.Projects.Locations.UpdateGoogleChannelConfig(cfgName, &eventarc.GoogleChannelConfig{
		CryptoKeyName: "projects/p/locations/us-central1/keyRings/r/cryptoKeys/k",
	}).UpdateMask("crypto_key_name").Context(ctx).Do()
	require.NoError(t, err)
	require.Equal(t, "projects/p/locations/us-central1/keyRings/r/cryptoKeys/k", updated.CryptoKeyName)

	// --- Delete the message bus ---
	_, err = svc.Projects.Locations.MessageBuses.Delete(busName).Context(ctx).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.MessageBuses.Get(busName).Context(ctx).Do()
	require.Error(t, err)
}
