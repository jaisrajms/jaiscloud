package sdk_eventarc_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	eventarc "google.golang.org/api/eventarc/v1"
)

// TestSDKEventarcAdvancedListParams drives the documented filter/orderBy
// parameters of the advanced Eventarc list methods through the official REST
// client. Before TD17 the emulator read neither on messageBuses/enrollments/
// pipelines/googleApiSources/channels/providers, so a client that sorted or
// filtered silently got the unfiltered, name-ascending list.
func TestSDKEventarcAdvancedListParams(t *testing.T) {
	ctx := context.Background()
	svc := depthEventarc(t)
	parent := depthParent("us-central1")

	// A run-unique id/label keeps these records isolated from any other message
	// bus in the location.
	run := unique("lp")
	idA, idB, idC := run+"-a", run+"-b", run+"-c"
	for _, mb := range []struct{ id, env string }{{idA, "test"}, {idB, "prod"}, {idC, "test"}} {
		_, err := svc.Projects.Locations.MessageBuses.Create(parent, &eventarc.MessageBus{
			DisplayName: mb.id,
			Labels:      map[string]string{"run": run, "env": mb.env},
		}).MessageBusId(mb.id).Context(ctx).Do()
		require.NoError(t, err)
	}
	defer func() {
		for _, id := range []string{idA, idB, idC} {
			_, _ = svc.Projects.Locations.MessageBuses.Delete(parent + "/messageBuses/" + id).Context(ctx).Do()
		}
	}()

	// list pages through the run's message buses with the filter/order and
	// returns their ids in order.
	list := func(filter, orderBy string, pageSize int64) []string {
		t.Helper()
		var out []string
		token := ""
		prefix := parent + "/messageBuses/"
		for {
			call := svc.Projects.Locations.MessageBuses.List(parent).Filter(filter)
			if orderBy != "" {
				call = call.OrderBy(orderBy)
			}
			if pageSize > 0 {
				call = call.PageSize(pageSize)
			}
			if token != "" {
				call = call.PageToken(token)
			}
			resp, err := call.Do()
			require.NoError(t, err)
			for _, mb := range resp.MessageBuses {
				out = append(out, strings.TrimPrefix(mb.Name, prefix))
			}
			if resp.NextPageToken == "" {
				return out
			}
			token = resp.NextPageToken
		}
	}

	sel := "labels.run=" + run
	require.Equal(t, []string{idA, idB, idC}, list(sel, "", 0), "default order is ascending name")
	require.Equal(t, []string{idB}, list(sel+" AND labels.env=prod", "", 0), "AIP-160 filter restricts the list")
	require.Equal(t, []string{idA}, list(`displayName:"`+idA+`"`, "", 0), "displayName filter")

	// orderBy, composed with the filter and the page cursor.
	require.Equal(t, []string{idC, idB, idA}, list(sel, "name desc", 0))
	require.Equal(t, []string{idC, idB, idA}, list(sel, "messageBusId desc", 0))
	require.Equal(t, []string{idA, idB, idC}, list(sel, "createTime", 0))
	require.Equal(t, []string{idA, idB, idC}, list(sel, "create_time", 0), "AIP-160 snake_case orderBy field")
	// Paging with pageSize 1 walks the filtered, ordered set exactly once.
	require.Equal(t, []string{idC, idB, idA}, list(sel, "name desc", 1))

	// A partial filtered page stops early with a next-page token.
	first, err := svc.Projects.Locations.MessageBuses.List(parent).
		Filter(sel).OrderBy("name desc").PageSize(2).Do()
	require.NoError(t, err)
	require.Len(t, first.MessageBuses, 2)
	require.NotEmpty(t, first.NextPageToken, "a partial filtered page must report a next page")

	// An unsupported filter or orderBy fails loud (400), never silently
	// returning the unfiltered list.
	_, err = svc.Projects.Locations.MessageBuses.List(parent).Filter("bogus=1").Do()
	requireCode(t, err, http.StatusBadRequest, "unsupported filter")
	_, err = svc.Projects.Locations.MessageBuses.List(parent).OrderBy("bogus").Do()
	requireCode(t, err, http.StatusBadRequest, "unsupported orderBy")

	// channels.list documents orderBy (but no filter).
	chID1, chID2 := run+"-ch1", run+"-ch2"
	for _, id := range []string{chID1, chID2} {
		_, err := svc.Projects.Locations.Channels.Create(parent, &eventarc.Channel{}).ChannelId(id).Context(ctx).Do()
		require.NoError(t, err)
	}
	chResp, err := svc.Projects.Locations.Channels.List(parent).OrderBy("channel_id desc").Do()
	require.NoError(t, err)
	require.NotEmpty(t, chResp.Channels)
	// The list includes every channel in the location, so assert the order is
	// globally descending (the default is ascending) rather than pinning ids.
	for i := 1; i < len(chResp.Channels); i++ {
		require.LessOrEqual(t, chResp.Channels[i].Name, chResp.Channels[i-1].Name,
			"channels must be ordered by name descending")
	}
	_, err = svc.Projects.Locations.Channels.List(parent).OrderBy("bogus").Do()
	requireCode(t, err, http.StatusBadRequest, "unsupported channels orderBy")

	// providers.list documents filter and orderBy.
	prov, err := svc.Projects.Locations.Providers.List(parent).Filter(`displayName:"Storage"`).Do()
	require.NoError(t, err)
	require.Len(t, prov.Providers, 1)
	require.Contains(t, prov.Providers[0].Name, "storage.googleapis.com")

	prov, err = svc.Projects.Locations.Providers.List(parent).OrderBy("displayName desc").Do()
	require.NoError(t, err)
	require.NotEmpty(t, prov.Providers)
	require.Equal(t, "Cloud Storage", prov.Providers[0].DisplayName)

	// The Provider schema has no uid, so naming one is an unknown field.
	_, err = svc.Projects.Locations.Providers.List(parent).Filter("uid=x").Do()
	requireCode(t, err, http.StatusBadRequest, "unknown provider filter field")
}
