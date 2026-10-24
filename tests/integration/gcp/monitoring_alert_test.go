package gcp_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMonitoringAlertPolicyFiresIncident covers SPK5 end to end over the wire:
// a custom metric is written through the REST Monitoring surface, a
// notification channel of type `pubsub` and a `condition_threshold` alert policy
// are created, the metric crosses the threshold, and a deterministic evaluator
// tick (POST /_jaiscloud/monitoring-tick) fires an incident whose notification is
// delivered to the topic's pull subscription.
//
// Before SPK5 the alert-policy publisher wrote a topic-keyed message that no
// subscription could pull (it predated the Pub/Sub fan-out), so the incident had
// no observable effect; this asserts the modern fan-out path. The payload is the
// real Cloud Monitoring notification packet (schema version 1.2).
//
// The exactly-one-message assertion is robust to the always-on 30s evaluator
// ticker: the incident open path is guarded by FindOpenIncident/ErrIncidentExists,
// so a background tick that fires the same incident first does not produce a
// second notification.
func TestMonitoringAlertPolicyFiresIncident(t *testing.T) {
	resetState(t)
	const project = "proj"
	const topic = "demo-alerts"
	const sub = "demo-alerts-sub"
	jsonHdr := map[string]string{"Content-Type": "application/json"}
	metricType := "custom.googleapis.com/demo/trades_per_min"

	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return b
	}

	// The notification sink: a topic and one pull subscription, created before
	// anything publishes so fan-out reaches it.
	topicName := "projects/" + project + "/topics/" + topic
	resp, body := do(t, "PUT", "/v1/projects/"+project+"/topics/"+topic, []byte(`{}`), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create topic: %s", body)
	resp, body = do(t, "PUT", "/v1/projects/"+project+"/subscriptions/"+sub,
		mustJSON(map[string]any{"topic": topicName}), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create subscription: %s", body)

	// A pubsub notification channel on that topic.
	resp, body = do(t, "POST", "/v3/projects/"+project+"/notificationChannels",
		mustJSON(map[string]any{
			"type":        "pubsub",
			"displayName": "demo alerts",
			"enabled":     true,
			"labels":      map[string]string{"topic": topicName},
		}), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create notification channel: %s", body)
	channelName, _ := jsonMap(t, body)["name"].(string)
	require.NotEmpty(t, channelName)

	// A threshold policy on the custom metric, wired to the channel.
	filter := fmt.Sprintf(`metric.type = %q AND resource.type = "global"`, metricType)
	resp, body = do(t, "POST", "/v3/projects/"+project+"/alertPolicies",
		mustJSON(map[string]any{
			"displayName":          "demo trades alert",
			"combiner":             "OR",
			"enabled":              true,
			"notificationChannels": []string{channelName},
			"conditions": []any{map[string]any{
				"displayName": "trades high",
				"conditionThreshold": map[string]any{
					"filter":         filter,
					"comparison":     "COMPARISON_GT",
					"thresholdValue": 5,
				},
			}},
		}), jsonHdr)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create alert policy: %s", body)
	require.Contains(t, jsonMap(t, body)["name"], "/alertPolicies/")

	// Below the threshold: an evaluator tick must not fire.
	writeMetricPoint(t, project, metricType, 1)
	resp, _ = do(t, "POST", "/_jaiscloud/monitoring-tick", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Empty(t, pullAlertNotifications(t, project, sub), "incident fired below threshold")

	// Cross the threshold: the tick fires an incident and delivers a notification.
	writeMetricPoint(t, project, metricType, 10)
	resp, _ = do(t, "POST", "/_jaiscloud/monitoring-tick", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	msgs := pullAlertNotifications(t, project, sub)
	require.Len(t, msgs, 1, "expected exactly one incident notification")

	// Real Cloud Monitoring Pub/Sub notification packet, schema version 1.2:
	// {"version":"1.2","incident":{...}} with a lowercase incident state.
	var packet struct {
		Version  string `json:"version"`
		Incident struct {
			IncidentID    string `json:"incident_id"`
			State         string `json:"state"`
			PolicyName    string `json:"policy_name"`
			ConditionName string `json:"condition_name"`
		} `json:"incident"`
	}
	require.NoError(t, json.Unmarshal(msgs[0], &packet))
	require.Equal(t, "1.2", packet.Version)
	require.NotEmpty(t, packet.Incident.IncidentID)
	require.Equal(t, "open", packet.Incident.State)
	require.Equal(t, "demo trades alert", packet.Incident.PolicyName)
	require.Equal(t, "trades high", packet.Incident.ConditionName)
}

// writeMetricPoint writes one gauge point for metricType at the current time,
// mirroring what the demo app's CreateTimeSeries call sends.
func writeMetricPoint(t *testing.T, project, metricType string, value float64) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"timeSeries": []any{map[string]any{
			"metric":   map[string]any{"type": metricType},
			"resource": map[string]any{"type": "global", "labels": map[string]string{"project_id": project}},
			"points": []any{map[string]any{
				"interval": map[string]any{"endTime": time.Now().UTC().Format(time.RFC3339Nano)},
				"value":    map[string]any{"doubleValue": value},
			}},
		}},
	})
	require.NoError(t, err)
	resp, respBody := do(t, "POST", "/v3/projects/"+project+"/timeSeries", body,
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "create time series: %s", respBody)
}

// pullAlertNotifications pulls the given subscription and returns the decoded
// JSON payload of each received message (the alert notification body).
func pullAlertNotifications(t *testing.T, project, sub string) [][]byte {
	t.Helper()
	resp, body := do(t, "POST", "/v1/projects/"+project+"/subscriptions/"+sub+":pull",
		[]byte(`{"maxMessages":10,"returnImmediately":true}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "pull: %s", body)

	var out struct {
		ReceivedMessages []struct {
			Message struct {
				Data string `json:"data"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	payloads := make([][]byte, 0, len(out.ReceivedMessages))
	for _, rm := range out.ReceivedMessages {
		decoded, err := base64.StdEncoding.DecodeString(rm.Message.Data)
		require.NoError(t, err)
		payloads = append(payloads, decoded)
	}
	return payloads
}
