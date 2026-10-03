package kafka_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kafkaEndpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080"
}

func kafkaProject() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

func kafkaBase(project string) string {
	return fmt.Sprintf("%s/v1/projects/%s/locations/us-central1", kafkaEndpoint(), project)
}

func kafkaPost(t *testing.T, url string, body interface{}) map[string]interface{} {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

func kafkaGet(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

// kafkaGetStatus performs a GET and returns the raw HTTP status code (used by
// negative cases that expect a non-200).
func kafkaGetStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// kafkaPostStatus performs a POST and returns the raw HTTP status code (used by
// negative cases that expect a non-200).
func kafkaPostStatus(t *testing.T, url string, body interface{}) int {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// kafkaDeleteStatus performs a DELETE and returns the raw HTTP status code.
func kafkaDeleteStatus(t *testing.T, url string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, nil) //nolint:noctx
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func kafkaDelete(url string) {
	req, _ := http.NewRequest(http.MethodDelete, url, nil) //nolint:noctx
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func kafkaPatch(t *testing.T, url string, body interface{}) map[string]interface{} {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(data)) //nolint:noctx
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

func TestManagedKafka(t *testing.T) {
	project := kafkaProject()
	clusterID := uniqueName("go-cluster")
	topicID := uniqueName("go-topic")
	base := kafkaBase(project)

	t.Cleanup(func() {
		kafkaDelete(base + "/clusters/" + clusterID)
	})

	t.Run("CreateCluster", func(t *testing.T) {
		resp := kafkaPost(t, base+"/clusters?clusterId="+clusterID, map[string]interface{}{
			"capacityConfig": map[string]interface{}{"vcpuCount": 3, "memoryBytes": 3221225472},
			"gcpConfig":      map[string]interface{}{"accessConfig": map[string]interface{}{"networkConfigs": []interface{}{}}},
		})
		assert.Equal(t, true, resp["done"])
		response, _ := resp["response"].(map[string]interface{})
		name, _ := response["name"].(string)
		assert.Contains(t, name, clusterID)
		assert.Equal(t, "ACTIVE", response["state"])
	})

	t.Run("GetCluster", func(t *testing.T) {
		resp := kafkaGet(t, base+"/clusters/"+clusterID)
		name, _ := resp["name"].(string)
		assert.Contains(t, name, clusterID)
		assert.Equal(t, "ACTIVE", resp["state"])
	})

	t.Run("CreateDuplicateCluster", func(t *testing.T) {
		status := kafkaPostStatus(t, base+"/clusters?clusterId="+clusterID, map[string]interface{}{
			"capacityConfig": map[string]interface{}{"vcpuCount": 3, "memoryBytes": 3221225472},
		})
		assert.Equal(t, 409, status)
	})

	t.Run("DeleteMissingCluster", func(t *testing.T) {
		status := kafkaDeleteStatus(t, base+"/clusters/does-not-exist")
		assert.Equal(t, 404, status)
	})

	t.Run("ListClusters", func(t *testing.T) {
		resp := kafkaGet(t, base+"/clusters")
		clusters, _ := resp["clusters"].([]interface{})
		found := false
		for _, c := range clusters {
			if m, ok := c.(map[string]interface{}); ok {
				if name, _ := m["name"].(string); name != "" {
					if contains(name, clusterID) {
						found = true
						break
					}
				}
			}
		}
		assert.True(t, found, "created cluster should appear in list")
	})

	t.Run("UpdateCluster", func(t *testing.T) {
		resp := kafkaPatch(t, base+"/clusters/"+clusterID, map[string]interface{}{
			"capacityConfig": map[string]interface{}{"vcpuCount": 6, "memoryBytes": 6442450944},
		})
		assert.Equal(t, true, resp["done"])
		response, _ := resp["response"].(map[string]interface{})
		capacity, _ := response["capacityConfig"].(map[string]interface{})
		require.NotNil(t, capacity)
		assert.Equal(t, float64(6), capacity["vcpuCount"])
	})

	t.Run("CreateTopic", func(t *testing.T) {
		resp := kafkaPost(t, base+"/clusters/"+clusterID+"/topics?topicId="+topicID,
			map[string]interface{}{"partitionCount": 3, "replicationFactor": 1})
		name, _ := resp["name"].(string)
		assert.Contains(t, name, topicID)
		assert.Equal(t, float64(3), resp["partitionCount"])
	})

	t.Run("GetTopic", func(t *testing.T) {
		resp := kafkaGet(t, base+"/clusters/"+clusterID+"/topics/"+topicID)
		name, _ := resp["name"].(string)
		assert.Contains(t, name, topicID)
		assert.Equal(t, float64(3), resp["partitionCount"])
	})

	t.Run("ListTopics", func(t *testing.T) {
		resp := kafkaGet(t, base+"/clusters/"+clusterID+"/topics")
		topics, _ := resp["topics"].([]interface{})
		found := false
		for _, topic := range topics {
			if m, ok := topic.(map[string]interface{}); ok {
				if name, _ := m["name"].(string); contains(name, topicID) {
					found = true
					break
				}
			}
		}
		assert.True(t, found, "created topic should appear in list")
	})

	t.Run("UpdateTopic", func(t *testing.T) {
		resp := kafkaPatch(t, base+"/clusters/"+clusterID+"/topics/"+topicID,
			map[string]interface{}{"partitionCount": 6})
		assert.Equal(t, float64(6), resp["partitionCount"])
	})

	t.Run("ListConsumerGroups", func(t *testing.T) {
		resp := kafkaGet(t, base+"/clusters/"+clusterID+"/consumerGroups")
		_, ok := resp["consumerGroups"]
		assert.True(t, ok, "response should have consumerGroups key")
	})

	t.Run("DeleteTopic", func(t *testing.T) {
		kafkaDelete(base + "/clusters/" + clusterID + "/topics/" + topicID)

		resp := kafkaGet(t, base+"/clusters/"+clusterID+"/topics")
		topics, _ := resp["topics"].([]interface{})
		for _, topic := range topics {
			if m, ok := topic.(map[string]interface{}); ok {
				name, _ := m["name"].(string)
				assert.False(t, contains(name, topicID), "deleted topic should not appear in list")
			}
		}
	})

	t.Run("DeleteCluster", func(t *testing.T) {
		kafkaDelete(base + "/clusters/" + clusterID)

		resp := kafkaGet(t, base+"/clusters")
		clusters, _ := resp["clusters"].([]interface{})
		for _, c := range clusters {
			if m, ok := c.(map[string]interface{}); ok {
				name, _ := m["name"].(string)
				assert.False(t, contains(name, clusterID))
			}
		}
	})
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
