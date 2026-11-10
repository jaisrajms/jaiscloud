package bigquery

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

const testDatasetSelfLink = "https://bigquery.googleapis.com/bigquery/v2/projects/test-project/datasets/sales"

// TestDatasetSelfLink covers the dataset resource's selfLink as real BigQuery
// returns it (Discovery: Dataset.selfLink, readOnly, "A URL that can be used to
// access the resource again"; the committed real-GCP golden
// tests/gcpdifferential/testdata/golden/046-bigquery-dataset_get.json carries
// it on create and get). The value is server-owned: a caller-supplied selfLink
// never wins, and datasets.list (DatasetList.datasets) does not carry it.
func TestDatasetSelfLink(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())

	nrWith := func(params map[string]any) *model.NormalizedRequest {
		params["project"] = "test-project"
		return &model.NormalizedRequest{AccountID: "test-project", Params: params}
	}

	created, err := p.CreateDataset(ctx, nrWith(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "test-project", "datasetId": "sales"},
		// A caller-supplied selfLink is stored (the body round-trips) but
		// must never be rendered: Discovery marks the field readOnly.
		"selfLink": "https://caller.example/bigquery/v2/projects/test-project/datasets/sales",
	}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := created.Data["selfLink"]; got != testDatasetSelfLink {
		t.Fatalf("create selfLink = %v, want %s", got, testDatasetSelfLink)
	}

	got, err := p.GetDataset(ctx, nrWith(map[string]any{"datasetId": "sales"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["selfLink"] != testDatasetSelfLink {
		t.Fatalf("get selfLink = %v, want %s", got.Data["selfLink"], testDatasetSelfLink)
	}

	updated, err := p.UpdateDataset(ctx, nrWith(map[string]any{
		"datasetId": "sales",
		"body": map[string]any{
			"friendlyName": "Sales",
			// A PATCH that tries to set the field is ignored as well.
			"selfLink": "https://caller.example/other",
		},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Data["selfLink"] != testDatasetSelfLink {
		t.Fatalf("update selfLink = %v, want %s", updated.Data["selfLink"], testDatasetSelfLink)
	}

	list, err := p.ListDatasets(ctx, nrWith(map[string]any{}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["datasets"].([]any)
	if len(items) != 1 {
		t.Fatalf("list items = %d, want 1", len(items))
	}
	item, _ := items[0].(map[string]any)
	if selfLink, ok := item["selfLink"]; ok {
		t.Fatalf("datasets.list item carries selfLink %v; Discovery DatasetList.datasets has no such field", selfLink)
	}
}

// TestDatasetSelfLinkREST asserts the same field on the raw REST wire: decode
// the real create/get/list requests with the BigQuery codec, dispatch them
// through the provider the way the emulator does, and read the JSON body the
// codec returns to a client.
func TestDatasetSelfLinkREST(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	codec := &gcpadapter.BigQueryCodec{Service: "bigquery"}

	call := func(method, path, body string) map[string]any {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		nr, err := codec.Decode(req, []byte(body))
		if err != nil {
			t.Fatalf("%s %s decode: %v", method, path, err)
		}
		handler := p.Routes()["BigQuery."+nr.Action]
		if handler == nil {
			t.Fatalf("%s %s decoded to %q, which is not routed", method, path, nr.Action)
		}
		resp, err := handler(ctx, nr)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		status, _, raw := codec.Encode(nr, resp)
		if status != 200 {
			t.Fatalf("%s %s status = %d, want 200 (%s)", method, path, status, raw)
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s body is not JSON: %v (%s)", method, path, err, raw)
		}
		return out
	}

	created := call("POST", "/bigquery/v2/projects/test-project/datasets", `{
		"datasetReference": {"projectId": "test-project", "datasetId": "sales"},
		"location": "US"
	}`)
	if got := created["selfLink"]; got != testDatasetSelfLink {
		t.Fatalf("create selfLink = %v, want %s", got, testDatasetSelfLink)
	}

	got := call("GET", "/bigquery/v2/projects/test-project/datasets/sales", "")
	if got["selfLink"] != testDatasetSelfLink {
		t.Fatalf("get selfLink = %v, want %s", got["selfLink"], testDatasetSelfLink)
	}

	list := call("GET", "/bigquery/v2/projects/test-project/datasets", "")
	items, _ := list["datasets"].([]any)
	if len(items) != 1 {
		t.Fatalf("datasets.list items = %d, want 1", len(items))
	}
	item, _ := items[0].(map[string]any)
	if selfLink, ok := item["selfLink"]; ok {
		t.Fatalf("datasets.list item carries selfLink %v, want it omitted (Discovery DatasetList.datasets)", selfLink)
	}
}
