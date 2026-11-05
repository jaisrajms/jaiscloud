// topic_update_fields_test.go drives PTF2 through the official Pub/Sub v2
// admin client: UpdateTopic must accept masks for the whole mutable topic
// surface (CMEK, storage policy, schema settings, message transforms), not just
// labels/retention, and GetTopic must serve them back.
package sdkpubsub_test

import (
	"context"
	"reflect"
	"testing"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestTopicUpdateFieldsSDK(t *testing.T) {
	c, ctx := newClient(t)
	name := topicName("update-fields")
	if _, err := c.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	t.Cleanup(func() {
		_ = c.TopicAdminClient.DeleteTopic(context.Background(), &pubsubpb.DeleteTopicRequest{Topic: name})
	})

	const key = "projects/test-project/locations/global/keyRings/sdk-ring/cryptoKeys/sdk-key"
	policy := &pubsubpb.MessageStoragePolicy{
		AllowedPersistenceRegions: []string{"us-central1", "europe-west1"},
		EnforceInTransit:          true,
	}
	schema := &pubsubpb.SchemaSettings{
		Schema:   "projects/test-project/schemas/orders",
		Encoding: pubsubpb.Encoding_JSON,
	}
	transforms := []*pubsubpb.MessageTransform{{
		Transform: &pubsubpb.MessageTransform_JavascriptUdf{
			JavascriptUdf: &pubsubpb.JavaScriptUDF{
				FunctionName: "redact",
				Code:         "function redact(m) { return m; }",
			},
		},
		Enabled: true,
	}}

	updated, err := c.TopicAdminClient.UpdateTopic(ctx, &pubsubpb.UpdateTopicRequest{
		Topic: &pubsubpb.Topic{
			Name:                 name,
			KmsKeyName:           key,
			MessageStoragePolicy: policy,
			SchemaSettings:       schema,
			MessageTransforms:    transforms,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			"kms_key_name",
			"message_storage_policy",
			"schema_settings",
			"message_transforms",
		}},
	})
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if updated.GetKmsKeyName() != key {
		t.Errorf("UpdateTopic kmsKeyName = %q, want %q", updated.GetKmsKeyName(), key)
	}
	if !reflect.DeepEqual(updated.GetMessageStoragePolicy().GetAllowedPersistenceRegions(),
		policy.GetAllowedPersistenceRegions()) {
		t.Errorf("UpdateTopic regions = %v, want %v",
			updated.GetMessageStoragePolicy().GetAllowedPersistenceRegions(), policy.GetAllowedPersistenceRegions())
	}
	if !updated.GetMessageStoragePolicy().GetEnforceInTransit() {
		t.Error("UpdateTopic enforceInTransit = false, want true")
	}
	if updated.GetSchemaSettings().GetSchema() != schema.GetSchema() ||
		updated.GetSchemaSettings().GetEncoding() != schema.GetEncoding() {
		t.Errorf("UpdateTopic schemaSettings = %v, want %v", updated.GetSchemaSettings(), schema)
	}
	if got := updated.GetMessageTransforms(); len(got) != 1 ||
		got[0].GetJavascriptUdf().GetFunctionName() != "redact" {
		t.Errorf("UpdateTopic messageTransforms = %v, want the redact transform", got)
	}

	got, err := c.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.GetKmsKeyName() != key {
		t.Errorf("GetTopic kmsKeyName = %q, want %q", got.GetKmsKeyName(), key)
	}
	if !reflect.DeepEqual(got.GetMessageStoragePolicy().GetAllowedPersistenceRegions(),
		policy.GetAllowedPersistenceRegions()) {
		t.Errorf("GetTopic regions = %v, want %v",
			got.GetMessageStoragePolicy().GetAllowedPersistenceRegions(), policy.GetAllowedPersistenceRegions())
	}
	if got.GetSchemaSettings().GetSchema() != schema.GetSchema() {
		t.Errorf("GetTopic schemaSettings.schema = %q, want %q", got.GetSchemaSettings().GetSchema(), schema.GetSchema())
	}
	if len(got.GetMessageTransforms()) != 1 {
		t.Errorf("GetTopic messageTransforms = %v, want one entry", got.GetMessageTransforms())
	}
}
