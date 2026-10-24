package grpcconformance

import (
	"context"
	"fmt"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// loggingAdminChecks covers the Cloud Logging Admin v2 surface on
// ConfigServiceV2: log buckets (sync + async), views, BigQuery links, and the
// per-scope Settings/CMEK records, all through the official generated
// ConfigClient. The checks run in order and share two run-unique buckets: one
// holds the views and links, and one exercises the async create/update and the
// delete/undelete lifecycle.
func loggingAdminChecks() []Check {
	return []Check{
		{Service: "logging", RPC: "CreateBucket", KeyField: "bucket name/retentionDays/lifecycleState", Run: checkLoggingCreateBucket},
		{Service: "logging", RPC: "GetBucket", KeyField: "bucket by full resource name", Run: checkLoggingGetBucket},
		{Service: "logging", RPC: "ListBuckets", KeyField: "buckets[] contains the created bucket", Run: checkLoggingListBuckets},
		{Service: "logging", RPC: "UpdateBucket", KeyField: "masked description update preserves retention", Run: checkLoggingUpdateBucket},
		{Service: "logging", RPC: "CreateBucketAsync", KeyField: "operation completes with the created bucket", Run: checkLoggingCreateBucketAsync},
		{Service: "logging", RPC: "UpdateBucketAsync", KeyField: "operation completes with the updated bucket", Run: checkLoggingUpdateBucketAsync},
		{Service: "logging", RPC: "DeleteBucket", KeyField: "lifecycleState becomes DELETE_REQUESTED", Run: checkLoggingDeleteBucket},
		{Service: "logging", RPC: "UndeleteBucket", KeyField: "lifecycleState returns to ACTIVE", Run: checkLoggingUndeleteBucket},
		{Service: "logging", RPC: "CreateView", KeyField: "view name/filter round-trip", Run: checkLoggingCreateView},
		{Service: "logging", RPC: "GetView", KeyField: "view by full resource name", Run: checkLoggingGetView},
		{Service: "logging", RPC: "ListViews", KeyField: "views[] contains the created view", Run: checkLoggingListViews},
		{Service: "logging", RPC: "UpdateView", KeyField: "masked description update", Run: checkLoggingUpdateView},
		{Service: "logging", RPC: "DeleteView", KeyField: "view absent after delete", Run: checkLoggingDeleteView},
		{Service: "logging", RPC: "CreateLink", KeyField: "operation completes with the created link", Run: checkLoggingCreateLink},
		{Service: "logging", RPC: "GetLink", KeyField: "link by full resource name", Run: checkLoggingGetLink},
		{Service: "logging", RPC: "ListLinks", KeyField: "links[] contains the created link", Run: checkLoggingListLinks},
		{Service: "logging", RPC: "DeleteLink", KeyField: "operation completes and the link is gone", Run: checkLoggingDeleteLink},
		{Service: "logging", RPC: "GetSettings", KeyField: "settings with a service account id", Run: checkLoggingGetSettings},
		{Service: "logging", RPC: "UpdateSettings", KeyField: "masked storageLocation update", Run: checkLoggingUpdateSettings},
		{Service: "logging", RPC: "GetCmekSettings", KeyField: "cmek settings with a service account id", Run: checkLoggingGetCmekSettings},
		{Service: "logging", RPC: "UpdateCmekSettings", KeyField: "masked kmsKeyName update", Run: checkLoggingUpdateCmekSettings},
	}
}

func loggingLocationParent(cfg Config) string { return loggingParent(cfg) + "/locations/global" }

func loggingBucketID(cfg Config) string  { return cfg.ResourceName("gcpc-grpc-bucket") }
func loggingBucketID2(cfg Config) string { return cfg.ResourceName("gcpc-grpc-bucket2") }

func loggingBucketName(cfg Config) string {
	return fmt.Sprintf("%s/buckets/%s", loggingLocationParent(cfg), loggingBucketID(cfg))
}
func loggingBucketName2(cfg Config) string {
	return fmt.Sprintf("%s/buckets/%s", loggingLocationParent(cfg), loggingBucketID2(cfg))
}
func loggingViewID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-view") }
func loggingViewName(cfg Config) string {
	return fmt.Sprintf("%s/views/%s", loggingBucketName(cfg), loggingViewID(cfg))
}
func loggingLinkID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-link") }
func loggingLinkName(cfg Config) string {
	return fmt.Sprintf("%s/links/%s", loggingBucketName(cfg), loggingLinkID(cfg))
}
func loggingLinkDatasetID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-dataset") }

func checkLoggingCreateBucket(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	b, err := client.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
		Parent:   loggingLocationParent(cfg),
		BucketId: loggingBucketID(cfg),
		Bucket:   &loggingpb.LogBucket{Description: "conformance", RetentionDays: 30},
	})
	if err != nil {
		return fmt.Errorf("CreateBucket: %w", err)
	}
	if b.GetName() != loggingBucketName(cfg) {
		return fmt.Errorf("bucket name = %q, want %q", b.GetName(), loggingBucketName(cfg))
	}
	if b.GetRetentionDays() != 30 {
		return fmt.Errorf("bucket retentionDays = %d, want 30", b.GetRetentionDays())
	}
	if b.GetLifecycleState() != loggingpb.LifecycleState_ACTIVE {
		return fmt.Errorf("bucket lifecycleState = %v, want ACTIVE", b.GetLifecycleState())
	}
	if b.GetCreateTime() == nil {
		return fmt.Errorf("bucket createTime is unset")
	}
	return nil
}

func checkLoggingGetBucket(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	b, err := client.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: loggingBucketName(cfg)})
	if err != nil {
		return fmt.Errorf("GetBucket: %w", err)
	}
	if b.GetDescription() != "conformance" {
		return fmt.Errorf("bucket description = %q, want %q", b.GetDescription(), "conformance")
	}
	return nil
}

func checkLoggingListBuckets(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	it := client.ListBuckets(ctx, &loggingpb.ListBucketsRequest{Parent: loggingLocationParent(cfg)})
	for {
		b, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created bucket %q not listed", loggingBucketID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListBuckets: %w", err)
		}
		if b.GetName() == loggingBucketName(cfg) {
			return nil
		}
	}
}

func checkLoggingUpdateBucket(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	b, err := client.UpdateBucket(ctx, &loggingpb.UpdateBucketRequest{
		Name:       loggingBucketName(cfg),
		Bucket:     &loggingpb.LogBucket{Description: "updated"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateBucket: %w", err)
	}
	if b.GetDescription() != "updated" {
		return fmt.Errorf("bucket description = %q, want %q", b.GetDescription(), "updated")
	}
	if b.GetRetentionDays() != 30 {
		return fmt.Errorf("masked update changed retentionDays: %d", b.GetRetentionDays())
	}
	return nil
}

func checkLoggingCreateBucketAsync(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateBucketAsync(ctx, &loggingpb.CreateBucketRequest{
		Parent:   loggingLocationParent(cfg),
		BucketId: loggingBucketID2(cfg),
		Bucket:   &loggingpb.LogBucket{Description: "async"},
	})
	if err != nil {
		return fmt.Errorf("CreateBucketAsync: %w", err)
	}
	b, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("CreateBucketAsync wait: %w", err)
	}
	if b.GetName() != loggingBucketName2(cfg) {
		return fmt.Errorf("async bucket name = %q, want %q", b.GetName(), loggingBucketName2(cfg))
	}
	if b.GetLifecycleState() != loggingpb.LifecycleState_ACTIVE {
		return fmt.Errorf("async bucket lifecycleState = %v, want ACTIVE", b.GetLifecycleState())
	}
	return nil
}

func checkLoggingUpdateBucketAsync(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	op, err := client.UpdateBucketAsync(ctx, &loggingpb.UpdateBucketRequest{
		Name:       loggingBucketName2(cfg),
		Bucket:     &loggingpb.LogBucket{Description: "async-updated"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateBucketAsync: %w", err)
	}
	b, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("UpdateBucketAsync wait: %w", err)
	}
	if b.GetDescription() != "async-updated" {
		return fmt.Errorf("async bucket description = %q, want %q", b.GetDescription(), "async-updated")
	}
	return nil
}

func checkLoggingDeleteBucket(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteBucket(ctx, &loggingpb.DeleteBucketRequest{Name: loggingBucketName2(cfg)}); err != nil {
		return fmt.Errorf("DeleteBucket: %w", err)
	}
	b, err := client.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: loggingBucketName2(cfg)})
	if err != nil {
		return fmt.Errorf("GetBucket after DeleteBucket: %w", err)
	}
	if b.GetLifecycleState() != loggingpb.LifecycleState_DELETE_REQUESTED {
		return fmt.Errorf("lifecycleState = %v, want DELETE_REQUESTED", b.GetLifecycleState())
	}
	return nil
}

func checkLoggingUndeleteBucket(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	if err := client.UndeleteBucket(ctx, &loggingpb.UndeleteBucketRequest{Name: loggingBucketName2(cfg)}); err != nil {
		return fmt.Errorf("UndeleteBucket: %w", err)
	}
	b, err := client.GetBucket(ctx, &loggingpb.GetBucketRequest{Name: loggingBucketName2(cfg)})
	if err != nil {
		return fmt.Errorf("GetBucket after UndeleteBucket: %w", err)
	}
	if b.GetLifecycleState() != loggingpb.LifecycleState_ACTIVE {
		return fmt.Errorf("lifecycleState = %v, want ACTIVE", b.GetLifecycleState())
	}
	return nil
}

func checkLoggingCreateView(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	v, err := client.CreateView(ctx, &loggingpb.CreateViewRequest{
		Parent: loggingBucketName(cfg),
		ViewId: loggingViewID(cfg),
		View:   &loggingpb.LogView{Description: "conformance", Filter: "severity>=ERROR"},
	})
	if err != nil {
		return fmt.Errorf("CreateView: %w", err)
	}
	if v.GetName() != loggingViewName(cfg) {
		return fmt.Errorf("view name = %q, want %q", v.GetName(), loggingViewName(cfg))
	}
	if v.GetFilter() != "severity>=ERROR" {
		return fmt.Errorf("view filter = %q, want %q", v.GetFilter(), "severity>=ERROR")
	}
	return nil
}

func checkLoggingGetView(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	v, err := client.GetView(ctx, &loggingpb.GetViewRequest{Name: loggingViewName(cfg)})
	if err != nil {
		return fmt.Errorf("GetView: %w", err)
	}
	if v.GetDescription() != "conformance" {
		return fmt.Errorf("view description = %q, want %q", v.GetDescription(), "conformance")
	}
	return nil
}

func checkLoggingListViews(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	it := client.ListViews(ctx, &loggingpb.ListViewsRequest{Parent: loggingBucketName(cfg)})
	for {
		v, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created view %q not listed", loggingViewID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListViews: %w", err)
		}
		if v.GetName() == loggingViewName(cfg) {
			return nil
		}
	}
}

func checkLoggingUpdateView(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	v, err := client.UpdateView(ctx, &loggingpb.UpdateViewRequest{
		Name:       loggingViewName(cfg),
		View:       &loggingpb.LogView{Description: "updated"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateView: %w", err)
	}
	if v.GetDescription() != "updated" {
		return fmt.Errorf("view description = %q, want %q", v.GetDescription(), "updated")
	}
	if v.GetFilter() != "severity>=ERROR" {
		return fmt.Errorf("masked update changed filter: %q", v.GetFilter())
	}
	return nil
}

func checkLoggingDeleteView(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteView(ctx, &loggingpb.DeleteViewRequest{Name: loggingViewName(cfg)}); err != nil {
		return fmt.Errorf("DeleteView: %w", err)
	}
	if _, err := client.GetView(ctx, &loggingpb.GetViewRequest{Name: loggingViewName(cfg)}); err == nil {
		return fmt.Errorf("GetView after DeleteView succeeded")
	}
	return nil
}

func checkLoggingCreateLink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateLink(ctx, &loggingpb.CreateLinkRequest{
		Parent: loggingBucketName(cfg),
		LinkId: loggingLinkID(cfg),
		Link: &loggingpb.Link{
			Description:     "conformance",
			BigqueryDataset: &loggingpb.BigQueryDataset{DatasetId: loggingLinkDatasetID(cfg)},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateLink: %w", err)
	}
	l, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("CreateLink wait: %w", err)
	}
	if l.GetName() != loggingLinkName(cfg) {
		return fmt.Errorf("link name = %q, want %q", l.GetName(), loggingLinkName(cfg))
	}
	if l.GetBigqueryDataset().GetDatasetId() != loggingLinkDatasetID(cfg) {
		return fmt.Errorf("link dataset = %q, want %q", l.GetBigqueryDataset().GetDatasetId(), loggingLinkDatasetID(cfg))
	}
	return nil
}

func checkLoggingGetLink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	l, err := client.GetLink(ctx, &loggingpb.GetLinkRequest{Name: loggingLinkName(cfg)})
	if err != nil {
		return fmt.Errorf("GetLink: %w", err)
	}
	if l.GetDescription() != "conformance" {
		return fmt.Errorf("link description = %q, want %q", l.GetDescription(), "conformance")
	}
	return nil
}

func checkLoggingListLinks(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	it := client.ListLinks(ctx, &loggingpb.ListLinksRequest{Parent: loggingBucketName(cfg)})
	for {
		l, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created link %q not listed", loggingLinkID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListLinks: %w", err)
		}
		if l.GetName() == loggingLinkName(cfg) {
			return nil
		}
	}
}

func checkLoggingDeleteLink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	op, err := client.DeleteLink(ctx, &loggingpb.DeleteLinkRequest{Name: loggingLinkName(cfg)})
	if err != nil {
		return fmt.Errorf("DeleteLink: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteLink wait: %w", err)
	}
	if _, err := client.GetLink(ctx, &loggingpb.GetLinkRequest{Name: loggingLinkName(cfg)}); err == nil {
		return fmt.Errorf("GetLink after DeleteLink succeeded")
	}
	return nil
}

func checkLoggingGetSettings(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	st, err := client.GetSettings(ctx, &loggingpb.GetSettingsRequest{Name: loggingParent(cfg)})
	if err != nil {
		return fmt.Errorf("GetSettings: %w", err)
	}
	if st.GetKmsServiceAccountId() == "" {
		return fmt.Errorf("settings kmsServiceAccountId is empty")
	}
	return nil
}

func checkLoggingUpdateSettings(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	st, err := client.UpdateSettings(ctx, &loggingpb.UpdateSettingsRequest{
		Name:       loggingParent(cfg),
		Settings:   &loggingpb.Settings{StorageLocation: "us"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"storage_location"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSettings: %w", err)
	}
	if st.GetStorageLocation() != "us" {
		return fmt.Errorf("settings storageLocation = %q, want %q", st.GetStorageLocation(), "us")
	}
	if st.GetKmsServiceAccountId() == "" {
		return fmt.Errorf("settings lost its service account id")
	}
	return nil
}

func checkLoggingGetCmekSettings(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	cs, err := client.GetCmekSettings(ctx, &loggingpb.GetCmekSettingsRequest{Name: loggingParent(cfg)})
	if err != nil {
		return fmt.Errorf("GetCmekSettings: %w", err)
	}
	if cs.GetServiceAccountId() == "" {
		return fmt.Errorf("cmek serviceAccountId is empty")
	}
	return nil
}

func checkLoggingUpdateCmekSettings(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	const key = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
	cs, err := client.UpdateCmekSettings(ctx, &loggingpb.UpdateCmekSettingsRequest{
		Name:         loggingParent(cfg),
		CmekSettings: &loggingpb.CmekSettings{KmsKeyName: key},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"kms_key_name"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCmekSettings: %w", err)
	}
	if cs.GetKmsKeyName() != key {
		return fmt.Errorf("cmek kmsKeyName = %q, want %q", cs.GetKmsKeyName(), key)
	}
	return nil
}
