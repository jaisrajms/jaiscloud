package storage

import (
	"context"
	"reflect"
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestGRPCBucketModuleFieldsRoundTrip pins the CFT-module bucket fields on the
// gRPC surface (TGM1-3): website, logging, autoclass, hierarchical namespace,
// custom dual-region placement and IP filtering are stored on create and
// returned by get, under the same JSON-shaped keys the REST insert body uses so
// both transports read one bucket record.
func TestGRPCBucketModuleFieldsRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	terminal := "NEARLINE"
	mode := "Enabled"
	network := "projects/test-project/global/networks/default"
	allowAll := true
	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-fields",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "us-central1",
			Website: &storagepb.Bucket_Website{
				MainPageSuffix: "index.html",
				NotFoundPage:   "404.html",
			},
			Logging: &storagepb.Bucket_Logging{
				LogBucket:       "logs",
				LogObjectPrefix: "access/",
			},
			Autoclass: &storagepb.Bucket_Autoclass{
				Enabled:              true,
				TerminalStorageClass: &terminal,
			},
			HierarchicalNamespace: &storagepb.Bucket_HierarchicalNamespace{Enabled: true},
			CustomPlacementConfig: &storagepb.Bucket_CustomPlacementConfig{
				DataLocations: []string{"us-central1", "us-east1"},
			},
			IpFilter: &storagepb.Bucket_IpFilter{
				Mode: &mode,
				PublicNetworkSource: &storagepb.Bucket_IpFilter_PublicNetworkSource{
					AllowedIpCidrRanges: []string{"0.0.0.0/0"},
				},
				VpcNetworkSources: []*storagepb.Bucket_IpFilter_VpcNetworkSource{{
					Network:             &network,
					AllowedIpCidrRanges: []string{"10.0.0.0/8"},
				}},
				AllowCrossOrgVpcs:          true,
				AllowAllServiceAgentAccess: &allowAll,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	for _, b := range []*storagepb.Bucket{created, got} {
		if b.GetWebsite().GetMainPageSuffix() != "index.html" || b.GetWebsite().GetNotFoundPage() != "404.html" {
			t.Errorf("website = %+v, want index.html/404.html", b.GetWebsite())
		}
		if b.GetLogging().GetLogBucket() != "logs" || b.GetLogging().GetLogObjectPrefix() != "access/" {
			t.Errorf("logging = %+v, want logs/access/", b.GetLogging())
		}
		if !b.GetAutoclass().GetEnabled() || b.GetAutoclass().GetTerminalStorageClass() != "NEARLINE" {
			t.Errorf("autoclass = %+v, want enabled + NEARLINE", b.GetAutoclass())
		}
		if !b.GetHierarchicalNamespace().GetEnabled() {
			t.Errorf("hierarchicalNamespace = %+v, want enabled", b.GetHierarchicalNamespace())
		}
		// GCS serves custom-placement region codes uppercase.
		if want := []string{"US-CENTRAL1", "US-EAST1"}; !reflect.DeepEqual(b.GetCustomPlacementConfig().GetDataLocations(), want) {
			t.Errorf("customPlacementConfig = %v, want %v", b.GetCustomPlacementConfig().GetDataLocations(), want)
		}
		if b.GetIpFilter().GetMode() != "Enabled" ||
			!reflect.DeepEqual(b.GetIpFilter().GetPublicNetworkSource().GetAllowedIpCidrRanges(), []string{"0.0.0.0/0"}) ||
			len(b.GetIpFilter().GetVpcNetworkSources()) != 1 ||
			b.GetIpFilter().GetVpcNetworkSources()[0].GetNetwork() != network ||
			!b.GetIpFilter().GetAllowCrossOrgVpcs() ||
			!b.GetIpFilter().GetAllowAllServiceAgentAccess() {
			t.Errorf("ipFilter = %+v, want the created configuration", b.GetIpFilter())
		}
	}
}

// TestGRPCUpdateBucketModuleFields pins the mutable half under an update mask:
// website/logging/autoclass/ip_filter are applied (and cleared when the masked
// message is absent), while hierarchical_namespace and custom_placement_config
// are immutable after create and keep their stored values.
func TestGRPCUpdateBucketModuleFields(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-update-fields",
		Bucket: &storagepb.Bucket{
			Project: "projects/test-project", Location: "us-central1",
			Website:               &storagepb.Bucket_Website{MainPageSuffix: "index.html"},
			Logging:               &storagepb.Bucket_Logging{LogBucket: "logs"},
			Autoclass:             &storagepb.Bucket_Autoclass{Enabled: true},
			HierarchicalNamespace: &storagepb.Bucket_HierarchicalNamespace{Enabled: true},
			CustomPlacementConfig: &storagepb.Bucket_CustomPlacementConfig{DataLocations: []string{"us-central1", "us-east1"}},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	name := "projects/_/buckets/bucket-update-fields"

	// Apply website + autoclass, clear logging, and try to change the immutable
	// hierarchical_namespace/custom_placement_config (ignored).
	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:                  name,
			Website:               &storagepb.Bucket_Website{MainPageSuffix: "home.html", NotFoundPage: "404.html"},
			Autoclass:             &storagepb.Bucket_Autoclass{Enabled: false},
			HierarchicalNamespace: &storagepb.Bucket_HierarchicalNamespace{Enabled: false},
			CustomPlacementConfig: &storagepb.Bucket_CustomPlacementConfig{DataLocations: []string{"europe-west1", "europe-west2"}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			"website", "logging", "autoclass", "hierarchical_namespace", "custom_placement_config",
		}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if updated.GetWebsite().GetMainPageSuffix() != "home.html" || updated.GetWebsite().GetNotFoundPage() != "404.html" {
		t.Errorf("website after update = %+v, want home.html/404.html", updated.GetWebsite())
	}
	if updated.GetLogging() != nil {
		t.Errorf("logging after a masked clear = %+v, want nil", updated.GetLogging())
	}
	if updated.GetAutoclass().GetEnabled() {
		t.Errorf("autoclass after update = %+v, want disabled", updated.GetAutoclass())
	}
	if !updated.GetHierarchicalNamespace().GetEnabled() {
		t.Error("an update changed the immutable hierarchical_namespace")
	}
	wantLocations := []string{"US-CENTRAL1", "US-EAST1"}
	if !reflect.DeepEqual(updated.GetCustomPlacementConfig().GetDataLocations(), wantLocations) {
		t.Errorf("an update changed the immutable custom_placement_config: %v", updated.GetCustomPlacementConfig().GetDataLocations())
	}

	// The same "absent masked message clears" contract for an ip_filter block.
	mode := "Disabled"
	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:     name,
			IpFilter: &storagepb.Bucket_IpFilter{Mode: &mode},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ip_filter"}},
	}); err != nil {
		t.Fatalf("UpdateBucket(ip_filter): %v", err)
	}
	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: name})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetIpFilter().GetMode() != "Disabled" {
		t.Errorf("ipFilter mode = %q, want Disabled", got.GetIpFilter().GetMode())
	}
	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: name},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ip_filter"}},
	}); err != nil {
		t.Fatalf("UpdateBucket(clear ip_filter): %v", err)
	}
	got, err = client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: name})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetIpFilter() != nil {
		t.Errorf("ipFilter after a masked clear = %+v, want nil", got.GetIpFilter())
	}
}

// TestGRPCUpdateBucketNestedMaskPaths pins that an update mask naming a leaf
// inside a block is applied. The API's field masks carry leaf paths
// (website.main_page_suffix, logging.log_bucket, ip_filter.mode); a block-level
// apply that only matched the block name silently ignored them, so a client
// could PATCH a nested field and read back the old value (FC3).
func TestGRPCUpdateBucketNestedMaskPaths(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-nested-mask",
		Bucket: &storagepb.Bucket{
			Project: "projects/test-project", Location: "us-central1",
			Website: &storagepb.Bucket_Website{MainPageSuffix: "index.html", NotFoundPage: "404.html"},
			Logging: &storagepb.Bucket_Logging{LogBucket: "logs", LogObjectPrefix: "access/"},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	name := "projects/_/buckets/bucket-nested-mask"
	mode := "Disabled"

	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:     name,
			Website:  &storagepb.Bucket_Website{MainPageSuffix: "home.html"},
			Logging:  &storagepb.Bucket_Logging{LogBucket: "audit-logs"},
			IpFilter: &storagepb.Bucket_IpFilter{Mode: &mode},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			"website.main_page_suffix", "logging.log_bucket", "ip_filter.mode",
		}},
	}); err != nil {
		t.Fatalf("UpdateBucket(nested masks): %v", err)
	}
	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: name})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetWebsite().GetMainPageSuffix() != "home.html" {
		t.Errorf("website.main_page_suffix = %q, want home.html", got.GetWebsite().GetMainPageSuffix())
	}
	if got.GetLogging().GetLogBucket() != "audit-logs" {
		t.Errorf("logging.log_bucket = %q, want audit-logs", got.GetLogging().GetLogBucket())
	}
	if got.GetIpFilter().GetMode() != "Disabled" {
		t.Errorf("ip_filter.mode = %q, want Disabled", got.GetIpFilter().GetMode())
	}
}
