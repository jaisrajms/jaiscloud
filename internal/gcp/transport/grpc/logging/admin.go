package logging

import (
	"context"
	"time"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file implements the Cloud Logging Admin v2 RPCs on ConfigService:
// buckets (sync + async), views, BigQuery links, and the Settings/CMEK records.
// The async/mutating methods that real Logging returns as operations
// (CreateBucketAsync/UpdateBucketAsync, CreateLink/DeleteLink, CopyLogEntries)
// return a terminal done=true operation wrapping the typed response, matching
// the emulator's synchronous-operation convention (see firestoreadmin).

// adminParent resolves a create/list parent, falling back to the project's
// global location when the request carries none.
func (s *ConfigService) adminParent(ctx context.Context, parent string) string {
	if parent != "" {
		return parent
	}
	return "projects/" + grpcutil.ProjectFromMetadata(ctx, s.defaultProj) + "/locations/global"
}

// settingsName resolves a Settings/CMEK resource name, falling back to the
// project container when the request carries none.
func (s *ConfigService) settingsName(ctx context.Context, name string) string {
	if name != "" {
		return name
	}
	return resource.ResourceID(grpcutil.ProjectFromMetadata(ctx, s.defaultProj))("project", "")
}

// doneOperation wraps a typed response in a terminal google.longrunning.Operation.
func doneOperation(name string, resp proto.Message) (*longrunningpb.Operation, error) {
	any, err := anypb.New(resp)
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", err.Error(), 500))
	}
	return &longrunningpb.Operation{
		Name:   name,
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: any},
	}, nil
}

// ─── buckets ──────────────────────────────────────────────────────────────────

func (s *ConfigService) ListBuckets(ctx context.Context, req *loggingpb.ListBucketsRequest) (*loggingpb.ListBucketsResponse, error) {
	parent := s.adminParent(ctx, req.GetParent())
	page, next, err := s.core.ListBuckets(ctx, parent, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogBucket, 0, len(page))
	for _, b := range page {
		out = append(out, bucketToProto(parent, b))
	}
	return &loggingpb.ListBucketsResponse{Buckets: out, NextPageToken: next}, nil
}

func (s *ConfigService) GetBucket(ctx context.Context, req *loggingpb.GetBucketRequest) (*loggingpb.LogBucket, error) {
	b, err := s.core.GetBucket(ctx, req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	return bucketToProto(parentOfBucket(req.GetName()), b), nil
}

func (s *ConfigService) CreateBucket(ctx context.Context, req *loggingpb.CreateBucketRequest) (*loggingpb.LogBucket, error) {
	parent := s.adminParent(ctx, req.GetParent())
	b, err := s.core.CreateBucket(ctx, parent, req.GetBucketId(), bucketFromProto(req.GetBucket()))
	if err != nil {
		return nil, mapError(err)
	}
	return bucketToProto(parent, b), nil
}

func (s *ConfigService) CreateBucketAsync(ctx context.Context, req *loggingpb.CreateBucketRequest) (*longrunningpb.Operation, error) {
	parent := s.adminParent(ctx, req.GetParent())
	b, err := s.core.CreateBucket(ctx, parent, req.GetBucketId(), bucketFromProto(req.GetBucket()))
	if err != nil {
		return nil, mapError(err)
	}
	return doneOperation(parent+"/operations/create-bucket-"+b.Name, bucketToProto(parent, b))
}

func (s *ConfigService) UpdateBucket(ctx context.Context, req *loggingpb.UpdateBucketRequest) (*loggingpb.LogBucket, error) {
	b, err := s.core.UpdateBucket(ctx, req.GetName(), bucketFromProto(req.GetBucket()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return bucketToProto(parentOfBucket(req.GetName()), b), nil
}

func (s *ConfigService) UpdateBucketAsync(ctx context.Context, req *loggingpb.UpdateBucketRequest) (*longrunningpb.Operation, error) {
	b, err := s.core.UpdateBucket(ctx, req.GetName(), bucketFromProto(req.GetBucket()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return doneOperation(parentOfBucket(req.GetName())+"/operations/update-bucket-"+b.Name, bucketToProto(parentOfBucket(req.GetName()), b))
}

func (s *ConfigService) DeleteBucket(ctx context.Context, req *loggingpb.DeleteBucketRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteBucket(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *ConfigService) UndeleteBucket(ctx context.Context, req *loggingpb.UndeleteBucketRequest) (*emptypb.Empty, error) {
	if err := s.core.UndeleteBucket(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── views ────────────────────────────────────────────────────────────────────

func (s *ConfigService) ListViews(ctx context.Context, req *loggingpb.ListViewsRequest) (*loggingpb.ListViewsResponse, error) {
	page, next, err := s.core.ListViews(ctx, req.GetParent(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogView, 0, len(page))
	for _, v := range page {
		out = append(out, viewToProto(req.GetParent(), v))
	}
	return &loggingpb.ListViewsResponse{Views: out, NextPageToken: next}, nil
}

func (s *ConfigService) GetView(ctx context.Context, req *loggingpb.GetViewRequest) (*loggingpb.LogView, error) {
	v, err := s.core.GetView(ctx, req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	return viewToProto(parentOfView(req.GetName()), v), nil
}

func (s *ConfigService) CreateView(ctx context.Context, req *loggingpb.CreateViewRequest) (*loggingpb.LogView, error) {
	v, err := s.core.CreateView(ctx, req.GetParent(), req.GetViewId(), viewFromProto(req.GetView()))
	if err != nil {
		return nil, mapError(err)
	}
	return viewToProto(req.GetParent(), v), nil
}

func (s *ConfigService) UpdateView(ctx context.Context, req *loggingpb.UpdateViewRequest) (*loggingpb.LogView, error) {
	v, err := s.core.UpdateView(ctx, req.GetName(), viewFromProto(req.GetView()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return viewToProto(parentOfView(req.GetName()), v), nil
}

func (s *ConfigService) DeleteView(ctx context.Context, req *loggingpb.DeleteViewRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteView(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── links ────────────────────────────────────────────────────────────────────

func (s *ConfigService) ListLinks(ctx context.Context, req *loggingpb.ListLinksRequest) (*loggingpb.ListLinksResponse, error) {
	page, next, err := s.core.ListLinks(ctx, req.GetParent(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.Link, 0, len(page))
	for _, l := range page {
		out = append(out, linkToProto(req.GetParent(), l))
	}
	return &loggingpb.ListLinksResponse{Links: out, NextPageToken: next}, nil
}

func (s *ConfigService) GetLink(ctx context.Context, req *loggingpb.GetLinkRequest) (*loggingpb.Link, error) {
	l, err := s.core.GetLink(ctx, req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	return linkToProto(parentOfLink(req.GetName()), l), nil
}

func (s *ConfigService) CreateLink(ctx context.Context, req *loggingpb.CreateLinkRequest) (*longrunningpb.Operation, error) {
	l, err := s.core.CreateLink(ctx, req.GetParent(), req.GetLinkId(), linkFromProto(req.GetLink()))
	if err != nil {
		return nil, mapError(err)
	}
	return doneOperation(operationLocation(req.GetParent())+"/operations/create-link-"+l.Name, linkToProto(req.GetParent(), l))
}

func (s *ConfigService) DeleteLink(ctx context.Context, req *loggingpb.DeleteLinkRequest) (*longrunningpb.Operation, error) {
	if err := s.core.DeleteLink(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}
	return doneOperation(operationLocation(parentOfLink(req.GetName()))+"/operations/delete-link-"+req.GetName(), &emptypb.Empty{})
}

// ─── settings / cmek ──────────────────────────────────────────────────────────

func (s *ConfigService) GetSettings(ctx context.Context, req *loggingpb.GetSettingsRequest) (*loggingpb.Settings, error) {
	name := s.settingsName(ctx, req.GetName())
	st, err := s.core.GetSettings(ctx, name)
	if err != nil {
		return nil, mapError(err)
	}
	return settingsToProto(name, st), nil
}

func (s *ConfigService) UpdateSettings(ctx context.Context, req *loggingpb.UpdateSettingsRequest) (*loggingpb.Settings, error) {
	name := s.settingsName(ctx, req.GetName())
	st, err := s.core.UpdateSettings(ctx, name, settingsFromProto(req.GetSettings()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return settingsToProto(name, st), nil
}

func (s *ConfigService) GetCmekSettings(ctx context.Context, req *loggingpb.GetCmekSettingsRequest) (*loggingpb.CmekSettings, error) {
	name := s.settingsName(ctx, req.GetName())
	st, err := s.core.GetCmekSettings(ctx, name)
	if err != nil {
		return nil, mapError(err)
	}
	return cmekToProto(name, st), nil
}

func (s *ConfigService) UpdateCmekSettings(ctx context.Context, req *loggingpb.UpdateCmekSettingsRequest) (*loggingpb.CmekSettings, error) {
	name := s.settingsName(ctx, req.GetName())
	st, err := s.core.UpdateCmekSettings(ctx, name, cmekFromProto(req.GetCmekSettings()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return cmekToProto(name, st), nil
}

// ─── transcoding ──────────────────────────────────────────────────────────────

func parentOfBucket(name string) string {
	if lp, _, err := core.ParseBucketName(name); err == nil {
		return lp
	}
	return ""
}

func parentOfView(name string) string {
	if b, _, err := core.ParseViewName(name); err == nil {
		return b
	}
	return ""
}

func parentOfLink(name string) string {
	if b, _, err := core.ParseLinkName(name); err == nil {
		return b
	}
	return ""
}

// operationLocation maps a bucket name to the location parent real GCP scopes
// that bucket's operations under.
func operationLocation(bucket string) string {
	if lp, _, err := core.ParseBucketName(bucket); err == nil {
		return lp
	}
	return bucket
}

func bucketFromProto(p *loggingpb.LogBucket) loggingstore.LogBucket {
	if p == nil {
		return loggingstore.LogBucket{}
	}
	b := loggingstore.LogBucket{
		Name:             p.GetName(),
		Description:      p.GetDescription(),
		RetentionDays:    p.GetRetentionDays(),
		Locked:           p.GetLocked(),
		LifecycleState:   lifecycleName(p.GetLifecycleState()),
		AnalyticsEnabled: p.GetAnalyticsEnabled(),
		RestrictedFields: append([]string(nil), p.GetRestrictedFields()...),
	}
	for _, ic := range p.GetIndexConfigs() {
		b.IndexConfigs = append(b.IndexConfigs, loggingstore.LogIndexConfig{
			FieldPath: ic.GetFieldPath(),
			Type:      ic.GetType().String(),
		})
	}
	if p.GetCmekSettings() != nil {
		c := cmekFromProto(p.GetCmekSettings())
		b.Cmek = &c
	}
	return b
}

func bucketToProto(parent string, b loggingstore.LogBucket) *loggingpb.LogBucket {
	out := &loggingpb.LogBucket{
		Name:             core.BucketResourceName(parent, b.Name),
		Description:      b.Description,
		RetentionDays:    b.RetentionDays,
		Locked:           b.Locked,
		LifecycleState:   lifecycleValue(b.LifecycleState),
		AnalyticsEnabled: b.AnalyticsEnabled,
		RestrictedFields: append([]string(nil), b.RestrictedFields...),
	}
	for _, ic := range b.IndexConfigs {
		out.IndexConfigs = append(out.IndexConfigs, &loggingpb.IndexConfig{
			FieldPath:  ic.FieldPath,
			Type:       indexTypeValue(ic.Type),
			CreateTime: nanosToTimestamp(ic.CreateTime),
		})
	}
	if b.Cmek != nil {
		out.CmekSettings = cmekToProto(core.BucketResourceName(parent, b.Name), *b.Cmek)
	}
	if b.CreateTime != 0 {
		out.CreateTime = timestamp.Proto(nanosToTime(b.CreateTime))
	}
	if b.UpdateTime != 0 {
		out.UpdateTime = timestamp.Proto(nanosToTime(b.UpdateTime))
	}
	return out
}

func viewFromProto(p *loggingpb.LogView) loggingstore.LogView {
	if p == nil {
		return loggingstore.LogView{}
	}
	return loggingstore.LogView{
		Name:        p.GetName(),
		Description: p.GetDescription(),
		Filter:      p.GetFilter(),
	}
}

func viewToProto(parent string, v loggingstore.LogView) *loggingpb.LogView {
	out := &loggingpb.LogView{
		Name:        core.ViewResourceName(parent, v.Name),
		Description: v.Description,
		Filter:      v.Filter,
	}
	if v.CreateTime != 0 {
		out.CreateTime = timestamp.Proto(nanosToTime(v.CreateTime))
	}
	if v.UpdateTime != 0 {
		out.UpdateTime = timestamp.Proto(nanosToTime(v.UpdateTime))
	}
	return out
}

func linkFromProto(p *loggingpb.Link) loggingstore.LogLink {
	if p == nil {
		return loggingstore.LogLink{}
	}
	l := loggingstore.LogLink{
		Name:        p.GetName(),
		Description: p.GetDescription(),
	}
	if p.GetBigqueryDataset() != nil {
		l.BigQueryDatasetID = p.GetBigqueryDataset().GetDatasetId()
	}
	return l
}

func linkToProto(parent string, l loggingstore.LogLink) *loggingpb.Link {
	out := &loggingpb.Link{
		Name:            core.LinkResourceName(parent, l.Name),
		Description:     l.Description,
		LifecycleState:  lifecycleValue(l.LifecycleState),
		BigqueryDataset: &loggingpb.BigQueryDataset{DatasetId: l.BigQueryDatasetID},
	}
	if l.CreateTime != 0 {
		out.CreateTime = timestamp.Proto(nanosToTime(l.CreateTime))
	}
	return out
}

func settingsFromProto(p *loggingpb.Settings) loggingstore.LogSettings {
	if p == nil {
		return loggingstore.LogSettings{}
	}
	return loggingstore.LogSettings{
		KmsKeyName:          p.GetKmsKeyName(),
		KmsServiceAccountID: p.GetKmsServiceAccountId(),
		StorageLocation:     p.GetStorageLocation(),
		DisableDefaultSink:  p.GetDisableDefaultSink(),
	}
}

func settingsToProto(name string, s loggingstore.LogSettings) *loggingpb.Settings {
	return &loggingpb.Settings{
		Name:                name,
		KmsKeyName:          s.KmsKeyName,
		KmsServiceAccountId: s.KmsServiceAccountID,
		StorageLocation:     s.StorageLocation,
		DisableDefaultSink:  s.DisableDefaultSink,
	}
}

func cmekFromProto(p *loggingpb.CmekSettings) loggingstore.LogCmekSettings {
	if p == nil {
		return loggingstore.LogCmekSettings{}
	}
	return loggingstore.LogCmekSettings{
		KmsKeyName:        p.GetKmsKeyName(),
		KmsKeyVersionName: p.GetKmsKeyVersionName(),
		ServiceAccountID:  p.GetServiceAccountId(),
	}
}

func cmekToProto(name string, c loggingstore.LogCmekSettings) *loggingpb.CmekSettings {
	return &loggingpb.CmekSettings{
		Name:              name,
		KmsKeyName:        c.KmsKeyName,
		KmsKeyVersionName: c.KmsKeyVersionName,
		ServiceAccountId:  c.ServiceAccountID,
	}
}

func lifecycleName(v loggingpb.LifecycleState) string {
	switch v {
	case loggingpb.LifecycleState_ACTIVE:
		return "ACTIVE"
	case loggingpb.LifecycleState_DELETE_REQUESTED:
		return "DELETE_REQUESTED"
	case loggingpb.LifecycleState_UPDATING:
		return "UPDATING"
	case loggingpb.LifecycleState_CREATING:
		return "CREATING"
	case loggingpb.LifecycleState_FAILED:
		return "FAILED"
	default:
		return ""
	}
}

func lifecycleValue(name string) loggingpb.LifecycleState {
	switch name {
	case "ACTIVE":
		return loggingpb.LifecycleState_ACTIVE
	case "DELETE_REQUESTED":
		return loggingpb.LifecycleState_DELETE_REQUESTED
	case "UPDATING":
		return loggingpb.LifecycleState_UPDATING
	case "CREATING":
		return loggingpb.LifecycleState_CREATING
	case "FAILED":
		return loggingpb.LifecycleState_FAILED
	default:
		return loggingpb.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED
	}
}

func indexTypeValue(name string) loggingpb.IndexType {
	if v, ok := loggingpb.IndexType_value[name]; ok {
		return loggingpb.IndexType(v)
	}
	return loggingpb.IndexType_INDEX_TYPE_UNSPECIFIED
}

func nanosToTime(nanos int64) time.Time { return time.Unix(0, nanos).UTC() }

func nanosToTimestamp(nanos int64) *timestamppb.Timestamp {
	if nanos == 0 {
		return nil
	}
	return timestamp.Proto(nanosToTime(nanos))
}
