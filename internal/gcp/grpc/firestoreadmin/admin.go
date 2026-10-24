package firestoreadmin

// Firestore Admin control-plane RPCs (google.firestore.admin.v1) beyond the
// composite-index CRUD in service.go: databases, collection-group fields,
// backup schedules, backups and user creds. All domain logic lives in the
// shared transport-neutral provider/firestore.Service; this file is the wire
// adapter (proto ↔ stored shape) and the resource-name parsing.

import (
	"context"
	"strings"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ─── generic operation wrapper ────────────────────────────────────────────────

// operationResponse wraps a protobuf message in a terminal
// google.longrunning.Operation (done=true). The emulator's Admin control plane
// is synchronous, so there is nothing to poll.
func operationResponse(opName string, msg proto.Message) (*longrunningpb.Operation, error) {
	resp, err := anypb.New(msg)
	if err != nil {
		return nil, mapError(model.NewProviderError("Internal", err.Error(), 500))
	}
	return &longrunningpb.Operation{
		Name:   opName,
		Done:   true,
		Result: &longrunningpb.Operation_Response{Response: resp},
	}, nil
}

// ─── name parsing ─────────────────────────────────────────────────────────────

// projectFromParent extracts the project from any "projects/{project}/..."
// resource name; "" when the name is empty or malformed.
func projectFromParent(name string) string {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

// splitDatabaseName parses "projects/{p}/databases/{db}".
func splitDatabaseName(name string) (project, database string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "databases" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// splitFieldName parses
// "projects/{p}/databases/{db}/collectionGroups/{cg}/fields/{fieldPath}".
func splitFieldName(name string) (project, database, cg, fieldPath string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "databases" ||
		parts[4] != "collectionGroups" || parts[6] != "fields" {
		return "", "", "", "", false
	}
	return parts[1], parts[3], parts[5], parts[7], true
}

// splitUserCredsName parses "projects/{p}/databases/{db}/userCreds/{id}".
func splitUserCredsName(name string) (project, database, id string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "userCreds" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// splitBackupScheduleName parses
// "projects/{p}/databases/{db}/backupSchedules/{id}".
func splitBackupScheduleName(name string) (project, database, id string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "backupSchedules" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// splitBackupName parses "projects/{p}/locations/{loc}/backups/{id}".
func splitBackupName(name string) (project, location, id string, ok bool) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "backups" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// splitBackupParent parses "projects/{p}/locations/{loc}".
func splitBackupParent(parent string) (project, location string, ok bool) {
	parts := strings.Split(strings.Trim(parent, "/"), "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "locations" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// enumName maps a proto enum value to its name, returning "" for the
// unspecified (zero) value so callers can apply their own default.
func enumName(v int32, names map[int32]string) string {
	if v == 0 {
		return ""
	}
	return names[v]
}

// ─── Database conversions ─────────────────────────────────────────────────────

func databaseFromProto(p *adminpb.Database) firestoreprovider.DatabaseDef {
	d := firestoreprovider.DatabaseDef{
		Name:                          p.GetName(),
		Uid:                           p.GetUid(),
		LocationID:                    p.GetLocationId(),
		KeyPrefix:                     p.GetKeyPrefix(),
		Etag:                          p.GetEtag(),
		Type:                          enumName(int32(p.GetType()), adminpb.Database_DatabaseType_name),
		ConcurrencyMode:               enumName(int32(p.GetConcurrencyMode()), adminpb.Database_ConcurrencyMode_name),
		AppEngineIntegrationMode:      enumName(int32(p.GetAppEngineIntegrationMode()), adminpb.Database_AppEngineIntegrationMode_name),
		PointInTimeRecoveryEnablement: enumName(int32(p.GetPointInTimeRecoveryEnablement()), adminpb.Database_PointInTimeRecoveryEnablement_name),
		DeleteProtectionState:         enumName(int32(p.GetDeleteProtectionState()), adminpb.Database_DeleteProtectionState_name),
		DatabaseEdition:               enumName(int32(p.GetDatabaseEdition()), adminpb.Database_DatabaseEdition_name),
	}
	if ts := p.GetCreateTime(); ts != nil {
		d.CreateTime = ts.AsTime()
	}
	if ts := p.GetUpdateTime(); ts != nil {
		d.UpdateTime = ts.AsTime()
	}
	if ts := p.GetDeleteTime(); ts != nil {
		d.DeleteTime = ts.AsTime()
	}
	if dur := p.GetVersionRetentionPeriod(); dur != nil {
		d.VersionRetentionPeriod = dur.AsDuration()
	}
	return d
}

func databaseToProto(d firestoreprovider.DatabaseDef) *adminpb.Database {
	out := &adminpb.Database{
		Name:       d.Name,
		Uid:        d.Uid,
		LocationId: d.LocationID,
		KeyPrefix:  d.KeyPrefix,
		Etag:       d.Etag,
	}
	if !d.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(d.CreateTime)
	}
	if !d.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(d.UpdateTime)
	}
	if !d.DeleteTime.IsZero() {
		out.DeleteTime = timestamp.Proto(d.DeleteTime)
	}
	if d.Type != "" {
		out.Type = adminpb.Database_DatabaseType(adminpb.Database_DatabaseType_value[d.Type])
	}
	if d.ConcurrencyMode != "" {
		out.ConcurrencyMode = adminpb.Database_ConcurrencyMode(adminpb.Database_ConcurrencyMode_value[d.ConcurrencyMode])
	}
	if d.AppEngineIntegrationMode != "" {
		out.AppEngineIntegrationMode = adminpb.Database_AppEngineIntegrationMode(adminpb.Database_AppEngineIntegrationMode_value[d.AppEngineIntegrationMode])
	}
	if d.PointInTimeRecoveryEnablement != "" {
		out.PointInTimeRecoveryEnablement = adminpb.Database_PointInTimeRecoveryEnablement(adminpb.Database_PointInTimeRecoveryEnablement_value[d.PointInTimeRecoveryEnablement])
	}
	if d.DeleteProtectionState != "" {
		out.DeleteProtectionState = adminpb.Database_DeleteProtectionState(adminpb.Database_DeleteProtectionState_value[d.DeleteProtectionState])
	}
	if d.DatabaseEdition != "" {
		out.DatabaseEdition = adminpb.Database_DatabaseEdition(adminpb.Database_DatabaseEdition_value[d.DatabaseEdition])
	}
	if d.VersionRetentionPeriod > 0 {
		out.VersionRetentionPeriod = durationpb.New(d.VersionRetentionPeriod)
	}
	// earliest_version_time is OUTPUT_ONLY and continuously updated
	// (database.proto: "populated with `now - version_retention_period`"), so it
	// is derived per response instead of being stored (DC54).
	out.EarliestVersionTime = timestamp.Proto(firestoreprovider.EarliestVersionTime(d))
	return out
}

// ─── Database RPCs ────────────────────────────────────────────────────────────

// CreateDatabase creates a named database and returns a terminal operation.
func (s *Service) CreateDatabase(ctx context.Context, req *adminpb.CreateDatabaseRequest) (*longrunningpb.Operation, error) {
	project := projectFromParent(req.GetParent())
	if project == "" {
		project = s.resolveProject(ctx)
	}
	db, opName, err := s.svc.CreateDatabaseDef(ctx, project, req.GetDatabaseId(), databaseFromProto(req.GetDatabase()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationResponse(opName, databaseToProto(db))
}

// GetDatabase returns a single database.
func (s *Service) GetDatabase(ctx context.Context, req *adminpb.GetDatabaseRequest) (*adminpb.Database, error) {
	project, database, ok := splitDatabaseName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	db, err := s.svc.GetDatabase(ctx, project, database)
	if err != nil {
		return nil, mapError(err)
	}
	return databaseToProto(db), nil
}

// ListDatabases returns every database in the project.
func (s *Service) ListDatabases(ctx context.Context, req *adminpb.ListDatabasesRequest) (*adminpb.ListDatabasesResponse, error) {
	project := projectFromParent(req.GetParent())
	if project == "" {
		project = s.resolveProject(ctx)
	}
	dbs, err := s.svc.ListDatabases(ctx, project, req.GetShowDeleted())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.Database, 0, len(dbs))
	for _, d := range dbs {
		out = append(out, databaseToProto(d))
	}
	return &adminpb.ListDatabasesResponse{Databases: out}, nil
}

// UpdateDatabase applies a field-mask update and returns a terminal operation.
func (s *Service) UpdateDatabase(ctx context.Context, req *adminpb.UpdateDatabaseRequest) (*longrunningpb.Operation, error) {
	project, database, ok := splitDatabaseName(req.GetDatabase().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	// A present-but-empty mask is AIP-134's no-op; only a nil mask is omitted.
	mask := req.GetUpdateMask().GetPaths()
	db, opName, err := s.svc.UpdateDatabaseDef(ctx, project, database, databaseFromProto(req.GetDatabase()), mask, req.GetUpdateMask() != nil, req.GetDatabase().GetEtag())
	if err != nil {
		return nil, mapError(err)
	}
	return operationResponse(opName, databaseToProto(db))
}

// DeleteDatabase soft-deletes a database and returns a terminal operation.
func (s *Service) DeleteDatabase(ctx context.Context, req *adminpb.DeleteDatabaseRequest) (*longrunningpb.Operation, error) {
	project, database, ok := splitDatabaseName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	deleted, opName, err := s.svc.DeleteDatabaseDef(ctx, project, database, req.GetEtag())
	if err != nil {
		return nil, mapError(err)
	}
	return operationResponse(opName, databaseToProto(deleted))
}

// ─── Field conversions ────────────────────────────────────────────────────────

func fieldFromProto(p *adminpb.Field) firestoreprovider.FieldDef {
	f := firestoreprovider.FieldDef{Name: p.GetName()}
	if ic := p.GetIndexConfig(); ic != nil {
		fic := &firestoreprovider.FieldIndexConfig{
			UsesAncestorConfig: ic.GetUsesAncestorConfig(),
			AncestorField:      ic.GetAncestorField(),
			Reverting:          ic.GetReverting(),
		}
		for _, ix := range ic.GetIndexes() {
			fic.Indexes = append(fic.Indexes, indexFromProto(ix))
		}
		f.IndexConfig = fic
	}
	if tc := p.GetTtlConfig(); tc != nil {
		ft := &firestoreprovider.FieldTtlConfig{
			State: enumName(int32(tc.GetState()), adminpb.Field_TtlConfig_State_name),
		}
		if d := tc.GetExpirationOffset(); d != nil {
			ft.ExpirationOffset = d.AsDuration()
		}
		f.TtlConfig = ft
	}
	return f
}

func fieldToProto(f firestoreprovider.FieldDef) *adminpb.Field {
	out := &adminpb.Field{Name: f.Name}
	if f.IndexConfig != nil {
		ic := &adminpb.Field_IndexConfig{
			UsesAncestorConfig: f.IndexConfig.UsesAncestorConfig,
			AncestorField:      f.IndexConfig.AncestorField,
			Reverting:          f.IndexConfig.Reverting,
		}
		for _, ix := range f.IndexConfig.Indexes {
			ic.Indexes = append(ic.Indexes, indexToProto(ix))
		}
		out.IndexConfig = ic
	}
	if f.TtlConfig != nil {
		tc := &adminpb.Field_TtlConfig{
			State: adminpb.Field_TtlConfig_State(adminpb.Field_TtlConfig_State_value[f.TtlConfig.State]),
		}
		if f.TtlConfig.ExpirationOffset > 0 {
			tc.ExpirationOffset = durationpb.New(f.TtlConfig.ExpirationOffset)
		}
		out.TtlConfig = tc
	}
	return out
}

// ─── Field RPCs ───────────────────────────────────────────────────────────────

// GetField returns a collection-group field.
func (s *Service) GetField(ctx context.Context, req *adminpb.GetFieldRequest) (*adminpb.Field, error) {
	project, database, cg, fieldPath, ok := splitFieldName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid field resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	f, err := s.svc.GetField(ctx, project, database, cg, fieldPath)
	if err != nil {
		return nil, mapError(err)
	}
	return fieldToProto(f), nil
}

// ListFields returns the configured collection-group fields, paginated.
func (s *Service) ListFields(ctx context.Context, req *adminpb.ListFieldsRequest) (*adminpb.ListFieldsResponse, error) {
	project, database, cg, ok := splitParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid collection group parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	fields, nextToken, err := s.svc.ListFields(ctx, project, database, cg, req.GetFilter(),
		firestoreprovider.NewPageParams(int(req.GetPageSize()), req.GetPageToken()))
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.Field, 0, len(fields))
	for _, f := range fields {
		out = append(out, fieldToProto(f))
	}
	return &adminpb.ListFieldsResponse{Fields: out, NextPageToken: nextToken}, nil
}

// UpdateField creates or updates a field's index/ttl config and returns a
// terminal operation.
func (s *Service) UpdateField(ctx context.Context, req *adminpb.UpdateFieldRequest) (*longrunningpb.Operation, error) {
	project, database, cg, fieldPath, ok := splitFieldName(req.GetField().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid field resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	field, opName, err := s.svc.UpdateFieldDef(ctx, project, database, cg, fieldPath,
		fieldFromProto(req.GetField()), req.GetUpdateMask().GetPaths(), req.GetUpdateMask() != nil)
	if err != nil {
		return nil, mapError(err)
	}
	return operationResponse(opName, fieldToProto(field))
}

// ─── User creds conversions ───────────────────────────────────────────────────

func userCredsToProto(u firestoreprovider.UserCredsDef) *adminpb.UserCreds {
	out := &adminpb.UserCreds{
		Name:           u.Name,
		SecurePassword: u.SecretPassword,
	}
	if !u.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(u.CreateTime)
	}
	if !u.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(u.UpdateTime)
	}
	if u.State != "" {
		out.State = adminpb.UserCreds_State(adminpb.UserCreds_State_value[u.State])
	}
	if u.Principal != "" {
		out.UserCredsIdentity = &adminpb.UserCreds_ResourceIdentity_{
			ResourceIdentity: &adminpb.UserCreds_ResourceIdentity{Principal: u.Principal},
		}
	}
	return out
}

// ─── User creds RPCs ──────────────────────────────────────────────────────────

// CreateUserCreds creates a user creds record, returning its secret once.
func (s *Service) CreateUserCreds(ctx context.Context, req *adminpb.CreateUserCredsRequest) (*adminpb.UserCreds, error) {
	project, database, ok := splitDatabaseName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	uc, err := s.svc.CreateUserCredsDef(ctx, project, database, req.GetUserCredsId(), firestoreprovider.UserCredsDef{})
	if err != nil {
		return nil, mapError(err)
	}
	return userCredsToProto(uc), nil
}

// GetUserCreds returns a user creds record (without its secret).
func (s *Service) GetUserCreds(ctx context.Context, req *adminpb.GetUserCredsRequest) (*adminpb.UserCreds, error) {
	project, database, id, ok := splitUserCredsName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	uc, err := s.svc.GetUserCreds(ctx, project, database, id)
	if err != nil {
		return nil, mapError(err)
	}
	return userCredsToProto(uc), nil
}

// ListUserCreds returns every user creds record in a database.
func (s *Service) ListUserCreds(ctx context.Context, req *adminpb.ListUserCredsRequest) (*adminpb.ListUserCredsResponse, error) {
	project, database, ok := splitDatabaseName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	ucs, err := s.svc.ListUserCreds(ctx, project, database)
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.UserCreds, 0, len(ucs))
	for _, uc := range ucs {
		out = append(out, userCredsToProto(uc))
	}
	return &adminpb.ListUserCredsResponse{UserCreds: out}, nil
}

// EnableUserCreds enables a user creds record.
func (s *Service) EnableUserCreds(ctx context.Context, req *adminpb.EnableUserCredsRequest) (*adminpb.UserCreds, error) {
	return s.setUserCredsState(ctx, req.GetName(), "ENABLED")
}

// DisableUserCreds disables a user creds record.
func (s *Service) DisableUserCreds(ctx context.Context, req *adminpb.DisableUserCredsRequest) (*adminpb.UserCreds, error) {
	return s.setUserCredsState(ctx, req.GetName(), "DISABLED")
}

func (s *Service) setUserCredsState(ctx context.Context, name, state string) (*adminpb.UserCreds, error) {
	project, database, id, ok := splitUserCredsName(name)
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	uc, err := s.svc.SetUserCredsState(ctx, project, database, id, state)
	if err != nil {
		return nil, mapError(err)
	}
	return userCredsToProto(uc), nil
}

// ResetUserPassword rotates and returns a user creds secret once.
func (s *Service) ResetUserPassword(ctx context.Context, req *adminpb.ResetUserPasswordRequest) (*adminpb.UserCreds, error) {
	project, database, id, ok := splitUserCredsName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	uc, err := s.svc.ResetUserPassword(ctx, project, database, id)
	if err != nil {
		return nil, mapError(err)
	}
	return userCredsToProto(uc), nil
}

// DeleteUserCreds removes a user creds record.
func (s *Service) DeleteUserCreds(ctx context.Context, req *adminpb.DeleteUserCredsRequest) (*emptypb.Empty, error) {
	project, database, id, ok := splitUserCredsName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	if err := s.svc.DeleteUserCreds(ctx, project, database, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── Backup schedule conversions ──────────────────────────────────────────────

func backupScheduleFromProto(p *adminpb.BackupSchedule) firestoreprovider.BackupScheduleDef {
	bs := firestoreprovider.BackupScheduleDef{}
	if d := p.GetRetention(); d != nil {
		bs.Retention = d.AsDuration()
	}
	switch r := p.GetRecurrence().(type) {
	case *adminpb.BackupSchedule_DailyRecurrence:
		bs.Recurrence = "DAILY"
	case *adminpb.BackupSchedule_WeeklyRecurrence:
		bs.Recurrence = "WEEKLY"
		bs.DayOfWeek = int(r.WeeklyRecurrence.GetDay())
	}
	return bs
}

func backupScheduleToProto(bs firestoreprovider.BackupScheduleDef) *adminpb.BackupSchedule {
	out := &adminpb.BackupSchedule{Name: bs.Name}
	if !bs.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(bs.CreateTime)
	}
	if !bs.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(bs.UpdateTime)
	}
	if bs.Retention > 0 {
		out.Retention = durationpb.New(bs.Retention)
	}
	switch bs.Recurrence {
	case "DAILY":
		out.Recurrence = &adminpb.BackupSchedule_DailyRecurrence{DailyRecurrence: &adminpb.DailyRecurrence{}}
	case "WEEKLY":
		out.Recurrence = &adminpb.BackupSchedule_WeeklyRecurrence{
			WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayOfWeek(bs.DayOfWeek)},
		}
	}
	return out
}

// dayOfWeek maps an int to the google.type.DayOfWeek enum value.
func dayOfWeek(v int) dayofweek.DayOfWeek { return dayofweek.DayOfWeek(v) }

// ─── Backup schedule RPCs ─────────────────────────────────────────────────────

// CreateBackupSchedule creates a backup schedule.
func (s *Service) CreateBackupSchedule(ctx context.Context, req *adminpb.CreateBackupScheduleRequest) (*adminpb.BackupSchedule, error) {
	project, database, ok := splitDatabaseName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	id := "1"
	if _, _, nameID, ok := splitBackupScheduleName(req.GetBackupSchedule().GetName()); ok {
		id = nameID
	}
	bs, err := s.svc.CreateBackupScheduleDef(ctx, project, database, id, backupScheduleFromProto(req.GetBackupSchedule()))
	if err != nil {
		return nil, mapError(err)
	}
	return backupScheduleToProto(bs), nil
}

// GetBackupSchedule returns a single backup schedule.
func (s *Service) GetBackupSchedule(ctx context.Context, req *adminpb.GetBackupScheduleRequest) (*adminpb.BackupSchedule, error) {
	project, database, id, ok := splitBackupScheduleName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	bs, err := s.svc.GetBackupSchedule(ctx, project, database, id)
	if err != nil {
		return nil, mapError(err)
	}
	return backupScheduleToProto(bs), nil
}

// ListBackupSchedules returns every backup schedule for a database.
func (s *Service) ListBackupSchedules(ctx context.Context, req *adminpb.ListBackupSchedulesRequest) (*adminpb.ListBackupSchedulesResponse, error) {
	project, database, ok := splitDatabaseName(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid database parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	bss, err := s.svc.ListBackupSchedules(ctx, project, database)
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.BackupSchedule, 0, len(bss))
	for _, bs := range bss {
		out = append(out, backupScheduleToProto(bs))
	}
	return &adminpb.ListBackupSchedulesResponse{BackupSchedules: out}, nil
}

// UpdateBackupSchedule applies a field-mask update to a backup schedule.
func (s *Service) UpdateBackupSchedule(ctx context.Context, req *adminpb.UpdateBackupScheduleRequest) (*adminpb.BackupSchedule, error) {
	project, database, id, ok := splitBackupScheduleName(req.GetBackupSchedule().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	bs, err := s.svc.UpdateBackupScheduleDef(ctx, project, database, id, backupScheduleFromProto(req.GetBackupSchedule()), req.GetUpdateMask().GetPaths(), req.GetUpdateMask() != nil)
	if err != nil {
		return nil, mapError(err)
	}
	return backupScheduleToProto(bs), nil
}

// DeleteBackupSchedule removes a backup schedule.
func (s *Service) DeleteBackupSchedule(ctx context.Context, req *adminpb.DeleteBackupScheduleRequest) (*emptypb.Empty, error) {
	project, database, id, ok := splitBackupScheduleName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	if err := s.svc.DeleteBackupSchedule(ctx, project, database, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── Backup conversions ───────────────────────────────────────────────────────

func backupToProto(b firestoreprovider.BackupDef) *adminpb.Backup {
	out := &adminpb.Backup{
		Name:        b.Name,
		Database:    b.Database,
		DatabaseUid: b.DatabaseUid,
	}
	if !b.SnapshotTime.IsZero() {
		out.SnapshotTime = timestamp.Proto(b.SnapshotTime)
	}
	if !b.ExpireTime.IsZero() {
		out.ExpireTime = timestamp.Proto(b.ExpireTime)
	}
	if b.State != "" {
		out.State = adminpb.Backup_State(adminpb.Backup_State_value[b.State])
	}
	// A READY backup carries measured statistics, including zero counts for a
	// database that holds nothing yet.
	if b.State == "READY" || b.DocumentCount != 0 || b.IndexCount != 0 || b.SizeBytes != 0 {
		out.Stats = &adminpb.Backup_Stats{
			SizeBytes:     b.SizeBytes,
			DocumentCount: b.DocumentCount,
			IndexCount:    b.IndexCount,
		}
	}
	return out
}

// ─── Backup RPCs ──────────────────────────────────────────────────────────────

// GetBackup returns a single backup.
func (s *Service) GetBackup(ctx context.Context, req *adminpb.GetBackupRequest) (*adminpb.Backup, error) {
	project, location, id, ok := splitBackupName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	b, err := s.svc.GetBackup(ctx, project, location, id)
	if err != nil {
		return nil, mapError(err)
	}
	return backupToProto(b), nil
}

// ListBackups returns the backups in a location.
func (s *Service) ListBackups(ctx context.Context, req *adminpb.ListBackupsRequest) (*adminpb.ListBackupsResponse, error) {
	project, location, ok := splitBackupParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup parent", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	backups, err := s.svc.ListBackups(ctx, project, location, req.GetFilter())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*adminpb.Backup, 0, len(backups))
	for _, b := range backups {
		out = append(out, backupToProto(b))
	}
	return &adminpb.ListBackupsResponse{Backups: out}, nil
}

// DeleteBackup removes a backup.
func (s *Service) DeleteBackup(ctx context.Context, req *adminpb.DeleteBackupRequest) (*emptypb.Empty, error) {
	project, location, id, ok := splitBackupName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid backup resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	if err := s.svc.DeleteBackup(ctx, project, location, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}
