// Package secretmanager implements the Cloud Secret Manager gRPC service
// (SecretManagerService plus the google.iam.v1.IAMPolicy surface for secret
// IAM) over the same shared secretmanagerstore.Store + policy +
// crypto.EnvelopeEncryptor backing the REST provider, so REST and gRPC share
// state.
package secretmanager

import (
	"context"
	"errors"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	secretmanagersvc "jaiscloud/internal/gcp/service/secretmanager"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rtSecretPolicy is the generic ResourceStore type for secret IAM policies,
// matching the REST provider's persisted type so both transports share state.
const rtSecretPolicy = "gcp_secret_policy"

// errSkipRotation aborts maybeRotate's atomic mutate without writing, when
// rotation turns out not to be due after the locked re-read.
var errSkipRotation = errors.New("secret rotation not due")

// Service implements secretmanagerpb.SecretManagerServiceServer and
// iampb.IAMPolicyServer over the shared stores.
type Service struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	iampb.UnimplementedIAMPolicyServer

	secrets     secretmanagerstore.Store
	resources   store.ResourceStore // IAM policies (control-plane)
	encryptor   crypto.EnvelopeEncryptor
	defaultProj string
	// publisher delivers the secret's event notifications (DC64); nil disables
	// them.
	publisher secretmanagersvc.Publisher
	// lifecycle is the proactive sweep worker (DC71/DC72), mirroring the REST
	// provider so a gRPC-only deployment still rotates/expires without reads.
	lifecycleMu     sync.Mutex
	lifecycleCancel context.CancelFunc
	lifecycleDone   chan struct{}
	lifecycleSeen   map[string]struct{}
	sweepInterval   time.Duration
}

// NewService returns a Secret Manager gRPC service backed by the shared stores.
// defaultProj is the config-default project used when a request carries none.
const defaultSweepInterval = time.Second

func NewService(secrets secretmanagerstore.Store, resources store.ResourceStore, encryptor crypto.EnvelopeEncryptor, defaultProj string) *Service {
	return &Service{
		secrets:       secrets,
		resources:     resources,
		encryptor:     encryptor,
		defaultProj:   defaultProj,
		lifecycleSeen: map[string]struct{}{},
		sweepInterval: defaultSweepInterval,
	}
}

// SetSweepInterval overrides the background lifecycle cadence (tests use a
// short interval; a non-positive value restores the default).
func (s *Service) SetSweepInterval(d time.Duration) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if d <= 0 {
		d = defaultSweepInterval
	}
	s.sweepInterval = d
}

// TrackProject registers a project for the proactive lifecycle sweep.
func (s *Service) TrackProject(project string) {
	if project == "" {
		return
	}
	s.lifecycleMu.Lock()
	if s.lifecycleSeen == nil {
		s.lifecycleSeen = map[string]struct{}{}
	}
	s.lifecycleSeen[project] = struct{}{}
	s.lifecycleMu.Unlock()
}

// Start launches the non-blocking lifecycle sweep. It is a no-op if already
// running; Stop (or the supplied context ending) shuts it down.
func (s *Service) Start(ctx context.Context) {
	s.lifecycleMu.Lock()
	if s.lifecycleCancel != nil {
		s.lifecycleMu.Unlock()
		return
	}
	interval := s.sweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.lifecycleCancel, s.lifecycleDone = cancel, done
	s.lifecycleMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				s.sweepLifecycle(runCtx)
			}
		}
	}()
}

// Stop halts the lifecycle sweep and waits for the current tick to finish.
func (s *Service) Stop() {
	s.lifecycleMu.Lock()
	cancel, done := s.lifecycleCancel, s.lifecycleDone
	s.lifecycleCancel, s.lifecycleDone = nil, nil
	s.lifecycleMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// sweepLifecycle applies every due time-driven transition across the projects
// this service has seen. Atomic store helpers ensure a concurrent read or tick
// cannot double-apply a transition or duplicate its notification.
func (s *Service) sweepLifecycle(ctx context.Context) {
	s.lifecycleMu.Lock()
	projects := make([]string, 0, len(s.lifecycleSeen))
	for project := range s.lifecycleSeen {
		projects = append(projects, project)
	}
	s.lifecycleMu.Unlock()

	for _, project := range projects {
		if ctx.Err() != nil {
			return
		}
		secrets, err := s.secrets.ListSecrets(ctx, project)
		if err != nil {
			continue
		}
		now := clock.Now()
		for _, sec := range secrets {
			if secretmanagersvc.Expired(sec, now) {
				_ = s.expireSecret(ctx, project, sec.ID)
				continue
			}
			if sec.Rotation != nil && sec.Rotation.NextRotationTime != "" {
				if next, err := time.Parse(time.RFC3339Nano, sec.Rotation.NextRotationTime); err == nil && !now.Before(next) {
					s.maybeRotate(ctx, project, sec.ID, sec)
				}
			}
			versions, err := s.secrets.ListVersions(ctx, project, sec.ID)
			if err != nil {
				continue
			}
			for _, v := range versions {
				if secretmanagersvc.ScheduledDestroyDue(v, now) {
					s.finalizeScheduledDestroy(ctx, project, sec.ID, v)
				}
			}
		}
	}
}

// SetEventPublisher wires the Pub/Sub publisher used to deliver a secret's
// event notifications, mirroring the REST provider's setter.
func (s *Service) SetEventPublisher(pub secretmanagersvc.Publisher) { s.publisher = pub }

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func mapSecretErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		return mapError(model.NewProviderError("NotFound", "secret not found", 404))
	}
	return mapError(err)
}

func mapVersionErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchVersion) {
		return mapError(model.NewProviderError("NotFound", "secret version not found", 404))
	}
	return mapError(err)
}

// ─── resource-name parsing ────────────────────────────────────────────────────

func secretName(project, id string) string {
	return resource.ResourceID(project)("secret", id)
}

func versionName(project, id, ver string) string {
	return secretName(project, id) + "/versions/" + ver
}

// splitSecretResource parses a full resource name into project, secret id, and
// version. It accepts "projects/{p}/secrets/{s}" (4 parts) and
// "projects/{p}/secrets/{s}/versions/{v}" (6 parts).
func splitSecretResource(name string) (project, id, version string, ok bool) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 4:
		if parts[0] == "projects" && parts[2] == "secrets" {
			return parts[1], parts[3], "", true
		}
	case 6:
		if parts[0] == "projects" && parts[2] == "secrets" && parts[4] == "versions" {
			return parts[1], parts[3], parts[5], true
		}
	}
	return "", "", "", false
}

// projectFromParent resolves the project from a "projects/{p}" parent (or a
// bare project id), falling back to metadata-derived project.
func (s *Service) projectFromParent(ctx context.Context, p string) string {
	if p != "" {
		return strings.TrimPrefix(p, "projects/")
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Secret Manager service ───────────────────────────────────────────────────

func (s *Service) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest) (*secretmanagerpb.ListSecretsResponse, error) {
	project := s.projectFromParent(ctx, req.GetParent())
	s.TrackProject(project)
	secrets, err := s.secrets.ListSecrets(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	// Sweep every secret whose expiration has passed before filtering/paging,
	// so an expired secret is neither counted nor returned (DC70).
	now := clock.Now()
	live := secrets[:0]
	for _, sec := range secrets {
		if secretmanagersvc.Expired(sec, now) {
			if err := s.expireSecret(ctx, project, sec.ID); err != nil {
				return nil, mapError(err)
			}
			// The locked sweep may have lost to a concurrent update that
			// extended the expiration: list the live record.
			if fresh, gerr := s.secrets.GetSecret(ctx, project, sec.ID); gerr == nil && !secretmanagersvc.Expired(fresh, now) {
				live = append(live, fresh)
			}
			continue
		}
		live = append(live, sec)
	}
	secrets = live
	matched, applied, err := secretmanagersvc.FilterSecrets(project, req.GetFilter(), secrets)
	if err != nil {
		return nil, mapError(err)
	}
	page, next, err := paging.Page(matched, func(sec secretmanagerstore.Secret) string { return sec.ID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*secretmanagerpb.Secret, 0, len(page))
	for _, sec := range page {
		out = append(out, secretToProto(project, sec))
	}
	// No total count is reported once a filter is applied.
	total := len(secrets)
	if applied {
		total = 0
	}
	return &secretmanagerpb.ListSecretsResponse{
		Secrets:       out,
		NextPageToken: next,
		TotalSize:     int32(total),
	}, nil
}

func (s *Service) CreateSecret(ctx context.Context, req *secretmanagerpb.CreateSecretRequest) (*secretmanagerpb.Secret, error) {
	project := s.projectFromParent(ctx, req.GetParent())
	s.TrackProject(project)
	id := req.GetSecretId()
	proto := req.GetSecret()
	if id == "" && proto != nil {
		_, id, _, _ = splitSecretResource(proto.GetName())
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing secretId", 400))
	}

	now := clock.Now()
	sec := secretmanagerstore.Secret{
		ID:         id,
		CreateTime: now,
		NextVer:    1,
	}
	if proto != nil {
		sec.Labels = proto.GetLabels()
		sec.Annotations = proto.GetAnnotations()
		topics, _ := topicsFromSecret(proto)
		sec.Topics = topics
		if r := proto.GetRotation(); r != nil {
			sec.Rotation = rotationFromProto(r)
		}
		if va := proto.GetVersionAliases(); va != nil {
			sec.VersionAliases = make(map[string]int, len(va))
			for k, v := range va {
				sec.VersionAliases[k] = int(v)
			}
		}
		// versionDestroyTtl and the expireTime/ttl oneof are part of the create
		// surface; ttl is input-only and resolves to an absolute expireTime.
		if ttl := proto.GetVersionDestroyTtl(); ttl != nil {
			if err := secretmanagersvc.ValidateVersionDestroyTTL(ttl.AsDuration()); err != nil {
				return nil, mapError(err)
			}
			sec.VersionDestroyTTL = secretmanagersvc.DurationString(ttl.AsDuration())
		}
		if exp := proto.GetExpireTime(); exp != nil {
			if err := secretmanagersvc.ValidateExpireTime(exp.AsTime(), now); err != nil {
				return nil, mapError(err)
			}
			sec.ExpireTime = exp.AsTime()
		} else if ttl := proto.GetTtl(); ttl != nil {
			expire, err := secretmanagersvc.ExpireTimeFromTTL(ttl.AsDuration(), now)
			if err != nil {
				return nil, mapError(err)
			}
			sec.ExpireTime = expire
		}
		replication, err := replicationFromSecret(proto)
		if err != nil {
			return nil, mapError(err)
		}
		if err := secretmanagersvc.ValidateReplicationKeys(ctx, s.encryptor, project, replication); err != nil {
			return nil, mapError(err)
		}
		sec.SetReplication(replication)
	}
	if err := secretmanagersvc.ValidateTopics(sec.Topics); err != nil {
		return nil, mapError(err)
	}
	if err := secretmanagersvc.ValidateRotation(sec.Rotation, sec.Topics, clock.Now(), true); err != nil {
		return nil, mapError(err)
	}

	if err := s.secrets.CreateSecret(ctx, project, id, sec); err != nil {
		if errors.Is(err, secretmanagerstore.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "secret already exists", 409))
		}
		return nil, mapError(err)
	}
	if len(sec.Topics) > 0 {
		secretmanagersvc.NotifyTopicConfigured(ctx, s.publisher, project, sec.Topics)
	}
	s.notifySecret(ctx, project, id, secretmanagersvc.EventSecretCreate, sec)
	return secretToProto(project, sec), nil
}

func (s *Service) GetSecret(ctx context.Context, req *secretmanagerpb.GetSecretRequest) (*secretmanagerpb.Secret, error) {
	project, id, _, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	s.TrackProject(project)
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	// A secret past its expireTime is already deleted in real GCP; the emulator
	// has no background worker, so a read performs the expiry sweep (DC70).
	if secretmanagersvc.Expired(sec, clock.Now()) {
		if err := s.expireSecret(ctx, project, id); err != nil {
			return nil, mapError(err)
		}
		return nil, mapSecretErr(secretmanagerstore.ErrNoSuchSecret)
	}
	sec = s.maybeRotate(ctx, project, id, sec)
	return secretToProto(project, sec), nil
}

func (s *Service) UpdateSecret(ctx context.Context, req *secretmanagerpb.UpdateSecretRequest) (*secretmanagerpb.Secret, error) {
	proto := req.GetSecret()
	if proto == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "secret is required", 400))
	}
	project, id, _, ok := splitSecretResource(proto.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// An expired secret is already gone in real GCP; sweep it before applying
	// the update so the caller sees the documented 404 (DC70).
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	// update_mask is required by the API: when present, only the listed fields
	// are written (a masked field set to empty clears the stored value); when
	// absent, every field present on the incoming Secret is applied (the
	// legacy merge behavior).
	mask := req.GetUpdateMask()
	applyAll := mask == nil || len(mask.GetPaths()) == 0
	fields := map[string]bool{}
	for _, p := range mask.GetPaths() {
		fields[p] = true
	}
	selected := func(name string) bool { return applyAll || fields[name] }

	// Parse the incoming replication policy once: it is request data, not store
	// state, so it must not be re-derived inside the (retried) atomic mutate.
	incomingReplication, err := replicationFromSecret(proto)
	if err != nil {
		return nil, mapError(err)
	}
	if incomingReplication != nil && selected("replication") {
		if err := secretmanagersvc.ValidateReplicationKeys(ctx, s.encryptor, project, incomingReplication); err != nil {
			return nil, mapError(err)
		}
	}

	// Rotation and topics are request data: parse them once, outside the
	// (retried) atomic mutate. A mask naming `rotation.next_rotation_time` or
	// `rotation.rotation_period` merges that subfield into the stored schedule
	// instead of replacing it, so a period-only patch keeps the next rotation
	// time.
	incomingRotation := rotationFromProto(proto.GetRotation())
	wholeRotation := applyAll || fields["rotation"]
	rotationNext := fields["rotation.next_rotation_time"]
	rotationPeriod := fields["rotation.rotation_period"]
	rotationTouched := proto.GetRotation() != nil && (wholeRotation || rotationNext || rotationPeriod)
	nextTimeSupplied := rotationTouched && incomingRotation.NextRotationTime != "" && (wholeRotation || rotationNext)
	incomingTopics, topicsSet := topicsFromSecret(proto)
	// A mask naming topics applies the list as sent (an empty list clears it);
	// a maskless update applies only a list the client actually populated,
	// mirroring the labels/annotations presence rule above.
	topicsTouched := selected("topics") && (topicsSet || !applyAll)
	now := clock.Now()

	// versionDestroyTtl and the expireTime/ttl oneof. The mask is a whitelist:
	// only a named field is read, so a labels-only update carrying a stale
	// read-back expiration cannot rewrite it. A named field the request does not
	// set clears the stored value; a maskless update only applies populated
	// fields.
	hasPaths := mask != nil && len(mask.GetPaths()) > 0
	incomingVersionTTL := proto.GetVersionDestroyTtl()
	applyVersionTTL := !hasPaths || fields["version_destroy_ttl"]
	versionTTLClear := hasPaths && fields["version_destroy_ttl"] && incomingVersionTTL == nil
	if applyVersionTTL && incomingVersionTTL != nil {
		if err := secretmanagersvc.ValidateVersionDestroyTTL(incomingVersionTTL.AsDuration()); err != nil {
			return nil, mapError(err)
		}
	}
	incomingExpire, incomingTTL := proto.GetExpireTime(), proto.GetTtl()
	// expire_time and ttl are one oneof: a mask naming either selects the group,
	// and the arm the request actually set is applied (REST cannot tell the arms
	// apart either).
	applyExpiration := !hasPaths || fields["expire_time"] || fields["ttl"]
	var resolvedExpire time.Time
	expireSet := false
	switch {
	case applyExpiration && incomingExpire != nil:
		if err := secretmanagersvc.ValidateExpireTime(incomingExpire.AsTime(), now); err != nil {
			return nil, mapError(err)
		}
		resolvedExpire, expireSet = incomingExpire.AsTime(), true
	case applyExpiration && incomingTTL != nil:
		expire, err := secretmanagersvc.ExpireTimeFromTTL(incomingTTL.AsDuration(), now)
		if err != nil {
			return nil, mapError(err)
		}
		resolvedExpire, expireSet = expire, true
	}
	expirationClear := hasPaths && (fields["expire_time"] || fields["ttl"]) && !expireSet

	// UpdateSecretAtomic reads, merges, and writes under one lock, so a
	// concurrent AddVersion's NextVersion() counter advance can't land
	// between our read and our write and get silently rolled back by it.
	updated, err := s.secrets.UpdateSecretAtomic(ctx, project, id, func(cur secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		// Secret.etag is the resource's optimistic-concurrency token (AIP-154),
		// enforced here like the delete and version-request preconditions; the
		// atomic mutate sees exactly the state this write replaces.
		if err := secretmanagersvc.CheckSecretEtag(secretName(project, id), cur, proto.GetEtag()); err != nil {
			return secretmanagerstore.Secret{}, err
		}
		if selected("labels") {
			if labels := proto.GetLabels(); labels != nil || !applyAll {
				cur.Labels = labels
			}
		}
		if selected("annotations") {
			if annotations := proto.GetAnnotations(); annotations != nil || !applyAll {
				cur.Annotations = annotations
			}
		}
		if rotationTouched {
			// managed_rotation_status is output-only: MergedRotation always
			// carries the stored config/status across a schedule update.
			cur.Rotation = secretmanagersvc.MergedRotation(cur.Rotation, incomingRotation, wholeRotation, rotationNext, rotationPeriod)
		}
		if topicsTouched {
			cur.Topics = incomingTopics
		}
		if selected("version_aliases") {
			if va := proto.GetVersionAliases(); va != nil {
				cur.VersionAliases = make(map[string]int, len(va))
				for k, v := range va {
					cur.VersionAliases[k] = int(v)
				}
			}
		}
		if versionTTLClear {
			cur.VersionDestroyTTL = ""
		} else if applyVersionTTL && incomingVersionTTL != nil {
			cur.VersionDestroyTTL = secretmanagersvc.DurationString(incomingVersionTTL.AsDuration())
		}
		if expirationClear {
			cur.ExpireTime = time.Time{}
		} else if expireSet {
			cur.ExpireTime = resolvedExpire
		}
		if selected("replication") {
			// The stored value is the raw policy (nil for a record written
			// before it was modelled), so a replication update adopts the arm
			// the client sends instead of conflicting with a reconstructed one.
			merged, err := secretmanagersvc.MergeReplication(cur.Replication, incomingReplication)
			if err != nil {
				return secretmanagerstore.Secret{}, err
			}
			// A nil merge result means the request carried no policy: leave the
			// record (including a legacy record's key) untouched.
			if merged != nil {
				cur.SetReplication(merged)
			}
		}
		// Validate the merged result: a patch that sets a period on a schedule
		// with no next rotation time, or removes every topic from a rotating
		// secret, is INVALID_ARGUMENT even though each half of the request
		// looked valid on its own.
		if topicsTouched {
			if err := secretmanagersvc.ValidateTopics(cur.Topics); err != nil {
				return secretmanagerstore.Secret{}, err
			}
		}
		if rotationTouched || topicsTouched {
			if err := secretmanagersvc.ValidateRotation(cur.Rotation, cur.Topics, now, nextTimeSupplied); err != nil {
				return secretmanagerstore.Secret{}, err
			}
		}
		return cur, nil
	})
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if topicsTouched && len(updated.Topics) > 0 {
		secretmanagersvc.NotifyTopicConfigured(ctx, s.publisher, project, updated.Topics)
	}
	s.notifySecret(ctx, project, id, secretmanagersvc.EventSecretUpdate, updated)
	return secretToProto(project, updated), nil
}

func (s *Service) DeleteSecret(ctx context.Context, req *secretmanagerpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	project, id, _, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	// An expired secret is already gone (and its EXPIRATION event published), so
	// a user delete answers the documented 404.
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	// The documented `etag` precondition (same contract as the REST provider),
	// checked inside the store's atomic delete so a concurrent update cannot
	// land between the comparison and the removal.
	var deleted secretmanagerstore.Secret
	err := s.secrets.DeleteSecretIf(ctx, project, id, func(sec secretmanagerstore.Secret) error {
		if err := secretmanagersvc.CheckSecretEtag(secretName(project, id), sec, req.GetEtag()); err != nil {
			return err
		}
		deleted = sec
		return nil
	})
	if err != nil {
		return nil, mapSecretErr(err)
	}
	// SECRET_DELETE carries the metadata as it was before the delete.
	s.notifySecretDelete(ctx, project, id, deleted, secretmanagersvc.DeleteTypeRequested)
	return &emptypb.Empty{}, nil
}

func (s *Service) AddSecretVersion(ctx context.Context, req *secretmanagerpb.AddSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	var data []byte
	if payload := req.GetPayload(); payload != nil {
		data = payload.GetData()
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}

	// Allocate the version number atomically (memory mutex / DB UPDATE
	// ... RETURNING), avoiding a Get→increment→Update race.
	ver, err := s.secrets.NextVersion(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	version := strconv.Itoa(ver)

	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}

	// The wrap keys are derived from the policy, never read off the
	// denormalised KmsKeyName mirror: a stale mirror must not wrap a version
	// with Google-managed encryption while the policy claims CMEK, and a
	// user-managed policy must wrap the same DEK under every replica's key
	// (DC57).
	wrap, err := secretmanagersvc.EncryptPayload(ctx, s.encryptor, project, sec.ReplicationPolicy(), data)
	if err != nil {
		return nil, mapError(err)
	}

	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: version, State: "ENABLED",
		CreateTime: clock.Now(), Data: wrap.Data,
		KmsKeyName: wrap.KmsKeyName, WrappedDEK: wrap.WrappedDEK,
		ReplicaDEKs: wrap.ReplicaDEKs,
	}
	if err := s.secrets.CreateVersion(ctx, project, stv); err != nil {
		return nil, mapError(err)
	}
	out := versionToProto(project, stv, sec.ReplicationPolicy())
	s.notifyVersion(ctx, project, sec, secretmanagersvc.EventSecretVersionAdd, out)
	return out, nil
}

func (s *Service) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	project, secret, version, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	version, err := s.resolveVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	if err := s.rejectExpired(ctx, project, secret); err != nil {
		return nil, err
	}
	v, err := s.secrets.GetVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v = s.finalizeScheduledDestroy(ctx, project, secret, v)
	if v.State != "ENABLED" {
		return nil, mapError(&model.ProviderError{
			Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
			Message: "secret version " + v.VersionID + " is " + v.State + ", not ENABLED",
		})
	}

	plain, err := s.decryptVersion(ctx, project, v)
	if err != nil {
		return nil, mapError(err)
	}

	crc := crc32cOf(plain)
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name: versionName(project, secret, version),
		Payload: &secretmanagerpb.SecretPayload{
			Data:       plain,
			DataCrc32C: &crc,
		},
	}, nil
}

func (s *Service) GetSecretVersion(ctx context.Context, req *secretmanagerpb.GetSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	project, secret, version, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	version, err := s.resolveVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	if err := s.rejectExpired(ctx, project, secret); err != nil {
		return nil, err
	}
	v, err := s.secrets.GetVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v = s.finalizeScheduledDestroy(ctx, project, secret, v)
	repl, err := secretmanagersvc.ParentReplication(ctx, s.secrets, project, secret)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	return versionToProto(project, v, repl), nil
}

// resolveVersion resolves the "latest" version alias to the highest existing
// version number. Any other version id passes through unchanged.
func (s *Service) resolveVersion(ctx context.Context, project, secret, version string) (string, error) {
	if version != "latest" {
		return version, nil
	}
	versions, err := s.secrets.ListVersions(ctx, project, secret)
	if err != nil {
		return "", err
	}
	latest := ""
	latestN := -1
	for _, v := range versions {
		if n, err := strconv.Atoi(v.VersionID); err == nil && n > latestN {
			latestN = n
			latest = v.VersionID
		}
	}
	if latest == "" {
		return "", secretmanagerstore.ErrNoSuchVersion
	}
	return latest, nil
}

func (s *Service) ListSecretVersions(ctx context.Context, req *secretmanagerpb.ListSecretVersionsRequest) (*secretmanagerpb.ListSecretVersionsResponse, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	versions, err := s.secrets.ListVersions(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	// Apply any delayed destruction that came due since the last read, so the
	// list (and its state filter) sees the DESTROYED version (DC70).
	for i := range versions {
		versions[i] = s.finalizeScheduledDestroy(ctx, project, id, versions[i])
	}
	parentReplication, err := secretmanagersvc.ParentReplication(ctx, s.secrets, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	matched, applied, err := secretmanagersvc.FilterVersions(project, id, req.GetFilter(), versions)
	if err != nil {
		return nil, mapError(err)
	}
	page, next, err := paging.Page(matched, func(v secretmanagerstore.Version) string { return v.VersionID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*secretmanagerpb.SecretVersion, 0, len(page))
	for _, v := range page {
		out = append(out, versionToProto(project, v, parentReplication))
	}
	total := len(versions)
	if applied {
		total = 0
	}
	return &secretmanagerpb.ListSecretVersionsResponse{
		Versions:      out,
		NextPageToken: next,
		TotalSize:     int32(total),
	}, nil
}

func (s *Service) DisableSecretVersion(ctx context.Context, req *secretmanagerpb.DisableSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DISABLED", req.GetEtag())
}

func (s *Service) EnableSecretVersion(ctx context.Context, req *secretmanagerpb.EnableSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "ENABLED", req.GetEtag())
}

func (s *Service) DestroySecretVersion(ctx context.Context, req *secretmanagerpb.DestroySecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DESTROYED", req.GetEtag())
}

// setVersionState applies a lifecycle transition (DISABLED/ENABLED/DESTROYED)
// to a secret version. etag is the optional documented precondition: a mismatch
// fails the transition with ABORTED.
func (s *Service) setVersionState(ctx context.Context, name, state, etag string) (*secretmanagerpb.SecretVersion, error) {
	project, secret, version, ok := splitSecretResource(name)
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.rejectExpired(ctx, project, secret); err != nil {
		return nil, err
	}
	// The delayed-destruction delay in force at this destroy attempt; a version
	// already scheduled for destruction keeps its own instant.
	parent, err := s.secrets.GetSecret(ctx, project, secret)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	var destroyDelay time.Duration
	if parent.VersionDestroyTTL != "" {
		if d, perr := secretmanagersvc.ParseVersionDestroyTTL(parent.VersionDestroyTTL); perr == nil {
			destroyDelay = d
		}
	}
	// Apply a delayed destruction that already came due, so enable/disable see
	// the terminal DESTROYED state instead of resurrecting a destroyed version.
	if cur, gerr := s.secrets.GetVersion(ctx, project, secret, version); gerr == nil {
		s.finalizeScheduledDestroy(ctx, project, secret, cur)
	}
	var scheduled, noop bool
	// The etag precondition and the transition run inside one atomic store
	// mutate, so a concurrent transition cannot invalidate the etag between the
	// check and the write.
	v, err := s.secrets.UpdateVersionIf(ctx, project, secret, version, func(v secretmanagerstore.Version) (secretmanagerstore.Version, error) {
		if err := secretmanagersvc.CheckVersionEtag(versionName(project, secret, version), v, etag); err != nil {
			return secretmanagerstore.Version{}, err
		}
		// DESTROYED is terminal: a version may not leave this state once entered.
		if v.State == "DESTROYED" {
			return secretmanagerstore.Version{}, &model.ProviderError{
				Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
				Message: "secret version " + v.VersionID + " is DESTROYED",
			}
		}
		now := clock.Now()
		if state == "DESTROYED" {
			// A second destroy of an already-scheduled version keeps its instant
			// and publishes nothing.
			if !v.ScheduledDestroyTime.IsZero() && now.Before(v.ScheduledDestroyTime) {
				noop = true
				return v, nil
			}
			if destroyDelay > 0 {
				// Delayed destruction: DISABLED now, DESTROYED when the delay
				// expires (SECRET_VERSION_DESTROY_SCHEDULED).
				v.State = "DISABLED"
				v.ScheduledDestroyTime = now.Add(destroyDelay)
				v.DestroyTime = time.Time{}
				scheduled = true
				return v, nil
			}
			v.State = "DESTROYED"
			v.DestroyTime = now
			v.ScheduledDestroyTime = time.Time{}
			return v, nil
		}
		// Enabling or disabling a scheduled version cancels the schedule.
		v.ScheduledDestroyTime = time.Time{}
		v.State = state
		return v, nil
	})
	if err != nil {
		return nil, mapVersionErr(err)
	}
	repl, err := secretmanagersvc.ParentReplication(ctx, s.secrets, project, secret)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	out := versionToProto(project, v, repl)
	if s.publisher != nil && !noop {
		eventType := secretmanagersvc.VersionStateEvent(state)
		if scheduled {
			eventType = secretmanagersvc.EventSecretVersionDestroyScheduled
		}
		s.notifyVersion(ctx, project, parent, eventType, out)
	}
	return out, nil
}

// expireSecret deletes id when its expireTime has passed, publishing
// SECRET_DELETE with deleteType EXPIRATION and the metadata as it was before
// the delete. A secret that is not due (or is already gone) is left alone.
func (s *Service) expireSecret(ctx context.Context, project, id string) error {
	expired, deleted, err := secretmanagersvc.ExpireSecretIfDue(ctx, s.secrets, project, id)
	if err != nil || !deleted {
		return err
	}
	s.notifySecretDelete(ctx, project, id, expired, secretmanagersvc.DeleteTypeExpiration)
	return nil
}

// rejectExpired performs the expiry sweep for a secret another handler is about
// to operate on, returning the documented NotFound once it has expired. A
// missing secret is left for the handler's own lookup to report.
func (s *Service) rejectExpired(ctx context.Context, project, id string) error {
	s.TrackProject(project)
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil || !secretmanagersvc.Expired(sec, clock.Now()) {
		return nil
	}
	if err := s.expireSecret(ctx, project, id); err != nil {
		return mapError(err)
	}
	// A concurrent update may have extended the expiration before the locked
	// delete re-checked it; proceed on the live record instead of a spurious
	// NotFound.
	if _, gerr := s.secrets.GetSecret(ctx, project, id); errors.Is(gerr, secretmanagerstore.ErrNoSuchSecret) {
		return mapSecretErr(secretmanagerstore.ErrNoSuchSecret)
	}
	return nil
}

// finalizeScheduledDestroy applies a due delayed destruction (shared with the
// REST provider) and publishes SECRET_VERSION_DESTROY when this call performed
// it. The background sweep applies it without traffic; every version read also
// applies it so the access gate is strict before the next tick.
func (s *Service) finalizeScheduledDestroy(ctx context.Context, project, secretID string, v secretmanagerstore.Version) secretmanagerstore.Version {
	updated, destroyed := secretmanagersvc.FinalizeScheduledDestroy(ctx, s.secrets, project, secretID, v)
	if destroyed && s.publisher != nil {
		if parent, perr := s.secrets.GetSecret(ctx, project, secretID); perr == nil {
			s.notifyVersion(ctx, project, parent, secretmanagersvc.EventSecretVersionDestroy,
				versionToProto(project, updated, parent.ReplicationPolicy()))
		}
	}
	return updated
}

// ─── Cloud SQL managed rotation ──────────────────────────────────────────────
// managedRotationConfigured reports whether Cloud SQL managed rotation has been
// enabled for sec. The API allows enabling it only once, so the enable
// precondition keys on presence (not on the ACTIVE state).
func managedRotationConfigured(sec secretmanagerstore.Secret) bool {
	return sec.Rotation != nil && sec.Rotation.ManagedRotation != nil
}

// managedRotationEnabled reports whether sec has ACTIVE Cloud SQL managed
// rotation.
func managedRotationEnabled(sec secretmanagerstore.Secret) bool {
	return sec.Rotation != nil && sec.Rotation.ManagedRotation != nil &&
		sec.Rotation.ManagedRotation.State == "ACTIVE"
}

func managedRotationErr(msg string) error {
	return mapError(&model.ProviderError{
		Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION", Message: msg,
	})
}

// EnableManagedRotation enables Cloud SQL managed rotation for a secret: it
// validates the Cloud SQL single-user credentials, records them, and stores a
// password (the supplied one, else a generated one) as a new ENABLED version.
// The emulator has no Cloud SQL data plane, so the password is not applied to a
// database user (GA7-D1).
func (s *Service) EnableManagedRotation(ctx context.Context, req *secretmanagerpb.EnableManagedRotationRequest) (*secretmanagerpb.SecretVersion, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	creds := req.GetCloudSqlSingleUserCredentials()
	if creds == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "cloud_sql_single_user_credentials is required", 400))
	}
	if creds.GetInstanceId() == "" || creds.GetUsername() == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "cloud_sql_single_user_credentials requires instance_id and username", 400))
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if managedRotationConfigured(sec) {
		return nil, managedRotationErr("managed rotation is already enabled for secret " + id)
	}
	password := creds.GetPassword()
	if password == "" {
		if password, err = secretmanagerstore.GeneratePassword(); err != nil {
			return nil, mapError(err)
		}
	}
	cfg := &secretmanagerstore.ManagedRotation{State: "ACTIVE", InstanceID: creds.GetInstanceId(), Username: creds.GetUsername()}
	version, err := s.appendManagedVersion(ctx, project, id, []byte(password), cfg)
	if err != nil {
		return nil, err
	}
	// Enabling managed rotation also records the schedule status on the secret,
	// so the secret itself is modified and SECRET_UPDATE follows the version
	// event.
	if updated, gerr := s.secrets.GetSecret(ctx, project, id); gerr == nil {
		s.notifySecret(ctx, project, id, secretmanagersvc.EventSecretUpdate, updated)
	}
	return version, nil
}

// RotateSecret performs a managed rotation for a secret whose managed rotation
// is already enabled: it stores a freshly generated password as a new ENABLED
// version. Without a Cloud SQL data plane the password is not applied to the
// linked database user (GA7-D1).
func (s *Service) RotateSecret(ctx context.Context, req *secretmanagerpb.RotateSecretRequest) (*secretmanagerpb.SecretVersion, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if !managedRotationEnabled(sec) {
		return nil, managedRotationErr("managed rotation is not enabled for secret " + id)
	}
	password, err := secretmanagerstore.GeneratePassword()
	if err != nil {
		return nil, mapError(err)
	}
	return s.appendManagedVersion(ctx, project, id, []byte(password), nil)
}

// appendManagedVersion stores data as a new ENABLED version. When cfg is
// non-nil it also records the managed-rotation config and allocates the version
// number in one locked read-modify-write, so a concurrent AddVersion cannot
// roll the counter back and managed rotation cannot be enabled twice. The
// payload is encrypted before the locked write, so a crypto failure cannot leave
// the secret one-shot-enabled with no version.
func (s *Service) appendManagedVersion(ctx context.Context, project, id string, data []byte, cfg *secretmanagerstore.ManagedRotation) (*secretmanagerpb.SecretVersion, error) {
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	// The wrap keys are derived from the policy, never read off the
	// denormalised KmsKeyName mirror (and one DEK is wrapped per CMEK binding
	// for a user-managed policy, DC57).
	wrap, err := secretmanagersvc.EncryptPayload(ctx, s.encryptor, project, sec.ReplicationPolicy(), data)
	if err != nil {
		return nil, mapError(err)
	}
	var ver int
	if _, err := s.secrets.UpdateSecretAtomic(ctx, project, id, func(cur secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		if cfg != nil && managedRotationConfigured(cur) {
			return secretmanagerstore.Secret{}, secretmanagerstore.ErrManagedRotationEnabled
		}
		ver = cur.NextVer
		cur.NextVer++
		if cfg != nil {
			// Copy the Rotation before setting the new field: GetSecret returns
			// a struct whose *Rotation aliases the store's map entry, so an
			// in-place field write would race a concurrent reader.
			rot := secretmanagerstore.Rotation{}
			if cur.Rotation != nil {
				rot = *cur.Rotation
			}
			rot.ManagedRotation = cfg
			cur.Rotation = &rot
		}
		return cur, nil
	}); err != nil {
		if errors.Is(err, secretmanagerstore.ErrManagedRotationEnabled) {
			return nil, managedRotationErr("managed rotation is already enabled for secret " + id)
		}
		return nil, mapSecretErr(err)
	}
	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(ver), State: "ENABLED",
		CreateTime: clock.Now(), Data: wrap.Data,
		KmsKeyName: wrap.KmsKeyName, WrappedDEK: wrap.WrappedDEK,
		ReplicaDEKs: wrap.ReplicaDEKs,
	}
	if err := s.secrets.CreateVersion(ctx, project, stv); err != nil {
		return nil, mapError(err)
	}
	// Managed rotation adds a version like AddSecretVersion does, so the
	// secret's topics see the same SECRET_VERSION_ADD.
	out := versionToProto(project, stv, sec.ReplicationPolicy())
	s.notifyVersion(ctx, project, sec, secretmanagersvc.EventSecretVersionAdd, out)
	return out, nil
}

// maybeRotate advances a secret's rotation schedule when it is due: it moves
// nextRotationTime forward by rotationPeriod. Real Secret Manager's rotation
// schedule is notification-only — it publishes a SECRET_ROTATE message to the
// secret's Pub/Sub topics and reschedules the next one; it never creates a
// SecretVersion (only *managed* rotation adds versions, see RotateSecret). The
// proactive sweep advances due schedules (mirroring the REST provider); reads
// still perform the same check before the next tick (DC71).
func (s *Service) maybeRotate(ctx context.Context, account, id string, sec secretmanagerstore.Secret) secretmanagerstore.Secret {
	if sec.Rotation == nil || sec.Rotation.NextRotationTime == "" {
		return sec
	}
	next, err := time.Parse(time.RFC3339Nano, sec.Rotation.NextRotationTime)
	if err != nil || clock.Now().Before(next) {
		return sec
	}
	// Everything that matters — whether rotation is STILL due and the schedule
	// advance — is re-done from a fresh read inside UpdateSecretAtomic's locked
	// section (mirroring the REST provider), so a concurrent Update/AddVersion/
	// maybeRotate cannot interleave. The caller's snapshot is used only for the
	// cheap up-front due check above.
	updated, err := s.secrets.UpdateSecretAtomic(ctx, account, id, func(cur secretmanagerstore.Secret) (secretmanagerstore.Secret, error) {
		if cur.Rotation == nil || cur.Rotation.NextRotationTime == "" {
			return secretmanagerstore.Secret{}, errSkipRotation
		}
		next, err := time.Parse(time.RFC3339Nano, cur.Rotation.NextRotationTime)
		if err != nil || clock.Now().Before(next) {
			return secretmanagerstore.Secret{}, errSkipRotation
		}
		if cur.Rotation.RotationPeriod != "" {
			if d, err := time.ParseDuration(cur.Rotation.RotationPeriod); err == nil {
				rotation := *cur.Rotation
				rotation.NextRotationTime = timestamp.Canonical(clock.Now().Add(d))
				cur.Rotation = &rotation
			}
		}
		return cur, nil
	})
	if err != nil {
		// errSkipRotation or a store error: re-read so the response (and the
		// etag, which covers nextRotationTime) reflects the stored state rather
		// than this caller's possibly stale snapshot.
		if fresh, gerr := s.secrets.GetSecret(ctx, account, id); gerr == nil {
			return fresh
		}
		return sec
	}
	s.notifySecret(ctx, account, id, secretmanagersvc.EventSecretRotate, updated)
	return updated
}

// ─── IAM (google.iam.v1.IAMPolicy over secrets) ───────────────────────────────

func (s *Service) requireSecret(ctx context.Context, project, id string) error {
	if _, err := s.secrets.GetSecret(ctx, project, id); err != nil {
		return mapSecretErr(err)
	}
	return nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	return policyToProto(policy.Load(ctx, s.resources, project, rtSecretPolicy, id)), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	pol, err := policy.Set(ctx, s.resources, project, rtSecretPolicy, id, protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.rejectExpired(ctx, project, id); err != nil {
		return nil, err
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: policy.TestPermissions(req.GetPermissions())}, nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func secretToProto(project string, s secretmanagerstore.Secret) *secretmanagerpb.Secret {
	out := &secretmanagerpb.Secret{
		Name: secretName(project, s.ID),
		// etag is the shared Secret Manager etag (see service/secretmanager):
		// stable across reads, different after any metadata mutation.
		Etag:        secretmanagersvc.SecretEtag(secretName(project, s.ID), s),
		Replication: replicationToProto(s.ReplicationPolicy()),
	}
	if !s.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(s.CreateTime)
	}
	if s.Labels != nil {
		out.Labels = s.Labels
	}
	if s.Annotations != nil {
		out.Annotations = s.Annotations
	}
	if len(s.Topics) > 0 {
		out.Topics = make([]*secretmanagerpb.Topic, 0, len(s.Topics))
		for _, t := range s.Topics {
			out.Topics = append(out.Topics, &secretmanagerpb.Topic{Name: t})
		}
	}
	if s.Rotation != nil {
		out.Rotation = rotationToProto(s.Rotation)
	}
	if s.VersionAliases != nil {
		out.VersionAliases = make(map[string]int64, len(s.VersionAliases))
		for k, v := range s.VersionAliases {
			out.VersionAliases[k] = int64(v)
		}
	}
	if s.VersionDestroyTTL != "" {
		if d, err := time.ParseDuration(s.VersionDestroyTTL); err == nil {
			out.VersionDestroyTtl = durationpb.New(d)
		}
	}
	if !s.ExpireTime.IsZero() {
		// expireTime is always returned as a timestamp; the input-only ttl is
		// never part of a stored record.
		out.Expiration = &secretmanagerpb.Secret_ExpireTime{ExpireTime: timestamppb.New(s.ExpireTime)}
	}
	return out
}

func versionToProto(project string, v secretmanagerstore.Version, repl *secretmanagerstore.Replication) *secretmanagerpb.SecretVersion {
	name := versionName(project, v.SecretID, v.VersionID)
	scheduledEtag := ""
	if !v.ScheduledDestroyTime.IsZero() {
		scheduledEtag = timestamp.Canonical(v.ScheduledDestroyTime)
	}
	out := &secretmanagerpb.SecretVersion{
		Name:  name,
		State: stateToProto(v.State),
		// SecretVersion.etag is output-only in the API and is the value the
		// disable/enable/destroy preconditions compare against, so it must be
		// present on every response (the gRPC surface used to omit it).
		Etag: secretmanagersvc.VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, scheduledEtag),
		// Mirror the REST provider's versionToMap: real Secret Manager marks
		// version responses as carrying a client-specified payload checksum.
		ClientSpecifiedPayloadChecksum: true,
		// The version reports the CMEK status of its own payload under the
		// parent secret's replication arm (DC44/DC45).
		ReplicationStatus: versionReplicationStatusToProto(v, repl),
	}
	if !v.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(v.CreateTime)
	}
	if !v.DestroyTime.IsZero() {
		out.DestroyTime = timestamp.Proto(v.DestroyTime)
	}
	if !v.ScheduledDestroyTime.IsZero() {
		out.ScheduledDestroyTime = timestamp.Proto(v.ScheduledDestroyTime)
	}
	return out
}

// versionReplicationStatusToProto renders the SecretVersion's replication
// status. For an automatic policy real GCP always sets the automatic arm and
// nests customerManagedEncryption (the CryptoKeyVersion that wrapped this
// version) only when CMEK was used; for a user-managed policy it reports one
// status per replica of the parent secret, with the CMEK status on the replica
// whose key wrapped the payload. Sibling to the REST provider's
// versionReplicationStatusToMap.
func versionReplicationStatusToProto(v secretmanagerstore.Version, repl *secretmanagerstore.Replication) *secretmanagerpb.ReplicationStatus {
	keyVersion := secretmanagersvc.KmsKeyVersionName(v.KmsKeyName, v.WrappedDEK)
	if repl != nil && repl.UserManaged != nil {
		statuses := secretmanagersvc.VersionReplicaStatuses(secretmanagersvc.WrapOf(v), repl)
		replicas := make([]*secretmanagerpb.ReplicationStatus_UserManagedStatus_ReplicaStatus, 0, len(statuses))
		for _, st := range statuses {
			rep := &secretmanagerpb.ReplicationStatus_UserManagedStatus_ReplicaStatus{Location: st.Location}
			if st.KmsKeyVersionName != "" {
				rep.CustomerManagedEncryption = &secretmanagerpb.CustomerManagedEncryptionStatus{
					KmsKeyVersionName: st.KmsKeyVersionName,
				}
			}
			replicas = append(replicas, rep)
		}
		return &secretmanagerpb.ReplicationStatus{
			ReplicationStatus: &secretmanagerpb.ReplicationStatus_UserManaged{
				UserManaged: &secretmanagerpb.ReplicationStatus_UserManagedStatus{Replicas: replicas},
			},
		}
	}
	auto := &secretmanagerpb.ReplicationStatus_AutomaticStatus{}
	if keyVersion != "" {
		auto.CustomerManagedEncryption = &secretmanagerpb.CustomerManagedEncryptionStatus{
			KmsKeyVersionName: keyVersion,
		}
	}
	return &secretmanagerpb.ReplicationStatus{
		ReplicationStatus: &secretmanagerpb.ReplicationStatus_Automatic{Automatic: auto},
	}
}

func stateToProto(state string) secretmanagerpb.SecretVersion_State {
	switch state {
	case "ENABLED":
		return secretmanagerpb.SecretVersion_ENABLED
	case "DISABLED":
		return secretmanagerpb.SecretVersion_DISABLED
	case "DESTROYED":
		return secretmanagerpb.SecretVersion_DESTROYED
	}
	return secretmanagerpb.SecretVersion_STATE_UNSPECIFIED
}

func rotationFromProto(r *secretmanagerpb.Rotation) *secretmanagerstore.Rotation {
	out := &secretmanagerstore.Rotation{}
	if r.GetNextRotationTime() != nil {
		out.NextRotationTime = timestamp.Canonical(r.GetNextRotationTime().AsTime())
	}
	if r.GetRotationPeriod() != nil {
		// Store the API's google-duration JSON form ("3600s"), not Go's duration
		// syntax ("1h0m0s"): the two transports share this record, and REST
		// renders the stored string verbatim.
		out.RotationPeriod = secretmanagersvc.RotationPeriodString(r.GetRotationPeriod().AsDuration().String())
	}
	return out
}

func rotationToProto(r *secretmanagerstore.Rotation) *secretmanagerpb.Rotation {
	out := &secretmanagerpb.Rotation{}
	if r.NextRotationTime != "" {
		if t, err := time.Parse(time.RFC3339Nano, r.NextRotationTime); err == nil {
			// nextRotationTime is a client-supplied instant stored as an RFC
			// 3339 string; the REST reader echoes the stored string verbatim
			// (provider/secretmanager secret.go), so the gRPC reader must not
			// truncate it — doing so would make the two transports disagree on
			// a value a REST client wrote.
			out.NextRotationTime = timestamppb.New(t)
		}
	}
	if r.RotationPeriod != "" {
		if d, err := time.ParseDuration(r.RotationPeriod); err == nil {
			out.RotationPeriod = durationpb.New(d)
		}
	}
	if r.ManagedRotation != nil {
		out.ManagedRotationStatus = &secretmanagerpb.Rotation_ManagedRotationStatus{
			State: managedStateToProto(r.ManagedRotation.State),
		}
	}
	return out
}

// managedStateToProto maps the stored managed-rotation state to its proto enum.
func managedStateToProto(state string) secretmanagerpb.Rotation_ManagedRotationStatus_State {
	switch state {
	case "ACTIVE":
		return secretmanagerpb.Rotation_ManagedRotationStatus_ACTIVE
	case "INACTIVE":
		return secretmanagerpb.Rotation_ManagedRotationStatus_INACTIVE
	}
	return secretmanagerpb.Rotation_ManagedRotationStatus_STATE_UNSPECIFIED
}

// replicationToProto renders the secret's replication policy — the oneof
// exactly as the client set it (DC45). The nested customerManagedEncryption
// object is omitted (not sent empty) when no CMEK is configured, matching real
// GCP; this is the gRPC mirror of the REST provider's replicationToMap.
func replicationToProto(repl *secretmanagerstore.Replication) *secretmanagerpb.Replication {
	if repl != nil && repl.UserManaged != nil {
		replicas := make([]*secretmanagerpb.Replication_UserManaged_Replica, 0, len(repl.UserManaged.Replicas))
		for _, rep := range repl.UserManaged.Replicas {
			pb := &secretmanagerpb.Replication_UserManaged_Replica{Location: rep.Location}
			if cme := rep.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
				pb.CustomerManagedEncryption = &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: cme.KmsKeyName}
			}
			replicas = append(replicas, pb)
		}
		return &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{
				UserManaged: &secretmanagerpb.Replication_UserManaged{Replicas: replicas},
			},
		}
	}
	auto := &secretmanagerpb.Replication_Automatic{}
	if repl != nil && repl.Automatic != nil {
		if cme := repl.Automatic.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
			auto.CustomerManagedEncryption = &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: cme.KmsKeyName}
		}
	}
	return &secretmanagerpb.Replication{
		Replication: &secretmanagerpb.Replication_Automatic_{Automatic: auto},
	}
}

// replicationFromSecret parses the replication oneof out of an incoming Secret
// (mirrors the REST provider's replicationFromBody). It returns (nil, nil) when
// the request carries no replication policy; ValidateReplication checks the
// shape. A nil proto is the caller's to reject.
func replicationFromSecret(sec *secretmanagerpb.Secret) (*secretmanagerstore.Replication, error) {
	repl := sec.GetReplication()
	if repl == nil {
		return nil, nil
	}
	out := &secretmanagerstore.Replication{}
	if auto := repl.GetAutomatic(); auto != nil {
		out.Automatic = &secretmanagerstore.AutomaticReplication{
			CustomerManagedEncryption: customerManagedEncryptionFromProto(auto.GetCustomerManagedEncryption()),
		}
	}
	if um := repl.GetUserManaged(); um != nil {
		umOut := &secretmanagerstore.UserManagedReplication{}
		for _, rep := range um.GetReplicas() {
			umOut.Replicas = append(umOut.Replicas, secretmanagerstore.Replica{
				Location:                  rep.GetLocation(),
				CustomerManagedEncryption: customerManagedEncryptionFromProto(rep.GetCustomerManagedEncryption()),
			})
		}
		out.UserManaged = umOut
	}
	if out.Automatic == nil && out.UserManaged == nil {
		return nil, model.NewProviderError("InvalidArgument", "replication must set exactly one of automatic or userManaged", 400)
	}
	return out, nil
}

// customerManagedEncryptionFromProto converts an optional CMEK message. The
// proto's message presence is what distinguishes "no CMEK" (nil) from a CMEK
// block with no key (which ValidateReplication rejects).
func customerManagedEncryptionFromProto(cme *secretmanagerpb.CustomerManagedEncryption) *secretmanagerstore.CustomerManagedEncryption {
	if cme == nil {
		return nil
	}
	return &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: cme.GetKmsKeyName()}
}

// crc32cOf returns the CRC32C-Castagnoli checksum of the payload as an int64
// (google.protobuf.Int64Value encoding).
func crc32cOf(data []byte) int64 {
	return int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
}

// decryptVersion unwraps a stored version's envelope-encrypted payload, falling
// back to another replica's wrap when the primary one's key is unavailable (a
// user-managed version carries one wrap per CMEK replica, DC57).
func (s *Service) decryptVersion(ctx context.Context, project string, v secretmanagerstore.Version) ([]byte, error) {
	return secretmanagersvc.DecryptVersion(ctx, s.encryptor, project, v)
}

func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ─── event notifications (DC64) ──────────────────────────────────────────────

// topicsFromSecret reads a proto Secret's topics list. A nil slice means the
// field was unset; an empty (non-nil) slice means the client sent an empty list
// (which clears the stored topics on a masked update).
func topicsFromSecret(p *secretmanagerpb.Secret) ([]string, bool) {
	raw := p.GetTopics()
	if raw == nil {
		return nil, false
	}
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		out = append(out, t.GetName())
	}
	return out, true
}

// notifySecret publishes an event about the secret itself to each of its
// topics, with the Secret resource (proto3 JSON) as data.
func (s *Service) notifySecret(ctx context.Context, project, id, eventType string, sec secretmanagerstore.Secret) {
	if s.publisher == nil || len(sec.Topics) == 0 {
		return
	}
	data, err := protojson.Marshal(secretToProto(project, sec))
	if err != nil {
		return
	}
	secretmanagersvc.Notify(ctx, s.publisher, project, sec.Topics, data,
		secretmanagersvc.SecretEventAttributes(eventType, secretName(project, id), clock.Now()))
}

// notifySecretDelete publishes SECRET_DELETE (REQUESTED for a user delete,
// EXPIRATION for the expiry sweep) with the metadata as it was before the
// delete.
func (s *Service) notifySecretDelete(ctx context.Context, project, id string, sec secretmanagerstore.Secret, deleteType string) {
	if s.publisher == nil || len(sec.Topics) == 0 {
		return
	}
	data, err := protojson.Marshal(secretToProto(project, sec))
	if err != nil {
		return
	}
	secretmanagersvc.Notify(ctx, s.publisher, project, sec.Topics, data,
		secretmanagersvc.DeleteEventAttributes(secretName(project, id), deleteType, clock.Now()))
}

// notifyVersion publishes a version-scoped event to the parent secret's topics,
// with the SecretVersion resource (proto3 JSON) as data.
func (s *Service) notifyVersion(ctx context.Context, project string, parent secretmanagerstore.Secret, eventType string, v *secretmanagerpb.SecretVersion) {
	if s.publisher == nil || len(parent.Topics) == 0 {
		return
	}
	data, err := protojson.Marshal(v)
	if err != nil {
		return
	}
	secretmanagersvc.Notify(ctx, s.publisher, project, parent.Topics, data,
		secretmanagersvc.VersionEventAttributes(eventType, secretName(project, parent.ID), v.GetName(), clock.Now()))
}
