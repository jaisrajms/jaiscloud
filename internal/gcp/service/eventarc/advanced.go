package eventarc

// Eventarc "advanced" surface: MessageBus, Enrollment, Pipeline, GoogleApiSource,
// ChannelConnection and the GoogleChannelConfig singleton. Unlike triggers and
// channels (which have their own typed store in internal/gcp/store/eventarc),
// these families are pure metadata records stored in the shared ResourceStore —
// the same pattern the Firestore Admin control plane uses — so memory and --dsn
// (Postgres) backends behave identically, snapshots cover them for free, and no
// per-service store interface or migration is needed.
//
// This is deliberately a control plane: real Eventarc's advanced surface routes
// events between buses/pipelines/enrollments, and the emulator models no such
// delivery engine. Records echo the request body verbatim; output-only fields
// (name/uid/etag/times, and a channel connection's activation token) are
// overlaid on read, and content etags back optimistic concurrency exactly like
// triggers. Behaviour that would have to be fabricated (bus/pipeline delivery
// semantics) is a separate, scheduled follow-up.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// Kind identifies one advanced-surface resource family. It is a small,
// comparable value type so the transports can switch on it (e.g. to pick the
// proto message to transcode into) without the core depending on protobuf.
type Kind struct {
	Proto     string // protobuf message name, e.g. "MessageBus"
	StoreType string // shared ResourceStore type
	Formatter string // internal/gcp/resource formatter key
	IDParam   string // REST create query parameter, e.g. "messageBusId"
	// ListMethod / ListField are the list RPC's action name and response field
	// ("ListMessageBuses" / "messageBuses").
	ListMethod string
	ListField  string
	// Singleton marks the one-record-per-location GoogleChannelConfig: it has no
	// id, no create/delete and is never listed.
	Singleton bool
	// NoUpdate marks a family with no update method (channel connections).
	NoUpdate bool
	HasUID   bool // carries an output-only uid
	// HasCreateTime marks a family that carries createTime (GoogleChannelConfig
	// does not).
	HasCreateTime      bool
	HasActivationToken bool
}

// The advanced-surface kinds. Message buses, enrollments, pipelines, Google API
// sources and channel connections are collections; the Google channel config is
// a per-location singleton.
var (
	MessageBusKind = Kind{
		Proto: "MessageBus", StoreType: "gcp_eventarc_message_bus",
		Formatter: "eventarc-message-bus", IDParam: "messageBusId",
		ListMethod: "ListMessageBuses", ListField: "messageBuses",
		HasUID: true, HasCreateTime: true,
	}
	EnrollmentKind = Kind{
		Proto: "Enrollment", StoreType: "gcp_eventarc_enrollment",
		Formatter: "eventarc-enrollment", IDParam: "enrollmentId",
		ListMethod: "ListEnrollments", ListField: "enrollments",
		HasUID: true, HasCreateTime: true,
	}
	PipelineKind = Kind{
		Proto: "Pipeline", StoreType: "gcp_eventarc_pipeline",
		Formatter: "eventarc-pipeline", IDParam: "pipelineId",
		ListMethod: "ListPipelines", ListField: "pipelines",
		HasUID: true, HasCreateTime: true,
	}
	GoogleApiSourceKind = Kind{
		Proto: "GoogleApiSource", StoreType: "gcp_eventarc_google_api_source",
		Formatter: "eventarc-google-api-source", IDParam: "googleApiSourceId",
		ListMethod: "ListGoogleApiSources", ListField: "googleApiSources",
		HasUID: true, HasCreateTime: true,
	}
	ChannelConnectionKind = Kind{
		Proto: "ChannelConnection", StoreType: "gcp_eventarc_channel_connection",
		Formatter: "eventarc-channel-connection", IDParam: "channelConnectionId",
		ListMethod: "ListChannelConnections", ListField: "channelConnections",
		HasUID: true, HasCreateTime: true, HasActivationToken: true, NoUpdate: true,
	}
	GoogleChannelConfigKind = Kind{
		Proto: "GoogleChannelConfig", StoreType: "gcp_eventarc_google_channel_config",
		Formatter: "eventarc-google-channel-config", Singleton: true, NoUpdate: true,
	}
)

// AdvancedRecord is the stored form of any advanced-surface resource. Config
// holds the wire request body verbatim (so read-back echoes what the caller
// sent); UID/ActivationToken are server-assigned at create time and kept stable.
type AdvancedRecord struct {
	Location        string            `json:"location"`
	ID              string            `json:"id"`
	Config          json.RawMessage   `json:"config,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	UID             string            `json:"uid,omitempty"`
	Etag            string            `json:"etag,omitempty"`
	ActivationToken string            `json:"activationToken,omitempty"`
	CreateTime      time.Time         `json:"createTime"`
	UpdateTime      time.Time         `json:"updateTime"`
}

// Name renders the resource's canonical name.
func (k Kind) Name(project, location, id string) string {
	if k.Singleton {
		return resource.ResourceID(project)(k.Formatter, location)
	}
	return resource.ResourceID(project)(k.Formatter, location+"/"+id)
}

// key is the store id (relative to the project): "location/id", or just
// "location" for the singleton.
func (k Kind) key(location, id string) string {
	if k.Singleton {
		return location
	}
	return location + "/" + id
}

// TypeURL is the google.protobuf.Any type URL for the kind, used when packing
// a done operation's typed response.
func (k Kind) TypeURL() string {
	return "type.googleapis.com/google.cloud.eventarc.v1." + k.Proto
}

// policyType is the ResourceStore type for the kind's IAM policy.
func (k Kind) policyType() string { return k.StoreType + "_policy" }

// AdvancedKinds lists the collection kinds (everything except the singleton).
var AdvancedKinds = []Kind{
	MessageBusKind, EnrollmentKind, PipelineKind, GoogleApiSourceKind, ChannelConnectionKind,
}

// HasListParams reports whether the kind's list method documents the
// filter/orderBy request parameters. Every advanced collection except
// channelConnections — whose Discovery method carries only pageSize/pageToken —
// does.
func (k Kind) HasListParams() bool { return k != ChannelConnectionKind }

// advancedNotFound builds the canonical NotFound error for a kind.
func advancedNotFound(k Kind) error {
	return model.NewProviderError("NotFound", strings.ToLower(k.Proto)+" not found", 404)
}

// advancedAlreadyExists builds the canonical AlreadyExists error for a kind.
func advancedAlreadyExists(k Kind) error {
	return model.NewProviderError("AlreadyExists", strings.ToLower(k.Proto)+" already exists", 409)
}

// isAdvancedNotFound reports whether err is a canonical NotFound provider error.
func isAdvancedNotFound(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == "NotFound"
}

// advancedEtag derives the content etag backing optimistic concurrency.
func advancedEtag(r AdvancedRecord) string {
	labels, _ := json.Marshal(r.Labels)
	return contentEtag(r.Config, labels, []byte(r.ActivationToken))
}

// AdvancedJSON renders a stored record as the eventarc.v1 wire map shared by
// both transports. The stored body is echoed verbatim; name/uid/etag/times (and
// a channel connection's activation token) are overlaid as output-only.
func AdvancedJSON(project string, k Kind, r AdvancedRecord) map[string]any {
	out := map[string]any{}
	if len(r.Config) > 0 {
		_ = json.Unmarshal(r.Config, &out)
	}
	out["name"] = k.Name(project, r.Location, r.ID)
	if k.HasUID {
		out["uid"] = r.UID
	}
	out["etag"] = r.Etag
	if k.HasCreateTime && !r.CreateTime.IsZero() {
		out["createTime"] = formatTimestamp(r.CreateTime)
	}
	if !r.UpdateTime.IsZero() {
		out["updateTime"] = formatTimestamp(r.UpdateTime)
	}
	if k.HasActivationToken {
		out["activationToken"] = r.ActivationToken
	}
	labels := r.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// --- store helpers ---

// getAdvanced loads a record; a missing singleton (GoogleChannelConfig)
// synthesizes an empty per-location default rather than 404.
func (s *Service) getAdvanced(ctx context.Context, project string, k Kind, location, id string) (AdvancedRecord, error) {
	if s.resources == nil {
		if k.Singleton {
			return AdvancedRecord{Location: location}, nil
		}
		return AdvancedRecord{}, advancedNotFound(k)
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, k.StoreType, k.key(location, id))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			if k.Singleton {
				return AdvancedRecord{Location: location}, nil
			}
			return AdvancedRecord{}, advancedNotFound(k)
		}
		return AdvancedRecord{}, err
	}
	var rec AdvancedRecord
	if err := json.Unmarshal(e.Data, &rec); err != nil {
		return AdvancedRecord{}, model.NewProviderError("Internal", err.Error(), 500)
	}
	return rec, nil
}

// listAdvancedAll returns every record of a kind in a location (unordered; the
// caller filters and orders).
func (s *Service) listAdvancedAll(ctx context.Context, project string, k Kind, location string) ([]AdvancedRecord, error) {
	if s.resources == nil {
		return nil, nil
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, k.StoreType, k.key(location, ""))
	if err != nil {
		return nil, err
	}
	out := make([]AdvancedRecord, 0, len(entries))
	for _, e := range entries {
		var rec AdvancedRecord
		if json.Unmarshal(e.Data, &rec) == nil {
			out = append(out, rec)
		}
	}
	return out, nil
}

// createAdvanced persists a new record (create semantics: AlreadyExists on a
// duplicate).
func (s *Service) createAdvanced(ctx context.Context, project string, k Kind, rec AdvancedRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	if s.resources == nil {
		return nil
	}
	err = s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: k.StoreType, ID: k.key(rec.Location, rec.ID), Data: data,
	})
	if errors.Is(err, store.ErrAlreadyExists) {
		return advancedAlreadyExists(k)
	}
	return err
}

// mutateAdvanced runs load → mutate → store inside the store's atomic
// read-modify-write, so etag/CAS checks can't race. A missing non-singleton is
// NotFound; a missing singleton starts from the synthesized default.
func (s *Service) mutateAdvanced(ctx context.Context, project string, k Kind, location, id string, mutate func(AdvancedRecord) (AdvancedRecord, error)) (AdvancedRecord, error) {
	if s.resources == nil {
		cur, err := s.getAdvanced(ctx, project, k, location, id)
		if err != nil {
			return AdvancedRecord{}, err
		}
		return mutate(cur)
	}
	var result AdvancedRecord
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, k.StoreType, k.key(location, id),
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var cur AdvancedRecord
			switch {
			case exists:
				if err := json.Unmarshal(current.Data, &cur); err != nil {
					return current, model.NewProviderError("Internal", err.Error(), 500)
				}
			case k.Singleton:
				cur = AdvancedRecord{Location: location}
			default:
				return current, advancedNotFound(k)
			}
			next, err := mutate(cur)
			if err != nil {
				return current, err
			}
			data, err := json.Marshal(next)
			if err != nil {
				return current, model.NewProviderError("Internal", err.Error(), 500)
			}
			result = next
			return store.ResourceEntry{Type: k.StoreType, ID: k.key(location, id), Data: data}, nil
		})
	if err != nil {
		return AdvancedRecord{}, err
	}
	return result, nil
}

// --- validation ---

// validateAdvanced enforces the reference contract for the families that carry
// one: an enrollment's messageBus must name an existing message bus, and a
// channel connection's channel must name an existing channel. The other
// families are pure metadata and only require their own id.
func (s *Service) validateAdvanced(ctx context.Context, project string, k Kind, body map[string]any) error {
	switch k.StoreType {
	case EnrollmentKind.StoreType:
		bus, _ := body["messageBus"].(string)
		if bus == "" {
			return invalidArgument("enrollment.messageBus is required")
		}
		loc, id := locationOf(bus), lastSegment(bus)
		if loc == "" || id == "" {
			return invalidArgument("invalid messageBus resource name")
		}
		if _, err := s.getAdvanced(ctx, project, MessageBusKind, loc, id); err != nil {
			return model.NewProviderError("NotFound", "messageBus not found: "+bus, 404)
		}
	case ChannelConnectionKind.StoreType:
		ch, _ := body["channel"].(string)
		if ch == "" {
			return invalidArgument("channelConnection.channel is required")
		}
		loc, id := locationOf(ch), lastSegment(ch)
		if loc == "" || id == "" {
			return invalidArgument("invalid channel resource name")
		}
		if _, err := s.store.GetChannel(ctx, project, loc, id); err != nil {
			return model.NewProviderError("NotFound", "channel not found: "+ch, 404)
		}
	}
	return nil
}

// --- update mask ---

// advancedOutputOnly names the fields the server owns. A mask naming one fails
// loud rather than letting a caller spoof it.
var advancedOutputOnly = map[string]bool{
	"name": true, "uid": true, "etag": true, "createtime": true,
	"updatetime": true, "activationtoken": true, "state": true,
	"satisfieszps": true, "pubsubtopic": true,
}

// advancedFields is the full update_mask field set of each advanced-surface
// kind (eventarc v1 Discovery schemas), normalized. A mask root outside a
// kind's set names no field and is a client error (400 INVALID_ARGUMENT,
// AIP-134/AIP-161); a root inside it that advancedOutputOnly owns stays a 501
// UnsupportedOperation.
var advancedFields = map[string]map[string]bool{
	"MessageBus": maskFieldSet(
		"annotations", "cryptoKeyName", "displayName", "labels", "loggingConfig",
		"name", "createTime", "etag", "uid", "updateTime",
	),
	"Enrollment": maskFieldSet(
		"annotations", "celMatch", "destination", "displayName", "labels",
		"messageBus", "name", "createTime", "etag", "uid", "updateTime",
	),
	"Pipeline": maskFieldSet(
		"annotations", "cryptoKeyName", "destinations", "displayName",
		"inputPayloadFormat", "labels", "loggingConfig", "mediations",
		"retryPolicy", "name", "createTime", "etag", "satisfiesPzs", "uid", "updateTime",
	),
	"GoogleApiSource": maskFieldSet(
		"annotations", "cryptoKeyName", "destination", "displayName", "labels",
		"loggingConfig", "organizationSubscription", "projectSubscriptions",
		"name", "createTime", "etag", "uid", "updateTime",
	),
	"GoogleChannelConfig": maskFieldSet(
		"cryptoKeyName", "labels", "name", "updateTime",
	),
	"ChannelConnection": maskFieldSet(
		"activationToken", "channel", "labels", "name", "createTime", "uid", "updateTime",
	),
}

// applyAdvancedMask merges an incoming body into a stored body under the
// updateMask paths (empty mask = apply every non-output-only body field). Path
// roots are matched case/underscore-insensitively and mapped onto their
// camelCase JSON key; a nested path applies its whole top-level field. A path
// that names no field of the kind is a 400 InvalidArgument; an output-only
// field a 501.
func applyAdvancedMask(k Kind, stored, incoming map[string]any, paths []string) (map[string]any, error) {
	merged := make(map[string]any, len(stored)+len(incoming))
	for key, v := range stored {
		merged[key] = v
	}
	if len(paths) == 0 {
		for key, v := range incoming {
			if advancedOutputOnly[normalizeMaskField(key)] {
				continue
			}
			merged[key] = v
		}
		return merged, nil
	}
	fields := advancedFields[k.Proto]
	for _, path := range paths {
		root := maskRoot(path)
		n := normalizeMaskField(root)
		if advancedOutputOnly[n] {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
		if !fields[n] {
			return nil, invalidMaskPath(path)
		}
		key := snakeToCamel(root)
		if v, present := incoming[key]; present {
			merged[key] = v
		}
	}
	return merged, nil
}

// mergeAdvancedRecord applies a PATCH body to a stored record. It performs no
// reference validation and never touches the store, so it is safe to call from
// inside the store's locked mutate closure.
func mergeAdvancedRecord(k Kind, stored AdvancedRecord, body map[string]any, paths []string) (AdvancedRecord, map[string]any, error) {
	cur := map[string]any{}
	if len(stored.Config) > 0 {
		_ = json.Unmarshal(stored.Config, &cur)
	}
	merged, err := applyAdvancedMask(k, cur, body, paths)
	if err != nil {
		return AdvancedRecord{}, nil, err
	}
	if labels := bodyStringMap(merged, "labels"); labels != nil {
		stored.Labels = labels
	}
	if data, err := json.Marshal(merged); err == nil {
		stored.Config = data
	}
	return stored, merged, nil
}

// snakeToCamel converts a proto snake_case field path to its JSON camelCase
// form ("logging_config" → "loggingConfig"); a path with no underscore is
// returned unchanged.
func snakeToCamel(s string) string {
	if !strings.ContainsRune(s, '_') {
		return s
	}
	parts := strings.Split(s, "_")
	var b strings.Builder
	b.WriteString(parts[0])
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		b.WriteString(p[1:])
	}
	return b.String()
}

// --- CRUD ---

// CreateAdvanced creates a record and returns it with the done create
// operation. validateOnly performs the validation and existence check without
// persisting.
func (s *Service) CreateAdvanced(ctx context.Context, project string, k Kind, location, id string, cfg json.RawMessage, validateOnly bool) (AdvancedRecord, Operation, error) {
	if location == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing location")
	}
	if !k.Singleton && id == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing " + k.IDParam)
	}
	if err := s.validateAdvanced(ctx, project, k, decodeBody(cfg)); err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	now := clock.Now().UTC()
	rec := AdvancedRecord{Location: location, ID: id, Labels: labelsFromConfig(cfg), CreateTime: now, UpdateTime: now}
	if k.HasUID {
		rec.UID = uuid.NewString()
	}
	if k.HasActivationToken {
		rec.ActivationToken = randomHex(32)
	}
	if len(cfg) > 0 {
		rec.Config = cfg
	}
	rec.Etag = advancedEtag(rec)
	target := k.Name(project, location, id)
	if validateOnly {
		if _, err := s.getAdvanced(ctx, project, k, location, id); err == nil {
			return AdvancedRecord{}, Operation{}, advancedAlreadyExists(k)
		} else if !isAdvancedNotFound(err) {
			return AdvancedRecord{}, Operation{}, err
		}
		return rec, newOperation(location, "create", target), nil
	}
	if err := s.createAdvanced(ctx, project, k, rec); err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	op, err := s.recordOperation(ctx, project, newOperation(location, "create", target),
		OperationResponse(k.TypeURL(), AdvancedJSON(project, k, rec)))
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	return rec, op, nil
}

// GetAdvanced returns one record. A missing singleton is synthesized.
func (s *Service) GetAdvanced(ctx context.Context, project string, k Kind, location, id string) (AdvancedRecord, error) {
	if location == "" {
		return AdvancedRecord{}, invalidArgument("missing location")
	}
	if !k.Singleton && id == "" {
		return AdvancedRecord{}, invalidArgument("missing " + k.IDParam)
	}
	return s.getAdvanced(ctx, project, k, location, id)
}

// ListAdvanced returns a cursor page of a collection in a location, restricted
// by the AIP-160 filter and ordered by orderBy (both documented on the
// collection's list method; see listparams.go). An empty filter matches every
// record and an empty orderBy is the default ascending-by-id order.
func (s *Service) ListAdvanced(ctx context.Context, project string, k Kind, location, filter, orderBy string, pageSize int, pageToken string) ([]AdvancedRecord, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	label := "eventarc " + k.ListMethod
	compiledFilter, err := compileListFilter(label, filter, advancedFilterFields(project, k))
	if err != nil {
		return nil, "", err
	}
	order, err := advancedOrderSpec(k).Parse(orderBy)
	if err != nil {
		return nil, "", invalidArgument(label + " orderBy: " + err.Error())
	}
	recs, err := s.listAdvancedAll(ctx, project, k, location)
	if err != nil {
		return nil, "", err
	}
	filtered := make([]AdvancedRecord, 0, len(recs))
	for _, r := range recs {
		if compiledFilter.match(r) {
			filtered = append(filtered, r)
		}
	}
	// The page cursor is applied after the filter and in the caller's order, so
	// paginating a filtered/ordered list never resumes in a different one.
	order.Sort(filtered)
	page, next, err := paging.PageOrdered(filtered, pageParams(pageSize, pageToken), order.Cursor, order.AfterCursor)
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// UpdateAdvanced merges the caller's fields into a record and returns it with
// the done update operation. reqEtag is the body-supplied etag precondition.
func (s *Service) UpdateAdvanced(ctx context.Context, project string, k Kind, location, id string, cfg json.RawMessage, mask, reqEtag string, validateOnly bool) (AdvancedRecord, Operation, error) {
	if location == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing location")
	}
	if !k.Singleton && id == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing " + k.IDParam)
	}
	body := decodeBody(cfg)
	paths := maskPaths(mask)

	// Reference validation reads other store entries, so it must run outside the
	// locked mutate closure (the store lock is not reentrant).
	stored, err := s.getAdvanced(ctx, project, k, location, id)
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	next, merged, err := mergeAdvancedRecord(k, stored, body, paths)
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	if err := s.validateAdvanced(ctx, project, k, merged); err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	target := k.Name(project, location, id)
	if validateOnly {
		return next, newOperation(location, "update", target), nil
	}
	updated, err := s.mutateAdvanced(ctx, project, k, location, id, func(cur AdvancedRecord) (AdvancedRecord, error) {
		if err := checkEtag(reqEtag, cur.Etag); err != nil {
			return AdvancedRecord{}, err
		}
		nx, _, err := mergeAdvancedRecord(k, cur, body, paths)
		if err != nil {
			return AdvancedRecord{}, err
		}
		nx.UpdateTime = clock.Now().UTC()
		nx.Etag = advancedEtag(nx)
		return nx, nil
	})
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	op, err := s.recordOperation(ctx, project, newOperation(location, "update", target),
		OperationResponse(k.TypeURL(), AdvancedJSON(project, k, updated)))
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	return updated, op, nil
}

// DeleteAdvanced deletes a record and returns the deleted record with the done
// delete operation.
func (s *Service) DeleteAdvanced(ctx context.Context, project string, k Kind, location, id, reqEtag string, validateOnly bool) (AdvancedRecord, Operation, error) {
	if location == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing location")
	}
	if !k.Singleton && id == "" {
		return AdvancedRecord{}, Operation{}, invalidArgument("missing " + k.IDParam)
	}
	stored, err := s.getAdvanced(ctx, project, k, location, id)
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	if err := checkEtag(reqEtag, stored.Etag); err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	target := k.Name(project, location, id)
	if validateOnly {
		return stored, newOperation(location, "delete", target), nil
	}
	if s.resources != nil {
		if err := s.resources.Delete(ctx, project, store.GlobalRegion, k.StoreType, k.key(location, id)); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return AdvancedRecord{}, Operation{}, advancedNotFound(k)
			}
			return AdvancedRecord{}, Operation{}, err
		}
	}
	op, err := s.recordOperation(ctx, project, newOperation(location, "delete", target),
		OperationResponse(k.TypeURL(), AdvancedJSON(project, k, stored)))
	if err != nil {
		return AdvancedRecord{}, Operation{}, err
	}
	return stored, op, nil
}

// ListMessageBusEnrollments returns the names of the enrollments attached to a
// message bus (the messageBuses.listEnrollments custom method). The message bus
// must exist.
func (s *Service) ListMessageBusEnrollments(ctx context.Context, project, location, busID string, pageSize int, pageToken string) ([]string, string, error) {
	if location == "" || busID == "" {
		return nil, "", invalidArgument("missing location or message bus id")
	}
	if _, err := s.GetAdvanced(ctx, project, MessageBusKind, location, busID); err != nil {
		return nil, "", err
	}
	recs, err := s.listAdvancedAll(ctx, project, EnrollmentKind, location)
	if err != nil {
		return nil, "", err
	}
	names := make([]string, 0, len(recs))
	for _, r := range recs {
		bus, _ := decodeBody(r.Config)["messageBus"].(string)
		if bus == "" {
			continue
		}
		if eventing.ResourceID(bus) == eventing.ResourceID(busID) || lastSegment(bus) == busID {
			names = append(names, EnrollmentKind.Name(project, location, r.ID))
		}
	}
	page, next, err := paging.Page(names, func(n string) string { return n }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// --- IAM ---

// requireAdvanced resolves the IAM policy resource for a kind and requires the
// record to exist (NotFound otherwise).
func (s *Service) requireAdvanced(ctx context.Context, project string, k Kind, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing resource name or location")
	}
	if _, err := s.getAdvanced(ctx, project, k, location, id); err != nil {
		return err
	}
	return nil
}

func (s *Service) AdvancedGetIamPolicy(ctx context.Context, project string, k Kind, location, id string) (policy.Policy, error) {
	if err := s.requireAdvanced(ctx, project, k, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Load(ctx, s.resources, project, k.policyType(), location+"/"+id), nil
}

func (s *Service) AdvancedSetIamPolicy(ctx context.Context, project string, k Kind, location, id string, body map[string]any) (policy.Policy, error) {
	if err := s.requireAdvanced(ctx, project, k, location, id); err != nil {
		return policy.Policy{}, err
	}
	return policy.Set(ctx, s.resources, project, k.policyType(), location+"/"+id, body)
}

func (s *Service) AdvancedTestIamPermissions(ctx context.Context, project string, k Kind, location, id string, perms []string) ([]string, error) {
	if err := s.requireAdvanced(ctx, project, k, location, id); err != nil {
		return nil, err
	}
	return policy.TestPermissions(perms), nil
}
