package managedkafkaui

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"jaiscloud/internal/config"
	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Managed Kafka UI API requests by calling the Managed Kafka
// core.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// formatTime renders a stored timestamp as RFC3339, or "" when zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// topicConfigs extracts the Kafka property overrides (the wire `configs` map)
// from a stored topic's verbatim config body.
func topicConfigs(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var body struct {
		Configs map[string]string `json:"configs"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body.Configs) == 0 {
		return nil
	}
	return body.Configs
}

// renderCluster flattens a stored cluster into the console row. The verbatim
// config is included only on the detail read to keep the list payload small.
func renderCluster(project string, c mkstore.Cluster, full bool) Cluster {
	// Match the wire read-back: a cluster with no live broker (the default mock
	// topology) renders the synthesized cloud.goog bootstrap address.
	addr := c.BootstrapAddress
	if addr == "" {
		addr = managedkafkacore.BootstrapAddress(project, c.Location, c.Name)
	}
	out := Cluster{
		ID:               c.Name,
		Name:             managedkafkacore.ClusterName(project, c.Location, c.Name),
		Location:         c.Location,
		State:            "ACTIVE",
		BootstrapAddress: addr,
		Labels:           c.Labels,
		CreateTime:       formatTime(c.CreateTime),
		UpdateTime:       formatTime(c.UpdateTime),
	}
	if full {
		out.Config = canonicalClusterConfig(c.Config)
	}
	return out
}

// canonicalClusterConfig narrows a stored cluster's verbatim request body to the
// config keys the wire read-back renders (capacityConfig, gcpConfig,
// rebalanceConfig, tlsConfig), dropping request-only fields such as the nested
// labels. It matches managedkafka.ClusterJSON so the console shows the same
// object the REST/gRPC APIs return.
func canonicalClusterConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var cfg map[string]json.RawMessage
	if json.Unmarshal(raw, &cfg) != nil {
		return raw
	}
	out := map[string]json.RawMessage{}
	for _, k := range []string{"capacityConfig", "gcpConfig", "rebalanceConfig", "tlsConfig"} {
		if v, ok := cfg[k]; ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return raw
	}
	return b
}

// renderTopic flattens a stored topic into the console row. The Kafka property
// overrides are included only on the detail read.
func renderTopic(project string, t mkstore.Topic, full bool) Topic {
	out := Topic{
		ID:                t.Name,
		Name:              managedkafkacore.TopicName(project, t.Location, t.ClusterName, t.Name),
		Location:          t.Location,
		Cluster:           t.ClusterName,
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
		CreateTime:        formatTime(t.CreateTime),
		UpdateTime:        formatTime(t.UpdateTime),
	}
	if full {
		out.Configs = topicConfigs(t.Config)
	}
	return out
}

// renderAcl flattens a stored ACL into the console row.
func renderAcl(project string, a mkstore.Acl) Acl {
	entries := make([]AclEntry, 0, len(a.AclEntries))
	for _, e := range a.AclEntries {
		entries = append(entries, AclEntry{
			Principal:      e.Principal,
			PermissionType: e.PermissionType,
			Operation:      e.Operation,
			Host:           e.Host,
		})
	}
	return Acl{
		ID:           a.Name,
		Name:         managedkafkacore.AclName(project, a.Location, a.ClusterName, a.Name),
		Location:     a.Location,
		Cluster:      a.ClusterName,
		ResourceType: a.ResourceType,
		ResourceName: a.ResourceName,
		PatternType:  a.PatternType,
		Etag:         a.Etag,
		Entries:      entries,
	}
}

// renderConsumerGroup flattens a group's topic-keyed committed offsets into the
// console's sorted row.
func renderConsumerGroup(project string, g managedkafkacore.ConsumerGroup) ConsumerGroup {
	offsets := make([]ConsumerGroupOffset, 0)
	for topic, gt := range g.Topics {
		for partition, meta := range gt.Partitions {
			offsets = append(offsets, ConsumerGroupOffset{
				Topic:     topic,
				Partition: partition,
				Offset:    meta.Offset,
				Metadata:  meta.Metadata,
			})
		}
	}
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		return offsets[i].Partition < offsets[j].Partition
	})
	return ConsumerGroup{
		ID:       g.Name,
		Name:     managedkafkacore.ConsumerGroupName(project, g.Location, g.Cluster, g.Name),
		Location: g.Location,
		Cluster:  g.Cluster,
		Offsets:  offsets,
	}
}

// ─── Clusters ────────────────────────────────────────────────────────────────

// GET /clusters
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	clusters, err := h.provider.ListAllClusters(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Cluster, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, renderCluster(project, c, false))
	}
	uihelper.WriteJSON(w, ListClustersResponse{Clusters: out, Total: len(out)})
}

// GET /clusters/{location}/{cluster}
func (h *Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	c, err := h.provider.GetCluster(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderCluster(project, c, true))
}

// ─── Topics ──────────────────────────────────────────────────────────────────

// GET /topics
func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	topics, err := h.provider.ListAllTopics(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	h.writeTopics(w, project, topics)
}

// GET /clusters/{location}/{cluster}/topics
func (h *Handler) ListClusterTopics(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	topics, err := h.provider.ListTopics(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	h.writeTopics(w, project, topics)
}

// GET /clusters/{location}/{cluster}/topics/{topic}
func (h *Handler) GetTopic(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	topic, ok := uihelper.Segment(r, "topic")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid topic", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	t, err := h.provider.GetTopic(r.Context(), project, location, cluster, topic)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTopic(project, t, true))
}

func (h *Handler) writeTopics(w http.ResponseWriter, project string, topics []mkstore.Topic) {
	out := make([]Topic, 0, len(topics))
	for _, t := range topics {
		out = append(out, renderTopic(project, t, false))
	}
	uihelper.WriteJSON(w, ListTopicsResponse{Topics: out, Total: len(out)})
}

// ─── ACLs (read-only) ────────────────────────────────────────────────────────

// GET /clusters/{location}/{cluster}/acls
func (h *Handler) ListAcls(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	acls, err := h.provider.ListAcls(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Acl, 0, len(acls))
	for _, a := range acls {
		out = append(out, renderAcl(project, a))
	}
	uihelper.WriteJSON(w, ListAclsResponse{Acls: out, Total: len(out)})
}

// ─── Consumer groups (read-only) ─────────────────────────────────────────────

// GET /clusters/{location}/{cluster}/consumer-groups
func (h *Handler) ListConsumerGroups(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	groups, err := h.provider.ListConsumerGroups(r.Context(), project, location, cluster)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]ConsumerGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, renderConsumerGroup(project, g))
	}
	uihelper.WriteJSON(w, ListConsumerGroupsResponse{Groups: out, Total: len(out)})
}

// ─── Cluster writes ──────────────────────────────────────────────────────────

// POST /clusters
func (h *Handler) CreateCluster(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[ClusterWriteInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	if in.ID == "" {
		uihelper.UIError(w, "InvalidArgument", "cluster id is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	c, err := h.provider.CreateCluster(r.Context(), project, in.Location, in.ID, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderCluster(project, c, true))
}

// PUT /clusters/{location}/{cluster}
func (h *Handler) UpdateCluster(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	in, ok := decodeInput[ClusterWriteInput](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	c, err := h.provider.UpdateCluster(r.Context(), project, location, cluster, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderCluster(project, c, true))
}

// ─── Topic writes ────────────────────────────────────────────────────────────

// POST /clusters/{location}/{cluster}/topics
func (h *Handler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	in, ok := decodeInput[TopicWriteInput](w, r)
	if !ok {
		return
	}
	if in.ID == "" {
		uihelper.UIError(w, "InvalidArgument", "topic id is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	t, err := h.provider.CreateTopic(r.Context(), project, location, cluster, in.ID, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderTopic(project, t, true))
}

// PUT /clusters/{location}/{cluster}/topics/{topic}
func (h *Handler) UpdateTopic(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	topic, ok := uihelper.Segment(r, "topic")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid topic", http.StatusBadRequest)
		return
	}
	in, ok := decodeInput[TopicWriteInput](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	t, err := h.provider.UpdateTopic(r.Context(), project, location, cluster, topic, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTopic(project, t, true))
}

// DELETE /clusters/{location}/{cluster}/topics/{topic}
func (h *Handler) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	topic, ok := uihelper.Segment(r, "topic")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid topic", http.StatusBadRequest)
		return
	}
	if err := h.provider.DeleteTopic(r.Context(), h.account(r), location, cluster, topic); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── ACL writes ──────────────────────────────────────────────────────────────

// POST /clusters/{location}/{cluster}/acls
func (h *Handler) CreateAcl(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	in, ok := decodeInput[AclWriteInput](w, r)
	if !ok {
		return
	}
	if in.ID == "" {
		uihelper.UIError(w, "InvalidArgument", "acl id is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	a, err := h.provider.CreateAcl(r.Context(), project, location, cluster, in.ID, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderAcl(project, a))
}

// PUT /clusters/{location}/{cluster}/acls/{acl}
func (h *Handler) UpdateAcl(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	acl, ok := aclParam(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[AclWriteInput](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	a, err := h.provider.UpdateAcl(r.Context(), project, location, cluster, acl, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderAcl(project, a))
}

// DELETE /clusters/{location}/{cluster}/acls/{acl}
func (h *Handler) DeleteAcl(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	acl, ok := aclParam(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteAcl(r.Context(), h.account(r), location, cluster, acl); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// AddAclEntryResponse is the response for POST .../acls/{acl}/entries.
type AddAclEntryResponse struct {
	Acl        Acl  `json:"acl"`
	AclCreated bool `json:"aclCreated"`
}

// POST /clusters/{location}/{cluster}/acls/{acl}/entries
func (h *Handler) AddAclEntry(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	acl, ok := aclParam(w, r)
	if !ok {
		return
	}
	entry, ok := decodeInput[AclEntry](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	a, created, err := h.provider.AddAclEntry(r.Context(), project, location, cluster, acl, entry)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, AddAclEntryResponse{Acl: renderAcl(project, a), AclCreated: created})
}

// RemoveAclEntryResponse is the response for DELETE .../acls/{acl}/entries.
// Exactly one of Acl / AclDeleted is set.
type RemoveAclEntryResponse struct {
	Acl        *Acl `json:"acl,omitempty"`
	AclDeleted bool `json:"aclDeleted,omitempty"`
}

// DELETE /clusters/{location}/{cluster}/acls/{acl}/entries
func (h *Handler) RemoveAclEntry(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	acl, ok := aclParam(w, r)
	if !ok {
		return
	}
	entry, ok := decodeInput[AclEntry](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	a, deleted, err := h.provider.RemoveAclEntry(r.Context(), project, location, cluster, acl, entry)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	if deleted || a == nil {
		uihelper.WriteJSON(w, RemoveAclEntryResponse{AclDeleted: true})
		return
	}
	rendered := renderAcl(project, *a)
	uihelper.WriteJSON(w, RemoveAclEntryResponse{Acl: &rendered})
}

// ─── Consumer-group writes ───────────────────────────────────────────────────

// PUT /clusters/{location}/{cluster}/consumer-groups/{group}
func (h *Handler) UpdateConsumerGroup(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	group, ok := uihelper.Segment(r, "group")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid consumer group", http.StatusBadRequest)
		return
	}
	in, ok := decodeInput[ConsumerGroupWriteInput](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	g, err := h.provider.UpdateConsumerGroup(r.Context(), project, location, cluster, group, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderConsumerGroup(project, g))
}

// DELETE /clusters/{location}/{cluster}/consumer-groups/{group}
func (h *Handler) DeleteConsumerGroup(w http.ResponseWriter, r *http.Request) {
	location, cluster, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	group, ok := uihelper.Segment(r, "group")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid consumer group", http.StatusBadRequest)
		return
	}
	if err := h.provider.DeleteConsumerGroup(r.Context(), h.account(r), location, cluster, group); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// decodeInput decodes a JSON request body into T, writing a 400 and reporting
// false on a malformed body.
func decodeInput[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var in T
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return in, false
	}
	return in, true
}

// aclParam returns the decoded {acl} path param. An ACL id may contain "/"
// (e.g. "topic/orders") and is carried percent-encoded; uihelper.PathParam
// decodes it, and the core validates the encoded pattern.
func aclParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	v := uihelper.PathParam(r, "acl")
	if v == "" {
		uihelper.UIError(w, "BadRequest", "invalid acl", http.StatusBadRequest)
		return "", false
	}
	return v, true
}

// target reads and validates the {location}/{<resource>} path parameters,
// writing a 400 and returning false when either is missing or malformed. key is
// the chi parameter name of the resource id segment ("cluster").
func (h *Handler) target(w http.ResponseWriter, r *http.Request, key string) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, key)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid "+key, http.StatusBadRequest)
		return "", "", false
	}
	return location, id, true
}
