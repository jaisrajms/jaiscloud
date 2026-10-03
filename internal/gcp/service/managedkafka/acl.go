package managedkafka

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"jaiscloud/internal/gcp/paging"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

// AclEntryInput carries the caller-supplied fields of one ACL entry.
type AclEntryInput struct {
	Principal      string
	PermissionType string
	Operation      string
	Host           string
}

// AclInput carries the caller-supplied fields of an ACL create/update. Etag is
// only meaningful for UpdateAcl, where it backs the optimistic-concurrency
// check.
type AclInput struct {
	AclEntries []AclEntryInput
	Etag       string
}

// AclBinding is one Kafka ACL binding: the broker-facing projection of a stored
// AclEntry. It is the plain-string type the transport-neutral Broker interface
// accepts, so the broker never has to import the store.
type AclBinding struct {
	Principal      string
	PermissionType string
	Operation      string
	Host           string
}

// aclOperations is the set of operation values the Managed Kafka API accepts for
// an ACL entry (case-insensitive), exactly the values the pinned proto documents.
var aclOperations = map[string]struct{}{
	"ALL": {}, "READ": {}, "WRITE": {}, "CREATE": {}, "DELETE": {},
	"ALTER": {}, "DESCRIBE": {}, "CLUSTER_ACTION": {}, "DESCRIBE_CONFIGS": {},
	"ALTER_CONFIGS": {}, "IDEMPOTENT_WRITE": {},
}

// validPermissionType reports whether p is ALLOW or DENY (case-insensitive).
func validPermissionType(p string) bool {
	switch strings.ToUpper(p) {
	case "ALLOW", "DENY":
		return true
	}
	return false
}

// validAclOperation reports whether op is an accepted ACL operation.
func validAclOperation(op string) bool {
	_, ok := aclOperations[strings.ToUpper(op)]
	return ok
}

// maxAclEntries is the documented maximum number of entries per Acl.
const maxAclEntries = 100

// validateAclEntries rejects an entry the Kafka authorizer could not map, before
// any metadata is written, so a bad value is InvalidArgument rather than a
// mid-mutation broker rejection. The principal must carry the Kafka
// StandardAuthorizer "User:" prefix and the host must be the "*" wildcard, as
// the API requires; anything else would leave the stored metadata and the
// broker's binding disagreeing.
func validateAclEntries(entries []AclEntryInput) error {
	if len(entries) > maxAclEntries {
		return invalidArgument("aclEntries must not contain more than 100 entries")
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Principal, "User:") {
			return invalidArgument("acl entry principal must carry the \"User:\" prefix")
		}
		if e.Host != "*" {
			return invalidArgument("acl entry host must be \"*\"")
		}
		if !validPermissionType(e.PermissionType) {
			return invalidArgument("invalid permissionType " + e.PermissionType + "; expected ALLOW or DENY")
		}
		if !validAclOperation(e.Operation) {
			return invalidArgument("invalid operation " + e.Operation)
		}
	}
	return nil
}

// aclBindings projects stored entries onto broker bindings.
func aclBindings(entries []mkstore.AclEntry) []AclBinding {
	out := make([]AclBinding, 0, len(entries))
	for _, e := range entries {
		out = append(out, AclBinding{
			Principal:      e.Principal,
			PermissionType: e.PermissionType,
			Operation:      e.Operation,
			Host:           e.Host,
		})
	}
	return out
}

// deriveAclPattern maps an acl id to the output-only (resourceType,
// resourceName, patternType) triple the API derives from it. An unrecognised id
// is InvalidArgument.
func deriveAclPattern(aclID string) (resourceType, resourceName, patternType string, err error) {
	switch aclID {
	case "cluster":
		return "CLUSTER", "kafka-cluster", "LITERAL", nil
	case "allTopics":
		return "TOPIC", "*", "LITERAL", nil
	case "allConsumerGroups":
		return "GROUP", "*", "LITERAL", nil
	case "allTransactionalIds":
		return "TRANSACTIONAL_ID", "*", "LITERAL", nil
	}
	type pattern struct {
		prefix      string
		resource    string
		patternType string
	}
	for _, p := range []pattern{
		{"topic/", "TOPIC", "LITERAL"},
		{"topicPrefixed/", "TOPIC", "PREFIXED"},
		{"consumerGroup/", "GROUP", "LITERAL"},
		{"consumerGroupPrefixed/", "GROUP", "PREFIXED"},
		{"transactionalId/", "TRANSACTIONAL_ID", "LITERAL"},
		{"transactionalIdPrefixed/", "TRANSACTIONAL_ID", "PREFIXED"},
	} {
		if name, ok := strings.CutPrefix(aclID, p.prefix); ok && name != "" {
			return p.resource, name, p.patternType, nil
		}
	}
	return "", "", "", invalidArgument("invalid acl id " + aclID)
}

// toEntries converts typed input entries to stored entries.
func toEntries(in []AclEntryInput) []mkstore.AclEntry {
	out := make([]mkstore.AclEntry, 0, len(in))
	for _, e := range in {
		out = append(out, mkstore.AclEntry{
			Principal:      e.Principal,
			PermissionType: e.PermissionType,
			Operation:      e.Operation,
			Host:           e.Host,
		})
	}
	return out
}

// CreateAcl creates an ACL for an existing cluster.
func (s *Service) CreateAcl(ctx context.Context, project, location, clusterID, aclID string, in AclInput) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	if len(in.AclEntries) == 0 {
		return mkstore.Acl{}, invalidArgument("aclEntries must not be empty")
	}
	if err := validateAclEntries(in.AclEntries); err != nil {
		return mkstore.Acl{}, err
	}
	resourceType, resourceName, patternType, err := deriveAclPattern(aclID)
	if err != nil {
		return mkstore.Acl{}, err
	}
	a := mkstore.Acl{
		Location:     location,
		ClusterName:  clusterID,
		Name:         aclID,
		AclEntries:   toEntries(in.AclEntries),
		Etag:         randomHex(24),
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
	}
	if err := s.store.CreateAcl(ctx, project, location, clusterID, a); err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	// Mirror the ACL onto the cluster's live broker. The mirror is a full
	// replace, so on failure the metadata is rolled back and any bindings the
	// failed replace already wrote are best-effort cleared, keeping the API and
	// the broker consistent.
	if err := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, aclBindings(a.AclEntries)); err != nil {
		s.rollbackCreatedAcl(ctx, project, location, clusterID, a)
		return mkstore.Acl{}, err
	}
	return a, nil
}

// GetAcl returns one ACL.
func (s *Service) GetAcl(ctx context.Context, project, location, clusterID, aclID string) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	a, err := s.store.GetAcl(ctx, project, location, clusterID, aclID)
	if err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	return a, nil
}

// ListAcls returns a cursor page of the ACLs in a cluster.
func (s *Service) ListAcls(ctx context.Context, project, location, clusterID string, pageSize int, pageToken string) ([]mkstore.Acl, string, error) {
	if location == "" || clusterID == "" {
		return nil, "", invalidArgument("missing location or clusterId")
	}
	if _, err := s.store.GetCluster(ctx, project, location, clusterID); err != nil {
		return nil, "", mapStoreError(err)
	}
	acls, err := s.store.ListAcls(ctx, project, location, clusterID)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(acls, func(a mkstore.Acl) string { return a.Name }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateAcl replaces an ACL's entries under the etag optimistic-concurrency
// check. A missing/stale etag is ABORTED, matching real GCP.
func (s *Service) UpdateAcl(ctx context.Context, project, location, clusterID, aclID string, in AclInput) (mkstore.Acl, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, invalidArgument("missing location, clusterId, or aclId")
	}
	if in.Etag == "" {
		return mkstore.Acl{}, invalidArgument("missing etag")
	}
	if len(in.AclEntries) == 0 {
		return mkstore.Acl{}, invalidArgument("aclEntries must not be empty; use DeleteAcl to remove an acl")
	}
	if err := validateAclEntries(in.AclEntries); err != nil {
		return mkstore.Acl{}, err
	}
	var prev []mkstore.AclEntry
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		if cur.Etag != in.Etag {
			return mkstore.Acl{}, etagMismatch()
		}
		prev = cur.AclEntries
		cur.AclEntries = toEntries(in.AclEntries)
		cur.Etag = randomHex(24)
		return cur, nil
	})
	if err != nil {
		return mkstore.Acl{}, mapStoreError(err)
	}
	if err := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, aclBindings(a.AclEntries)); err != nil {
		s.restoreAclEntries(ctx, project, location, clusterID, aclID, a.ResourceType, a.ResourceName, a.PatternType, a.Etag, prev)
		return mkstore.Acl{}, err
	}
	return a, nil
}

// DeleteAcl deletes an ACL.
func (s *Service) DeleteAcl(ctx context.Context, project, location, clusterID, aclID string) error {
	if location == "" || clusterID == "" || aclID == "" {
		return invalidArgument("missing location, clusterId, or aclId")
	}
	a, err := s.store.GetAcl(ctx, project, location, clusterID, aclID)
	if err != nil {
		return mapStoreError(err)
	}
	// Remove the broker bindings first, then the metadata, so a broker failure
	// leaves the API-visible ACL (and its enforcement) intact.
	if err := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, nil); err != nil {
		return err
	}
	if err := s.store.DeleteAcl(ctx, project, location, clusterID, aclID); err != nil {
		return mapStoreError(err)
	}
	return nil
}

// AddAclEntry adds an entry to an ACL, creating the ACL when it does not exist
// yet. It returns the updated (or created) ACL and whether the ACL was created.
// Adding an entry that already exists is a no-op.
func (s *Service) AddAclEntry(ctx context.Context, project, location, clusterID, aclID string, entry AclEntryInput) (mkstore.Acl, bool, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return mkstore.Acl{}, false, invalidArgument("missing location, clusterId, or aclId")
	}
	if err := validateAclEntries([]AclEntryInput{entry}); err != nil {
		return mkstore.Acl{}, false, err
	}
	created := false
	var prev []mkstore.AclEntry
	changed := false
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		prev = cur.AclEntries
		if !aclHasEntry(cur.AclEntries, entry) {
			if len(cur.AclEntries) >= maxAclEntries {
				return mkstore.Acl{}, invalidArgument("aclEntries must not contain more than 100 entries")
			}
			cur.AclEntries = append(cur.AclEntries, toEntry(entry))
			cur.Etag = randomHex(24)
			changed = true
		}
		return cur, nil
	})
	if err == nil {
		// Adding an entry that already exists is a no-op: the broker already
		// has the binding, so it is not touched.
		if changed {
			if mbErr := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, aclBindings(a.AclEntries)); mbErr != nil {
				s.restoreAclEntries(ctx, project, location, clusterID, aclID, a.ResourceType, a.ResourceName, a.PatternType, a.Etag, prev)
				return mkstore.Acl{}, false, mbErr
			}
		}
		return a, created, nil
	}
	if !errors.Is(err, mkstore.ErrNoSuchAcl) {
		return mkstore.Acl{}, false, mapStoreError(err)
	}
	// The acl does not exist yet: create it with the entry. If a concurrent
	// AddAclEntry created it first, fall back to appending to the winner.
	created = true
	res, err := s.CreateAcl(ctx, project, location, clusterID, aclID, AclInput{AclEntries: []AclEntryInput{entry}})
	if err == nil {
		return res, created, nil
	}
	if !isAlreadyExists(err) {
		return mkstore.Acl{}, false, err
	}
	var prev2 []mkstore.AclEntry
	changed = false
	a, err = s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		prev2 = cur.AclEntries
		if !aclHasEntry(cur.AclEntries, entry) {
			if len(cur.AclEntries) >= maxAclEntries {
				return mkstore.Acl{}, invalidArgument("aclEntries must not contain more than 100 entries")
			}
			cur.AclEntries = append(cur.AclEntries, toEntry(entry))
			cur.Etag = randomHex(24)
			changed = true
		}
		return cur, nil
	})
	if err != nil {
		return mkstore.Acl{}, false, mapStoreError(err)
	}
	if changed {
		if mbErr := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, aclBindings(a.AclEntries)); mbErr != nil {
			s.restoreAclEntries(ctx, project, location, clusterID, aclID, a.ResourceType, a.ResourceName, a.PatternType, a.Etag, prev2)
			return mkstore.Acl{}, false, mbErr
		}
	}
	return a, false, nil
}

// isAlreadyExists reports whether err is an AlreadyExists provider error.
func isAlreadyExists(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "AlreadyExists"
}

// RemoveAclEntry removes an entry from an ACL. If it was the last entry the ACL
// is deleted and deleted reports true; otherwise the updated ACL is returned.
// Removing an absent entry is a no-op.
func (s *Service) RemoveAclEntry(ctx context.Context, project, location, clusterID, aclID string, entry AclEntryInput) (*mkstore.Acl, bool, error) {
	if location == "" || clusterID == "" || aclID == "" {
		return nil, false, invalidArgument("missing location, clusterId, or aclId")
	}
	deleted := false
	changed := false
	var prev []mkstore.AclEntry
	a, err := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		prev = cur.AclEntries
		kept := make([]mkstore.AclEntry, 0, len(cur.AclEntries))
		for _, e := range cur.AclEntries {
			if sameEntry(e, entry) {
				continue
			}
			kept = append(kept, e)
		}
		if len(cur.AclEntries) > 0 && len(kept) == 0 {
			deleted = true
		}
		if len(kept) != len(cur.AclEntries) {
			cur.Etag = randomHex(24)
			changed = true
		}
		cur.AclEntries = kept
		return cur, nil
	})
	if err != nil {
		return nil, false, mapStoreError(err)
	}
	if deleted {
		if mbErr := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, nil); mbErr != nil {
			s.restoreAclEntries(ctx, project, location, clusterID, aclID, a.ResourceType, a.ResourceName, a.PatternType, a.Etag, prev)
			return nil, false, mbErr
		}
		if err := s.store.DeleteAcl(ctx, project, location, clusterID, aclID); err != nil {
			return nil, false, mapStoreError(err)
		}
		return nil, true, nil
	}
	// Removing an entry that was never present is a no-op: the broker already
	// lacks the binding, so it is not touched.
	if changed {
		if mbErr := s.replaceBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, aclBindings(a.AclEntries)); mbErr != nil {
			s.restoreAclEntries(ctx, project, location, clusterID, aclID, a.ResourceType, a.ResourceName, a.PatternType, a.Etag, prev)
			return nil, false, mbErr
		}
	}
	return &a, false, nil
}

func toEntry(e AclEntryInput) mkstore.AclEntry {
	return mkstore.AclEntry{
		Principal:      e.Principal,
		PermissionType: e.PermissionType,
		Operation:      e.Operation,
		Host:           e.Host,
	}
}

func aclHasEntry(entries []mkstore.AclEntry, e AclEntryInput) bool {
	for _, cur := range entries {
		if sameEntry(cur, e) {
			return true
		}
	}
	return false
}

func sameEntry(a mkstore.AclEntry, b AclEntryInput) bool {
	return a.Principal == b.Principal &&
		a.PermissionType == b.PermissionType &&
		a.Operation == b.Operation &&
		a.Host == b.Host
}

// rollbackCreatedAcl undoes an ACL create whose broker mirror failed. It
// deletes the metadata only when the stored record is still untouched (etag
// matches the one this call wrote), so a concurrent successful mutation is
// never clobbered, and best-effort clears any partial broker bindings the
// failed mirror wrote.
func (s *Service) rollbackCreatedAcl(ctx context.Context, project, location, clusterID string, a mkstore.Acl) {
	cur, err := s.store.GetAcl(ctx, project, location, clusterID, a.Name)
	if err != nil {
		if !errors.Is(err, mkstore.ErrNoSuchAcl) {
			slog.Warn("managedkafka: acl create rollback could not read the record", "acl", a.Name, "err", err)
		}
		return
	}
	if cur.Etag != a.Etag {
		slog.Warn("managedkafka: skipping acl create rollback; record was modified concurrently", "acl", a.Name)
		return
	}
	if rbErr := s.store.DeleteAcl(ctx, project, location, clusterID, a.Name); rbErr != nil {
		slog.Warn("managedkafka: acl create rollback failed", "acl", a.Name, "err", rbErr)
	}
	s.compensateBrokerAcl(ctx, project, location, clusterID, a.ResourceType, a.ResourceName, a.PatternType, nil)
}

// restoreAclEntries rolls an ACL's entry set back after a broker mirror failure,
// but only when the stored record still carries the etag this call wrote, so a
// concurrent successful mutation is never clobbered. It then best-effort
// re-installs those entries on the broker, which a failed full-replace left with
// a partial or empty binding set.
func (s *Service) restoreAclEntries(ctx context.Context, project, location, clusterID, aclID, resourceType, resourceName, patternType, etag string, entries []mkstore.AclEntry) {
	if _, rbErr := s.store.UpdateAclAtomic(ctx, project, location, clusterID, aclID, func(cur mkstore.Acl) (mkstore.Acl, error) {
		if cur.Etag == etag {
			cur.AclEntries = entries
			cur.Etag = randomHex(24)
		}
		return cur, nil
	}); rbErr != nil {
		slog.Warn("managedkafka: acl entry rollback failed", "acl", aclID, "err", rbErr)
	}
	s.compensateBrokerAcl(ctx, project, location, clusterID, resourceType, resourceName, patternType, entries)
}

// compensateBrokerAcl best-effort re-installs entries on the broker after a
// failed mirror so broker bindings track the rolled-back metadata. A failure is
// logged, not surfaced: the caller is already returning the original error.
func (s *Service) compensateBrokerAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []mkstore.AclEntry) {
	if s.broker == nil {
		return
	}
	if err := s.broker.ReplaceAcl(ctx, project, location, cluster, resourceType, resourceName, patternType, aclBindings(entries)); err != nil {
		slog.Warn("managedkafka: acl broker compensation failed; broker may not match the API", "cluster", cluster, "err", err)
	}
}

// etagMismatch builds the ABORTED error for a stale optimistic-concurrency
// token.
func etagMismatch() error {
	return model.NewProviderError("Aborted", "etag mismatch", 409)
}
