package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	core "jaiscloud/internal/gcp/service/managedkafka"
)

// aclSpec is one ACL resource pattern and its entry set: the broker-facing
// projection of a managed Kafka Acl. The managed Kafka aclId encodes the
// resource pattern (resourceType/resourceName/patternType), and each AclEntry
// is one Kafka ACL binding.
type aclSpec struct {
	ResourceType string
	ResourceName string
	PatternType  string
	Entries      []core.AclBinding
}

// replaceACLs mirrors spec onto endpoint's broker through the pool. An empty
// endpoint (no live broker) is a metadata-only no-op.
func replaceACLs(ctx context.Context, pool *adminPool, endpoint string, spec aclSpec) error {
	if endpoint == "" {
		return nil
	}
	a, err := pool.get(endpoint)
	if err != nil {
		return err
	}
	return a.ReplaceACLs(ctx, spec)
}

// aclPattern maps the API pattern type onto its Kafka equivalent.
func aclPattern(patternType string) (kadm.ACLPattern, error) {
	switch strings.ToUpper(patternType) {
	case "LITERAL":
		return kadm.ACLPatternLiteral, nil
	case "PREFIXED":
		return kadm.ACLPatternPrefixed, nil
	default:
		return kadm.ACLPatternUnknown, fmt.Errorf("managedkafka broker: unknown acl pattern type %q", patternType)
	}
}

// aclOperation maps an API operation (case-insensitive) onto its Kafka
// equivalent. The accepted set is exactly the pinned proto's documented values.
func aclOperation(op string) (kadm.ACLOperation, error) {
	switch strings.ToUpper(op) {
	case "ALL":
		return kadm.OpAll, nil
	case "READ":
		return kadm.OpRead, nil
	case "WRITE":
		return kadm.OpWrite, nil
	case "CREATE":
		return kadm.OpCreate, nil
	case "DELETE":
		return kadm.OpDelete, nil
	case "ALTER":
		return kadm.OpAlter, nil
	case "DESCRIBE":
		return kadm.OpDescribe, nil
	case "CLUSTER_ACTION":
		return kadm.OpClusterAction, nil
	case "DESCRIBE_CONFIGS":
		return kadm.OpDescribeConfigs, nil
	case "ALTER_CONFIGS":
		return kadm.OpAlterConfigs, nil
	case "IDEMPOTENT_WRITE":
		return kadm.OpIdempotentWrite, nil
	default:
		return kadm.OpUnknown, fmt.Errorf("managedkafka broker: unknown acl operation %q", op)
	}
}

// aclBuilder returns an ACLBuilder scoped to spec's resource pattern.
func aclBuilder(spec aclSpec, pattern kadm.ACLPattern) (*kadm.ACLBuilder, error) {
	b := kadm.NewACLs().ResourcePatternType(pattern)
	switch strings.ToUpper(spec.ResourceType) {
	case "CLUSTER":
		// The only Kafka cluster resource is "kafka-cluster"; the builder
		// derives it, so spec.ResourceName is intentionally ignored.
		b.Clusters()
	case "TOPIC":
		b.Topics(spec.ResourceName)
	case "GROUP":
		b.Groups(spec.ResourceName)
	case "TRANSACTIONAL_ID":
		b.TransactionalIDs(spec.ResourceName)
	default:
		return nil, fmt.Errorf("managedkafka broker: unknown acl resource type %q", spec.ResourceType)
	}
	return b, nil
}

// aclDeleteFilter builds a builder that matches every binding for spec's
// resource pattern, whatever principal, host, operation, or permission.
func aclDeleteFilter(spec aclSpec, pattern kadm.ACLPattern) (*kadm.ACLBuilder, error) {
	b, err := aclBuilder(spec, pattern)
	if err != nil {
		return nil, err
	}
	// All four "any" flags make kadm emit a single filter with no principal,
	// no host, ANY operation, and ANY permission.
	b.Allow().Deny().AllowHosts().DenyHosts().Operations()
	return b, nil
}

// ensureUserPrefix gives a principal the "User:" prefix the Kafka
// StandardAuthorizer requires. A value that already carries it is unchanged, so
// the API's "User:*" wildcard survives.
func ensureUserPrefix(p string) string {
	if strings.HasPrefix(p, "User:") {
		return p
	}
	return "User:" + p
}

// aclHost returns the binding host, defaulting to Kafka's "*" wildcard.
func aclHost(h string) string {
	if h == "" {
		return "*"
	}
	return h
}

// firstCreateACLErr returns the first per-creation error, if any. kadm's
// top-level error is request-level only; broker rejections live per creation.
func firstCreateACLErr(rs kadm.CreateACLsResults) error {
	for _, r := range rs {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// firstDeleteACLErr returns the first filter-level or per-matching-ACL error, if
// any: a filter can match several ACLs and each match reports its own error, so
// a partial delete must not be reported as success.
func firstDeleteACLErr(rs kadm.DeleteACLsResults) error {
	for _, r := range rs {
		if r.Err != nil {
			return r.Err
		}
		for _, d := range r.Deleted {
			if d.Err != nil {
				return d.Err
			}
		}
	}
	return nil
}
