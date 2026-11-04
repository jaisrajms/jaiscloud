package storage

import "jaiscloud/internal/model"

// The bucket's IAM configuration (GCS Bucket.iamConfiguration) is the switch
// every client reads to answer "is this bucket controlled by bucket-level IAM
// or by ACLs?". It has three members: uniformBucketLevelAccess (the real
// switch), bucketPolicyOnly (the deprecated pre-GA alias, which real GCS keeps
// identical to uniformBucketLevelAccess) and publicAccessPrevention.
//
// Both transports store and read it through the bucket meta map, normalized as
//
//	"iamConfiguration": {
//	    "uniformBucketLevelAccess": {"enabled": bool},
//	    "publicAccessPrevention": "inherited" | "enforced"
//	}
//
// so it survives a restart and a snapshot round-trip on both the memory and the
// Postgres stores — bucket metadata is a JSON blob there. It is a map rather
// than a struct for exactly that reason: the same object is persisted, read
// back by the REST provider and projected into the gRPC Bucket.IamConfig.

// The values GCS accepts for iamConfiguration.publicAccessPrevention.
const (
	PublicAccessPreventionInherited = "inherited"
	PublicAccessPreventionEnforced  = "enforced"
)

// IAMConfig is a bucket's iamConfiguration in normalized form: the two values
// real GCS reports (bucketPolicyOnly is derived from the uniform flag).
type IAMConfig struct {
	UniformBucketLevelAccess bool
	PublicAccessPrevention   string
}

// IAMConfigFromMap parses a stored (or client-supplied) iamConfiguration
// object. Absent members take real GCS's defaults: access is controlled by ACLs
// and IAM (uniform bucket-level access off) and public access prevention is
// inherited.
func IAMConfigFromMap(ic map[string]any) IAMConfig {
	cfg := IAMConfig{PublicAccessPrevention: PublicAccessPreventionInherited}
	if ic == nil {
		return cfg
	}
	// bucketPolicyOnly is the deprecated alias for uniformBucketLevelAccess;
	// real GCS keeps the two in sync, so either spelling a client stored means
	// the same thing.
	if uba, ok := ic["uniformBucketLevelAccess"].(map[string]any); ok {
		cfg.UniformBucketLevelAccess, _ = uba["enabled"].(bool)
	} else if bpo, ok := ic["bucketPolicyOnly"].(map[string]any); ok {
		cfg.UniformBucketLevelAccess, _ = bpo["enabled"].(bool)
	}
	if pap, ok := ic["publicAccessPrevention"].(string); ok && pap != "" {
		cfg.PublicAccessPrevention = pap
	}
	return cfg
}

// IAMConfigFromMeta parses the iamConfiguration stored on a bucket meta map.
func IAMConfigFromMeta(meta map[string]any) IAMConfig {
	ic, _ := meta["iamConfiguration"].(map[string]any)
	return IAMConfigFromMap(ic)
}

// MergeIAMConfig applies an iamConfiguration object from a create or patch
// request over the bucket's stored one and returns the normalized object to
// persist. Only the members present in the request change (JSON API patch
// semantics), bucketPolicyOnly is accepted as the deprecated alias for
// uniformBucketLevelAccess, and an unsupported publicAccessPrevention value is
// rejected the way GCS rejects it rather than silently ignored.
func MergeIAMConfig(stored, patch map[string]any) (map[string]any, error) {
	cfg := IAMConfigFromMap(stored)
	if patch != nil {
		if uba, ok := patch["uniformBucketLevelAccess"].(map[string]any); ok {
			if enabled, ok := uba["enabled"].(bool); ok {
				cfg.UniformBucketLevelAccess = enabled
			}
		} else if bpo, ok := patch["bucketPolicyOnly"].(map[string]any); ok {
			if enabled, ok := bpo["enabled"].(bool); ok {
				cfg.UniformBucketLevelAccess = enabled
			}
		}
		if pap, ok := patch["publicAccessPrevention"].(string); ok && pap != "" {
			switch pap {
			case PublicAccessPreventionInherited, PublicAccessPreventionEnforced:
				cfg.PublicAccessPrevention = pap
			case "unspecified":
				// "unspecified" was the pre-GA spelling of "inherited"; GCS
				// still maps it instead of rejecting it.
				cfg.PublicAccessPrevention = PublicAccessPreventionInherited
			default:
				return nil, model.NewProviderError("InvalidRequest",
					"Invalid value for iamConfiguration.publicAccessPrevention: "+pap+
						" (must be \"inherited\" or \"enforced\")", 400)
			}
		}
	}
	return map[string]any{
		"uniformBucketLevelAccess": map[string]any{"enabled": cfg.UniformBucketLevelAccess},
		"publicAccessPrevention":   cfg.PublicAccessPrevention,
	}, nil
}

// REST renders the iamConfiguration object the JSON API reports. The deprecated
// bucketPolicyOnly alias carries the same value as uniformBucketLevelAccess, as
// GCS does for backward compatibility — clients written against the old field
// must not see a different answer.
//
// lockedTime is deliberately not emitted. Real GCS sets it to the end of the
// 90-day window during which uniform bucket-level access can still be turned
// off; this emulator neither enforces that window nor tracks how long the
// setting has been on, so any deadline here would be an invented timestamp.
func (c IAMConfig) REST() map[string]any {
	return map[string]any{
		"uniformBucketLevelAccess": map[string]any{"enabled": c.UniformBucketLevelAccess},
		"bucketPolicyOnly":         map[string]any{"enabled": c.UniformBucketLevelAccess},
		"publicAccessPrevention":   c.PublicAccessPrevention,
	}
}
