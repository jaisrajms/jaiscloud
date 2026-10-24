// Rotation-schedule and event-notification-topic validation shared by the
// Secret Manager REST provider and gRPC service.
//
// Real Secret Manager constrains a rotation schedule on top of parsing it: the
// Discovery document's `Rotation` schema states that `nextRotationTime`
// "Cannot be set to less than 300s (5 min) in the future and at most
// 3153600000s (100 years)", that `rotationPeriod` "Must be in seconds and at
// least 3600s (1h) and at most 3153600000s (100 years)", that
// "nextRotationTime MUST be set if rotation_period is set", and that
// "Secret.topics must be set to configure rotation"; the rotation guide repeats
// all four as requirements and the Terraform provider enforces the two
// cross-field rules client-side (`rotation` is RequiredWith `topics`, and
// `next_rotation_time` is RequiredWith `rotation_period`). The `Secret.topics`
// schema allows "a list of up to 10 Pub/Sub topics", each a
// `projects/*/topics/*` resource name.
//
// The emulator used to store whatever it parsed — a 1-second period, a
// negative one, an already-past next rotation time, or a period with no next
// rotation time — where real GCP answers INVALID_ARGUMENT at create and patch.
// These helpers are transport-neutral so REST and gRPC cannot disagree about
// what a valid schedule (or topic list) is, and so the update paths share one
// definition of how a mask merges into the stored rotation.
package secretmanager

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

const (
	// MinSecretTopics/MaxSecretTopics bound a secret's event-notification list
	// ("A list of up to 10 Pub/Sub topics").
	MaxSecretTopics = 10
	// MinNextRotationLead is the documented minimum lead time: nextRotationTime
	// "Cannot be set to less than 300s (5 min) in the future".
	MinNextRotationLead = 300 * time.Second
	// MinRotationPeriod is the documented lower bound on a rotation period
	// ("at least 3600s (1h)"); MaxRotationSpan is the 100-year cap both fields
	// carry (3153600000s).
	MinRotationPeriod = 3600 * time.Second
	MaxRotationSpan   = 3153600000 * time.Second
)

// invalidRotation is the 400 INVALID_ARGUMENT every rotation-schedule and topic
// violation answers with, matching the plain descriptive messages the shared
// replication validators use.
func invalidRotation(format string, args ...any) error {
	return model.NewProviderError("InvalidArgument", fmt.Sprintf(format, args...), 400)
}

// ParseRotationDuration parses a google-duration JSON value the way Secret
// Manager accepts a rotation period: the shared google-duration parser, naming
// the rotation field in its error.
func ParseRotationDuration(s string) (time.Duration, error) {
	return parseDurationSeconds("rotation.rotationPeriod", s)
}

// DurationString renders a duration in the API's google-duration JSON form:
// seconds with the `s` suffix and protojson's 0/3/6/9 fractional digits
// ("3600s", "3600.500s", "3600.123456s"). This is what both transports store,
// so a value written through one transport reads back identically through the
// other.
func DurationString(d time.Duration) string {
	if d < 0 {
		// Negative durations are rejected by the parsers; render verbatim for a
		// legacy record rather than mangling it.
		return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
	}
	secs := int64(d / time.Second)
	nanos := d - time.Duration(secs)*time.Second
	if nanos == 0 {
		return strconv.FormatInt(secs, 10) + "s"
	}
	frac := fmt.Sprintf("%09d", nanos)
	switch {
	case strings.HasSuffix(frac, "000000"):
		frac = frac[:3]
	case strings.HasSuffix(frac, "000"):
		frac = frac[:6]
	}
	return fmt.Sprintf("%d.%ss", secs, frac)
}

// canonicalDuration is the internal spelling of DurationString.
func canonicalDuration(d time.Duration) string { return DurationString(d) }

// RotationPeriodString renders a stored period in the google-duration JSON form
// the API returns ("3600s"), not Go's duration syntax. It accepts both forms on
// input: a record written by an older build stored the gRPC client's
// `time.Duration.String()` value ("1h0m0s"), and both transports must render
// that record the same way the API would.
func RotationPeriodString(stored string) string {
	if stored == "" {
		return ""
	}
	if d, err := ParseRotationDuration(stored); err == nil {
		return canonicalDuration(d)
	}
	if d, err := time.ParseDuration(stored); err == nil {
		return canonicalDuration(d)
	}
	return stored
}

// ValidateTopics checks a secret's event-notification topic list: at most 10
// entries, each a Pub/Sub topic resource name (`projects/{p}/topics/{t}`).
// An empty list is valid (the secret simply has no notifications).
func ValidateTopics(topics []string) error {
	if len(topics) > MaxSecretTopics {
		return invalidRotation("a secret can be configured with at most %d topics, got %d", MaxSecretTopics, len(topics))
	}
	for i, t := range topics {
		if !validTopicName(t) {
			return invalidRotation("topics[%d].name %q is not a Pub/Sub topic name (projects/{project}/topics/{topic})", i, t)
		}
	}
	return nil
}

// validTopicName reports whether t is a `projects/{p}/topics/{t}` resource name
// with both components non-empty.
func validTopicName(t string) bool {
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[1] != "" && parts[2] == "topics" && parts[3] != ""
}

// ValidateRotation checks a secret's rotation schedule against the documented
// constraints, together with the topic list the schedule publishes to. now is
// the instant the request is served; nextTimeSupplied says whether this request
// set nextRotationTime (the five-minute lead time is a check on a *newly
// supplied* instant — it must be possible to patch the labels of a secret whose
// next rotation is minutes away). A nil schedule is valid.
func ValidateRotation(rot *secretmanagerstore.Rotation, topics []string, now time.Time, nextTimeSupplied bool) error {
	if rot == nil {
		return nil
	}
	if err := ValidateTopics(topics); err != nil {
		return err
	}
	if rot.RotationPeriod != "" {
		d, err := ParseRotationDuration(rot.RotationPeriod)
		if err != nil {
			return err
		}
		if d < MinRotationPeriod || d > MaxRotationSpan {
			return invalidRotation("rotation.rotationPeriod must be between 3600s (1h) and 3153600000s (100 years), got %q", rot.RotationPeriod)
		}
	}
	if rot.NextRotationTime == "" {
		if rot.RotationPeriod != "" {
			return invalidRotation("rotation.nextRotationTime must be set when rotation.rotationPeriod is set")
		}
		// A fieldless rotation object configures nothing (the Discovery marks
		// nextRotationTime Optional), and Cloud SQL managed rotation carries no
		// schedule of its own.
		return nil
	}
	next, err := time.Parse(time.RFC3339Nano, rot.NextRotationTime)
	if err != nil {
		return invalidRotation("rotation.nextRotationTime %q is not an RFC 3339 timestamp", rot.NextRotationTime)
	}
	if nextTimeSupplied {
		if next.Before(now.Add(MinNextRotationLead)) {
			return invalidRotation("rotation.nextRotationTime must be at least 5 minutes in the future")
		}
		if next.After(now.Add(MaxRotationSpan)) {
			return invalidRotation("rotation.nextRotationTime must be at most 100 years in the future")
		}
	}
	if len(topics) == 0 {
		return invalidRotation("topics must be set to configure rotation")
	}
	return nil
}

// MergedRotation applies a rotation update to the stored schedule. selectAll
// replaces the schedule with incoming (an absent incoming clears it, which is
// how a masked `rotation` patch with an empty body removes the schedule);
// otherwise only the named subfields are taken from incoming, so a patch
// selecting `rotation.rotation_period` keeps the stored nextRotationTime
// instead of silently dropping it. managedRotationStatus is output-only and is
// always carried over from the stored schedule.
func MergedRotation(current, incoming *secretmanagerstore.Rotation, selectAll, nextTime, period bool) *secretmanagerstore.Rotation {
	if selectAll {
		empty := incoming == nil || (incoming.NextRotationTime == "" && incoming.RotationPeriod == "" && incoming.ManagedRotation == nil)
		if empty {
			// A masked `rotation` patch with an empty body removes the schedule;
			// the output-only managed-rotation status survives it.
			if current != nil && current.ManagedRotation != nil {
				return &secretmanagerstore.Rotation{ManagedRotation: current.ManagedRotation}
			}
			return nil
		}
		out := *incoming
		if current != nil {
			out.ManagedRotation = current.ManagedRotation
		}
		return &out
	}
	if current == nil {
		current = &secretmanagerstore.Rotation{}
	}
	out := *current
	if incoming != nil {
		if nextTime {
			out.NextRotationTime = incoming.NextRotationTime
		}
		if period {
			out.RotationPeriod = incoming.RotationPeriod
		}
	}
	if out.NextRotationTime == "" && out.RotationPeriod == "" && out.ManagedRotation == nil {
		return nil
	}
	return &out
}
